"""The public API rejects over-context work before committing SSE headers."""

from __future__ import annotations

import asyncio
import json

import pytest

pytest.importorskip("ray", exc_type=ImportError)
pytest.importorskip("fastapi", exc_type=ImportError)

from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse

from exaserve.engines.base import GenDelta, GenResult
from exaserve import server


class _Backend:
    def __init__(self, *, prompt_tokens: int = 5, tokenizer_error: Exception | None = None):
        self.prompt_tokens = prompt_tokens
        self.tokenizer_error = tokenizer_error
        self.count_calls = []
        self.generate_calls = 0

    def build_chat_prompt(self, _messages, **_kwargs):
        return "rendered chat prompt"

    def count_prompt_tokens(self, prompt):
        self.count_calls.append(prompt)
        if self.tokenizer_error is not None:
            raise self.tokenizer_error
        return self.prompt_tokens

    async def generate(self, _prompt, _sampling):
        self.generate_calls += 1
        return GenResult(text="ok", prompt_tokens=self.prompt_tokens, completion_tokens=1)

    async def generate_stream(self, _prompt, _sampling):
        yield GenDelta(finish_reason="stop", prompt_tokens=self.prompt_tokens)


def _worker(backend: _Backend, *, max_model_len: int = 8):
    # Ray's ingress decorator adds an async ``__del__`` to the outer wrapper;
    # bypass it so a hermetic handler unit test does not manufacture an
    # un-awaited destructor coroutine when the fake worker is collected.
    worker_type = server.EngineWorker.func_or_class.__mro__[1]

    class WorkerForTest(worker_type):
        def __del__(self):
            return None

    worker = object.__new__(WorkerForTest)
    worker.backend = backend
    worker.max_model_len = max_model_len
    worker.model_id = "model-a"
    return worker_type, worker


def _request(path: str, body: dict, correlation: str = "caller-request-7") -> Request:
    encoded = json.dumps(body).encode("utf-8")
    delivered = False

    async def receive():
        nonlocal delivered
        if delivered:
            return {"type": "http.request", "body": b"", "more_body": False}
        delivered = True
        return {"type": "http.request", "body": encoded, "more_body": False}

    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode("ascii"),
            "query_string": b"",
            "headers": [
                (b"content-type", b"application/json"),
                (b"x-request-id", correlation.encode()),
            ],
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 80),
        },
        receive,
    )


def _case(endpoint: str, *, stream: bool, max_tokens: int) -> tuple[str, dict]:
    if endpoint == "chat":
        return "/v1/chat/completions", {
            "model": "model-a",
            "messages": [{"role": "user", "content": "hello"}],
            "max_tokens": max_tokens,
            "stream": stream,
        }
    return "/v1/completions", {
        "model": "model-a",
        "prompt": "plain prompt",
        "max_tokens": max_tokens,
        "stream": stream,
    }


async def _invoke(endpoint: str, *, stream: bool, max_tokens: int, backend: _Backend):
    worker_type, worker = _worker(backend)
    path, body = _case(endpoint, stream=stream, max_tokens=max_tokens)
    handler = worker_type.chat_completions if endpoint == "chat" else worker_type.completions
    return await handler(worker, _request(path, body))


@pytest.mark.parametrize("endpoint", ["chat", "completion"])
@pytest.mark.parametrize("stream", [False, True])
def test_over_context_is_a_correlated_typed_400_before_any_generation(endpoint, stream):
    backend = _Backend(prompt_tokens=5)
    response = asyncio.run(_invoke(endpoint, stream=stream, max_tokens=4, backend=backend))

    assert isinstance(response, JSONResponse), "an invalid stream must not commit SSE headers"
    assert response.status_code == 400
    assert response.headers["x-request-id"] == "caller-request-7"
    payload = json.loads(response.body)
    assert payload["request_id"] == "caller-request-7"
    assert payload["error"] == {
        "message": (
            "maximum context length is 8 tokens, but the rendered prompt uses 5 tokens "
            "and requests 4 completion tokens (9 total)"
        ),
        "type": "invalid_request_error",
        "param": "max_tokens",
        "code": "context_length_exceeded",
    }
    assert len(backend.count_calls) == 1
    assert backend.generate_calls == 0


@pytest.mark.parametrize("endpoint", ["chat", "completion"])
@pytest.mark.parametrize("stream", [False, True])
def test_exact_context_boundary_is_accepted(endpoint, stream):
    backend = _Backend(prompt_tokens=5)
    response = asyncio.run(_invoke(endpoint, stream=stream, max_tokens=3, backend=backend))

    assert response.status_code == 200
    assert response.headers["x-request-id"] == "caller-request-7"
    if stream:
        assert isinstance(response, StreamingResponse)
        assert backend.generate_calls == 0
    else:
        assert isinstance(response, JSONResponse)
        assert backend.generate_calls == 1
    assert len(backend.count_calls) == 1


@pytest.mark.parametrize("endpoint", ["chat", "completion"])
def test_tokenizer_failure_fails_closed_before_streaming_headers(endpoint):
    backend = _Backend(tokenizer_error=RuntimeError("tokenizer corrupt"))
    response = asyncio.run(_invoke(endpoint, stream=True, max_tokens=3, backend=backend))

    assert isinstance(response, JSONResponse)
    assert response.status_code == 500
    assert response.headers["x-request-id"] == "caller-request-7"
    payload = json.loads(response.body)
    assert payload["request_id"] == "caller-request-7"
    assert payload["error"]["type"] == "server_error"
    assert payload["error"]["code"] == "prompt_tokenization_failed"
    assert "corrupt" not in payload["error"]["message"]
    assert backend.generate_calls == 0
