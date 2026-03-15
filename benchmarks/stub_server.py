"""
Minimal, high-performance OpenAI-compatible stub HTTP server.

Purpose: act as a stand-in backend so that replay_client and LiteLLM proxy
can be benchmarked without any GPU / Ray / vLLM overhead.

Endpoints:
  POST /v1/chat/completions   -> returns a valid ChatCompletion response
  POST /v1/completions        -> returns a valid Completion response
  GET  /health                -> 200 OK
  GET  /health/liveliness     -> 200 OK  (LiteLLM health check compat)
  GET  /v1/models             -> returns a model list
  GET  /metrics               -> server-side counters (JSON)

CLI:
  python stub_server.py --port 8000 --latency-ms 0 --response-tokens 10
  python stub_server.py --port 8000 --latency-ms 5 --response-tokens 100 --model my-model
"""

import argparse
import asyncio
import json
import os
import socket as _socket
import sys
import time
import threading
from collections import deque

try:
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response
    from starlette.routing import Route
    import uvicorn
except ImportError:
    print("ERROR: starlette and uvicorn are required. Install via: pip install starlette uvicorn", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Shared state (thread-safe for the counters; asyncio for everything else)
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_total_requests = 0
_total_errors = 0
_request_timestamps: deque = deque(maxlen=10_000)  # wall-clock time of each request
_latency_samples: deque = deque(maxlen=10_000)      # per-request server processing time (s)

# Configuration (set at startup)
_MODEL_ID = "stub-model"
_LATENCY_S = 0.0
_RESPONSE_TOKENS = 10
_RESPONSE_TEXT = ""   # pre-built once at startup

# Pre-serialized response bytes — built once at startup so every request can
# return the same pre-encoded bytes without calling json.dumps() per call.
_CHAT_RESPONSE_BYTES: bytes = b""
_COMPLETION_RESPONSE_BYTES: bytes = b""


def _build_response_text(n_tokens: int) -> str:
    """Return a fixed response string approximating n_tokens output tokens."""
    word = "token"
    return " ".join([word] * n_tokens)


def _make_chat_completion(model: str, text: str, prompt_tokens: int = 8, completion_tokens: int = 10) -> dict:
    return {
        "id": "chatcmpl-stub",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": text},
            "finish_reason": "stop",
        }],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }


def _make_completion(model: str, text: str, prompt_tokens: int = 8, completion_tokens: int = 10) -> dict:
    return {
        "id": "cmpl-stub",
        "object": "text_completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "text": text,
            "index": 0,
            "logprobs": None,
            "finish_reason": "stop",
        }],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }


# ---------------------------------------------------------------------------
# Request handlers
# ---------------------------------------------------------------------------

async def _handle_chat_completions(request: Request) -> Response:
    global _total_requests
    t_start = time.perf_counter()

    if _LATENCY_S > 0:
        await asyncio.sleep(_LATENCY_S)

    # Drain the request body so the HTTP/2 flow-control window is released.
    # We intentionally skip json.loads() — the stub doesn't need the content,
    # and parsing is O(payload_size) work that is a measurable bottleneck at
    # high RPS with large prompts.
    await request.body()

    elapsed = time.perf_counter() - t_start
    now = time.time()
    with _lock:
        _total_requests += 1
        _request_timestamps.append(now)
        _latency_samples.append(elapsed)

    return Response(
        content=_CHAT_RESPONSE_BYTES,
        media_type="application/json",
    )


async def _handle_completions(request: Request) -> Response:
    global _total_requests
    t_start = time.perf_counter()

    if _LATENCY_S > 0:
        await asyncio.sleep(_LATENCY_S)

    await request.body()

    elapsed = time.perf_counter() - t_start
    now = time.time()
    with _lock:
        _total_requests += 1
        _request_timestamps.append(now)
        _latency_samples.append(elapsed)

    return Response(
        content=_COMPLETION_RESPONSE_BYTES,
        media_type="application/json",
    )


async def _handle_health(request: Request) -> Response:
    return Response("OK", status_code=200)


async def _handle_models(request: Request) -> JSONResponse:
    return JSONResponse({
        "object": "list",
        "data": [{"id": _MODEL_ID, "object": "model", "created": 0, "owned_by": "stub"}],
    })


async def _handle_metrics(request: Request) -> JSONResponse:
    now = time.time()
    with _lock:
        total = _total_requests
        errors = _total_errors
        timestamps = list(_request_timestamps)
        latencies = list(_latency_samples)

    # Requests in the last 1s and 10s windows
    rps_1s = sum(1 for t in timestamps if now - t <= 1.0)
    rps_10s = sum(1 for t in timestamps if now - t <= 10.0) / 10.0

    lat_p50 = lat_p99 = None
    if latencies:
        sorted_lat = sorted(latencies)
        n = len(sorted_lat)
        lat_p50 = sorted_lat[int(n * 0.50)]
        lat_p99 = sorted_lat[min(int(n * 0.99), n - 1)]

    return JSONResponse({
        "total_requests": total,
        "total_errors": errors,
        "rps_last_1s": rps_1s,
        "rps_last_10s": round(rps_10s, 2),
        "server_latency_p50_s": lat_p50,
        "server_latency_p99_s": lat_p99,
        "sample_count": len(latencies),
        "server_time": now,
    })


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

def build_app() -> Starlette:
    routes = [
        Route("/v1/chat/completions", _handle_chat_completions, methods=["POST"]),
        Route("/v1/completions",      _handle_completions,      methods=["POST"]),
        Route("/health",              _handle_health,            methods=["GET", "HEAD"]),
        Route("/health/liveliness",   _handle_health,            methods=["GET", "HEAD"]),
        Route("/v1/models",           _handle_models,            methods=["GET"]),
        Route("/metrics",             _handle_metrics,           methods=["GET"]),
    ]
    return Starlette(routes=routes)


def main():
    global _MODEL_ID, _LATENCY_S, _RESPONSE_TOKENS, _RESPONSE_TEXT
    global _CHAT_RESPONSE_BYTES, _COMPLETION_RESPONSE_BYTES

    parser = argparse.ArgumentParser(
        description="Minimal OpenAI-compatible stub HTTP server for benchmarking."
    )
    parser.add_argument("--port",            type=int,   default=8000,         help="Port to listen on")
    parser.add_argument("--host",            type=str,   default="0.0.0.0",    help="Host/IP to bind")
    parser.add_argument("--latency-ms",      type=float, default=0.0,          help="Artificial response latency (ms)")
    parser.add_argument("--response-tokens", type=int,   default=10,           help="Output tokens to include in response")
    parser.add_argument("--model",           type=str,   default="stub-model", help="Model name to advertise")
    parser.add_argument("--workers",         type=int,   default=1,            help="(Deprecated) ignored — use --stub-workers in run_bench.sh instead")
    parser.add_argument("--reuse-port",      action="store_true",              help="Bind with SO_REUSEPORT so multiple processes share the same port")
    parser.add_argument("--log-level",       type=str,   default="warning",    help="uvicorn log level")
    args = parser.parse_args()

    _MODEL_ID = args.model
    _LATENCY_S = args.latency_ms / 1000.0
    _RESPONSE_TOKENS = args.response_tokens
    _RESPONSE_TEXT = _build_response_text(max(1, _RESPONSE_TOKENS))

    # Pre-build serialized response bytes once so handlers avoid json.dumps()
    # on every request.  This eliminates O(response_size) work per call.
    _CHAT_RESPONSE_BYTES = json.dumps(
        _make_chat_completion(_MODEL_ID, _RESPONSE_TEXT,
                              prompt_tokens=8, completion_tokens=_RESPONSE_TOKENS)
    ).encode()
    _COMPLETION_RESPONSE_BYTES = json.dumps(
        _make_completion(_MODEL_ID, _RESPONSE_TEXT,
                         prompt_tokens=8, completion_tokens=_RESPONSE_TOKENS)
    ).encode()

    print(
        f"[StubServer] Starting on {args.host}:{args.port} | "
        f"latency={args.latency_ms}ms | response_tokens={args.response_tokens} | "
        f"model={_MODEL_ID} | reuse_port={args.reuse_port} | pid={os.getpid()}",
        flush=True,
    )

    app = build_app()

    if args.reuse_port:
        # Create the socket manually with SO_REUSEPORT so multiple independent
        # single-worker processes can all bind to the same port. The Linux kernel
        # load-balances new connections across them. This is more reliable than
        # uvicorn's built-in multiprocess mode (which requires the app to be an
        # importable string and is not fork-safe with uvloop).
        sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
        sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEPORT, 1)
        sock.bind((args.host, args.port))
        sock.set_inheritable(True)
        uvicorn.run(
            app,
            fd=sock.fileno(),
            log_level=args.log_level,
            access_log=False,
        )
        sock.close()
    else:
        uvicorn.run(
            app,
            host=args.host,
            port=args.port,
            log_level=args.log_level,
            access_log=False,
        )


if __name__ == "__main__":
    main()
