"""PR-011 acceptance: OpenAI request validation (hermetic, ray-free)."""

from __future__ import annotations

import pytest

from exaserve.request_validation import (
    ContextLengthExceeded,
    RequestValidationError,
    parse_sampling,
    validate_context_window,
    validate_chat_options,
    validate_messages,
    validate_model_field,
    validate_prompt,
)


def test_valid_sampling_normalized():
    s = parse_sampling({"temperature": 0.5, "top_p": 0.9, "max_tokens": 32})
    assert s["temperature"] == 0.5 and s["top_p"] == 0.9 and s["max_tokens"] == 32
    # defaults applied
    assert parse_sampling({})["max_tokens"] == 1024


@pytest.mark.parametrize(
    "body",
    [
        {"temperature": "hot"},  # was ValueError -> 500
        {"temperature": 5.0},  # out of range
        {"top_p": 2.0},  # out of range
        {"max_tokens": -1},  # non-positive
        {"max_tokens": 10**9},  # absurd
        {"max_tokens": "lots"},  # wrong type
        {"max_tokens": True},  # bool is not int here
        {"min_tokens": 100, "max_tokens": 10},  # min > max
        {"stop": 42},  # wrong type
    ],
)
def test_invalid_sampling_raises_400_class(body):
    with pytest.raises(RequestValidationError):
        parse_sampling(body)


def test_model_field_absent_ok_mismatch_rejected():
    allowed = {"meta-llama/Meta-Llama-3-8B-Instruct", "meta-llama--Meta-Llama-3-8B-Instruct"}
    validate_model_field({}, allowed)  # absent is fine
    validate_model_field({"model": "meta-llama/Meta-Llama-3-8B-Instruct"}, allowed)
    with pytest.raises(RequestValidationError, match="unknown model"):
        validate_model_field({"model": "gpt-4"}, allowed)


def test_messages_validation():
    validate_messages([{"role": "user", "content": "hi"}])
    validate_messages([{"role": "user", "content": [{"type": "text", "text": "hi"}]}])
    for bad in (
        [],
        "not a list",
        [{"content": "no role"}],
        [{"role": 7, "content": "hi"}],
        [{"role": "user", "content": 7}],
        [{"role": "user", "content": ["not an object part"]}],
        [{"role": "user", "content": [{"type": "image_url", "image_url": "x"}]}],
        [{"role": "user", "content": [{"type": "text", "text": 7}]}],
        [{"role": "user", "content": [{"type": "text", "text": "x", "extra": True}]}],
    ):
        with pytest.raises(RequestValidationError):
            validate_messages(bad)


def test_chat_template_options_are_typed_before_backend_expansion():
    validate_chat_options({})
    validate_chat_options(
        {"chat_template": "{{ messages }}", "chat_template_kwargs": {"tools": []}}
    )
    for bad in (
        {"chat_template": 7},
        {"chat_template_kwargs": []},
    ):
        with pytest.raises(RequestValidationError):
            validate_chat_options(bad)


def test_completion_prompt_rejects_unimplemented_batch_forms():
    assert validate_prompt("one prompt") == "one prompt"
    for unsupported in (["one", "two"], [1, 2], None, 7):
        with pytest.raises(RequestValidationError, match="prompt must be a string"):
            validate_prompt(unsupported)


def test_context_window_accepts_the_exact_boundary_and_rejects_one_token_over():
    validate_context_window(
        prompt_tokens=5,
        requested_completion_tokens=3,
        max_model_len=8,
        prompt_param="prompt",
    )

    with pytest.raises(ContextLengthExceeded) as excinfo:
        validate_context_window(
            prompt_tokens=5,
            requested_completion_tokens=4,
            max_model_len=8,
            prompt_param="prompt",
        )
    error = excinfo.value
    assert error.code == "context_length_exceeded"
    assert error.error_type == "invalid_request_error"
    assert error.param == "max_tokens"
    assert error.total_tokens == 9


def test_context_window_attributes_a_prompt_that_alone_exceeds_the_limit():
    with pytest.raises(ContextLengthExceeded) as excinfo:
        validate_context_window(
            prompt_tokens=9,
            requested_completion_tokens=1,
            max_model_len=8,
            prompt_param="messages",
        )
    assert excinfo.value.param == "messages"
