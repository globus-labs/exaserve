"""PR-011 acceptance: OpenAI request validation (hermetic, ray-free)."""

from __future__ import annotations

import pytest

from exaserve.request_validation import (
    RequestValidationError,
    parse_sampling,
    validate_messages,
    validate_model_field,
)


def test_valid_sampling_normalized():
    s = parse_sampling({"temperature": 0.5, "top_p": 0.9, "max_tokens": 32})
    assert s["temperature"] == 0.5 and s["top_p"] == 0.9 and s["max_tokens"] == 32
    # defaults applied
    assert parse_sampling({})["max_tokens"] == 1024


@pytest.mark.parametrize("body", [
    {"temperature": "hot"},          # was ValueError -> 500
    {"temperature": 5.0},            # out of range
    {"top_p": 2.0},                  # out of range
    {"max_tokens": -1},              # non-positive
    {"max_tokens": 10**9},           # absurd
    {"max_tokens": "lots"},          # wrong type
    {"max_tokens": True},            # bool is not int here
    {"min_tokens": 100, "max_tokens": 10},  # min > max
    {"stop": 42},                    # wrong type
])
def test_invalid_sampling_raises_400_class(body):
    with pytest.raises(RequestValidationError):
        parse_sampling(body)


def test_model_field_absent_ok_mismatch_rejected():
    allowed = {"meta-llama/Meta-Llama-3-8B-Instruct",
               "meta-llama--Meta-Llama-3-8B-Instruct"}
    validate_model_field({}, allowed)  # absent is fine
    validate_model_field({"model": "meta-llama/Meta-Llama-3-8B-Instruct"}, allowed)
    with pytest.raises(RequestValidationError, match="unknown model"):
        validate_model_field({"model": "gpt-4"}, allowed)


def test_messages_validation():
    validate_messages([{"role": "user", "content": "hi"}])
    for bad in ([], "not a list", [{"content": "no role"}]):
        with pytest.raises(RequestValidationError):
            validate_messages(bad)
