"""Test-support factories importable across process boundaries.

These are referenced by dotted path from experiment specs (e.g.
``trace.tokenizer_builder: eval.testing:whitespace_tokenizer_map``) so that
hermetic tests never load real tokenizers or touch the network, including
inside forkserver trace-materialization workers (WP0 baseline repair; see
doc/hardening/BASELINE.md F-01..F-06).
"""

from __future__ import annotations

from typing import Any


class _WhitespaceTokenizer:
    """Minimal tokenizer contract used by trace generation.

    Tokens are whitespace-separated words; encode/decode round-trips are
    stable, which is all ``_truncate_prompt`` requires.
    """

    model_max_length = 100_000_000

    def encode(self, text: str, add_special_tokens: bool = False) -> list[str]:
        return text.split()

    def decode(self, tokens: list[str], skip_special_tokens: bool = True) -> str:
        return " ".join(tokens)


def whitespace_tokenizer_map(spec: Any) -> dict[str, Any]:
    return {model.model_id: _WhitespaceTokenizer() for model in spec.deployment.models}
