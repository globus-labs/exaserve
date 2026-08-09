"""Import-light prompt serialization shared by engine adapters.

This module intentionally has no Ray or engine dependency so capability and
request-contract tests remain valid in the hermetic core installation.
"""

from __future__ import annotations

from typing import Any


def message_content_to_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if not (
                isinstance(item, dict)
                and set(item) == {"type", "text"}
                and item.get("type") == "text"
                and isinstance(item.get("text"), str)
            ):
                raise ValueError("chat content contains an unsupported non-text part")
            parts.append(item["text"])
        return " ".join(part for part in parts if part)
    raise ValueError(f"unsupported chat content type {type(content).__name__}")


def chat_messages_to_plain_prompt(messages: list[dict], *, add_generation_prompt: bool) -> str:
    """Serialize chat messages for a tokenizer without a native template."""
    lines = []
    for message in messages:
        if not isinstance(message, dict):
            raise ValueError("chat message must be an object")
        role = message.get("role")
        if not isinstance(role, str) or not role.strip():
            raise ValueError("chat message role must be nonempty text")
        role = role.strip()
        content = message_content_to_text(message.get("content"))
        if content:
            lines.append(f"{role.title()}: {content}")
    if add_generation_prompt:
        lines.append("Assistant:")
    return "\n".join(lines).strip()


__all__ = ["chat_messages_to_plain_prompt", "message_content_to_text"]
