"""Declared deployment capabilities (plan WP3, audit KI-D4, TD-PP-MULTI, TD-CHATTPL, KI-A5).

Some things ExaServe can do are only correct under conditions that are not
visible in the config that requests them. Pipeline parallelism with more than
one replica works through the shard-aware path and nowhere else; a tokenizer
without a chat template can be served with a plain-text rendering, but that is
a *different* prompt than the model was trained on; the thread-count guards
that keep tokenizer forks from oversubscribing a node are ambient shell
defaults that vanish the moment something is launched another way.

Each of those was previously handled where it happened — a silent fallback, an
`export` in a launcher script, a path that quietly did something else. The
failure mode is the same in every case: the deployment runs, produces output,
and the output is not what the operator asked for.

A capability names the thing, states what enables it, and says what happens
when it is unavailable — refuse, or degrade loudly and record it. Nothing is
allowed to degrade silently.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable


class CapabilityUnavailable(RuntimeError):
    """A configuration requests a capability this deployment cannot provide."""


@dataclass(frozen=True)
class Capability:
    name: str
    summary: str
    enabled_by: str  # what turns it on
    on_unavailable: str  # "refuse" | "degrade"
    degraded_meaning: str = ""  # what the user gets instead, if degraded


CAPABILITIES: tuple[Capability, ...] = (
    Capability(
        "chat_template_fallback",
        "Serving /v1/chat/completions for a tokenizer with no chat template.",
        "an explicit compatible chat_template or tokenizer configuration",
        "refuse",
        "messages are flattened to a plain-text prompt, which is NOT the "
        "format the model was fine-tuned on; outputs are not comparable to a "
        "correctly templated run.",
    ),
    Capability(
        "real_streaming_metrics",
        "Per-token streaming latency (TBT) measured against a real streaming transport.",
        "a proxy that forwards SSE token-by-token (direct, haproxy, envoy, "
        "nginx, pingora) — NOT litellm",
        "refuse",
        "litellm buffers and emits the whole completion at the end, so its TBT "
        "distribution is degenerate (~0) and its TTFT absorbs the entire "
        "generation. Mixing it into a real-streaming comparison compares two "
        "different things.",
    ),
    Capability(
        "thread_oversubscription_guard",
        "Bounded tokenizer/rayon thread counts so forks cannot oversubscribe a node.",
        "RAYON_NUM_THREADS and TOKENIZERS_PARALLELISM set in the environment",
        "degrade",
        "thread counts are left to library defaults, which oversubscribe at "
        "high replica counts per node.",
    ),
)


def get(name: str) -> Capability:
    for capability in CAPABILITIES:
        if capability.name == name:
            return capability
    raise KeyError(f"undeclared capability {name!r}")


def _chat_fallback_allowed() -> bool:
    # Flattening structured messages changes the model input and answer. There
    # is deliberately no process-global escape hatch for that substitution.
    return False


def _thread_guards_present() -> bool:
    return bool(os.environ.get("RAYON_NUM_THREADS") and os.environ.get("TOKENIZERS_PARALLELISM"))


def _real_streaming(dest_or_proxy: str = "") -> bool:
    """litellm is the only supported proxy that fake-streams."""
    return "litellm" not in str(dest_or_proxy).lower()


_PROBES: dict[str, Callable[[], bool]] = {
    "real_streaming_metrics": lambda: True,  # checked per-run, see require_streaming
    "chat_template_fallback": _chat_fallback_allowed,
    "thread_oversubscription_guard": _thread_guards_present,
}


def available(name: str) -> bool:
    return _PROBES[get(name).name]()


def require(name: str, context: str = "") -> None:
    """Raise unless the capability is available. Message says how to enable it."""
    capability = get(name)
    if available(name):
        return
    where = f" ({context})" if context else ""
    raise CapabilityUnavailable(
        f"capability {capability.name!r} is required{where} but not enabled. "
        f"{capability.summary} Enable with: {capability.enabled_by}."
    )


def degrade_or_refuse(name: str, context: str = "", log: Callable[[str], None] = print) -> bool:
    """Return True if the capability is available.

    When it is not: refuse (raise) for a capability whose absence changes the
    ANSWER, and degrade loudly for one whose absence only changes performance.
    A silent fallback is never an option here.
    """
    capability = get(name)
    if available(name):
        return True
    if capability.on_unavailable == "refuse":
        raise CapabilityUnavailable(
            f"{capability.name}: {capability.degraded_meaning or capability.summary} "
            f"Refusing rather than silently substituting. "
            f"Enable with: {capability.enabled_by}." + (f" ({context})" if context else "")
        )
    log(
        f"[Capability] DEGRADED {capability.name}: {capability.degraded_meaning} "
        f"Enable with: {capability.enabled_by}." + (f" ({context})" if context else "")
    )
    return False


def require_streaming_comparison(proxy_type: str, streaming: bool) -> None:
    """KI-B3: refuse to record streaming latency from a fake-streaming proxy.

    litellm buffers the completion and emits it in one burst, so its TBT is
    degenerate and its TTFT absorbs the whole generation. A run that records
    those numbers alongside real-streaming numbers is comparing two different
    things, and the comparison looks plausible.
    """
    if not streaming:
        return
    if not _real_streaming(proxy_type):
        capability = get("real_streaming_metrics")
        raise CapabilityUnavailable(
            f"streaming metrics requested through proxy {proxy_type!r}: "
            f"{capability.degraded_meaning} Enable with: {capability.enabled_by}."
        )


def validate_deployment(config, *, log: Callable[[str], None] = print) -> dict:
    """Check a deployment config against declared capabilities (KI-D4).

    Returns the capability report recorded in the readiness snapshot, so an
    operator can see which capabilities a run actually had.
    """
    report: dict[str, bool] = {}
    for capability in CAPABILITIES:
        report[capability.name] = available(capability.name)

    if not report["thread_oversubscription_guard"]:
        degrade_or_refuse("thread_oversubscription_guard", "deployment environment", log=log)
    return report


def report() -> dict:
    return {c.name: available(c.name) for c in CAPABILITIES}
