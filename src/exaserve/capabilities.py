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
from typing import Callable, Optional


class CapabilityUnavailable(RuntimeError):
    """A configuration requests a capability this deployment cannot provide."""


@dataclass(frozen=True)
class Capability:
    name: str
    summary: str
    enabled_by: str                 # what turns it on
    on_unavailable: str             # "refuse" | "degrade"
    degraded_meaning: str = ""      # what the user gets instead, if degraded


CAPABILITIES: tuple[Capability, ...] = (
    Capability(
        "pp_multi_replica",
        "Pipeline-parallel models served as more than one replica.",
        "EXASERVE_PP_SHARD_AWARE=1 (node-pinned per-stage placement)",
        "refuse",
    ),
    Capability(
        "chat_template_fallback",
        "Serving /v1/chat/completions for a tokenizer with no chat template.",
        "EXASERVE_ALLOW_CHAT_TEMPLATE_FALLBACK=1",
        "refuse",
        "messages are flattened to a plain-text prompt, which is NOT the "
        "format the model was fine-tuned on; outputs are not comparable to a "
        "correctly templated run.",
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


def _shard_aware() -> bool:
    return os.environ.get("EXASERVE_PP_SHARD_AWARE", "0") == "1"


def _chat_fallback_allowed() -> bool:
    return os.environ.get("EXASERVE_ALLOW_CHAT_TEMPLATE_FALLBACK", "0") == "1"


def _thread_guards_present() -> bool:
    return bool(os.environ.get("RAYON_NUM_THREADS")
                and os.environ.get("TOKENIZERS_PARALLELISM"))


_PROBES: dict[str, Callable[[], bool]] = {
    "pp_multi_replica": _shard_aware,
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
        f"{capability.summary} Enable with: {capability.enabled_by}.")


def degrade_or_refuse(name: str, context: str = "",
                      log: Callable[[str], None] = print) -> bool:
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
            f"Enable with: {capability.enabled_by}."
            + (f" ({context})" if context else ""))
    log(f"[Capability] DEGRADED {capability.name}: {capability.degraded_meaning} "
        f"Enable with: {capability.enabled_by}."
        + (f" ({context})" if context else ""))
    return False


def validate_deployment(config, *, log: Callable[[str], None] = print) -> dict:
    """Check a deployment config against declared capabilities (KI-D4).

    Returns the capability report recorded in the readiness snapshot, so an
    operator can see which capabilities a run actually had.
    """
    report: dict[str, bool] = {}
    for capability in CAPABILITIES:
        report[capability.name] = available(capability.name)

    for model in getattr(config, "model_configs", []) or []:
        pp = getattr(model, "pipeline_parallel_size", 1) or 1
        replicas = getattr(model, "num_replicas", None)
        if pp > 1 and replicas is not None and replicas > 1:
            require("pp_multi_replica",
                    f"{model.model_id}: pipeline_parallel_size={pp} with "
                    f"num_replicas={replicas}")

    if not report["thread_oversubscription_guard"]:
        degrade_or_refuse("thread_oversubscription_guard",
                          "deployment environment", log=log)
    return report


def report() -> dict:
    return {c.name: available(c.name) for c in CAPABILITIES}
