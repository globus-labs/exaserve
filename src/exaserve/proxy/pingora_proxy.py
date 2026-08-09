"""
Pingora backend implementation.

Writes a small YAML config consumed by the project's custom pingora_lb
binary (scripts/pingora_lb/).  The composition root owns process launch.

Pingora is Cloudflare's Rust async-I/O proxy framework. Unlike HAProxy and
NGINX (single process, single-thread accept loops historically) Pingora
runs an N-thread tokio runtime out of the box, which makes it the most
interesting candidate to break the HAProxy single-process plateau seen at
256+ nodes.

pingora_lb must be installed and on PATH (see scripts/build_pingora.sh).
"""

from collections import defaultdict
from pathlib import Path

from .base import (
    BackendEndpoint,
    ProxyBackend,
    reject_unknown_options,
    strict_int,
    strict_text,
    validate_endpoint,
)


class PingoraProxy(ProxyBackend):
    """Render the Pingora artifact consumed by the composition root."""

    def generate_config(
        self,
        backends: list[BackendEndpoint],
        output_dir: Path,
        **options,
    ) -> Path:
        """
        Write pingora.yaml to output_dir. Returns the config path.

        Options (all optional):
            lb_method (str):  "round_robin" | "least_request". Default "round_robin".
            threads (int):    Worker thread count (0 = all cores). Default: 0.
            connect_timeout_ms (int):  Default: 5000.
            request_timeout_ms (int):  0 = unbounded. Default: 330000.
            health_check_interval_s (int): 0 = disabled. Default: 5.

        Multi-model deployments are NOT supported by the minimal Rust binary;
        we raise here if asked to mix models. Add per-route dispatch in
        scripts/pingora_lb/src/main.rs if needed.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        reject_unknown_options(
            options,
            {
                "connect_timeout_ms",
                "health_check_interval_s",
                "lb_method",
                "listen_port",
                "request_timeout_ms",
                "threads",
            },
            "pingora",
        )

        by_model: dict[str, list[BackendEndpoint]] = defaultdict(list)
        for ep in backends:
            validate_endpoint(ep, "pingora")
            by_model[ep.model_id].append(ep)
        if len(by_model) > 1:
            raise ValueError(
                "PingoraProxy currently supports a single model_id only "
                f"(got {sorted(by_model)}). Extend scripts/pingora_lb/src/main.rs."
            )
        if not by_model:
            raise ValueError("Pingora requires at least one backend endpoint")

        eps = next(iter(by_model.values()))
        upstreams = [f"{ep.host}:{ep.port}" for ep in eps]

        listen_port = strict_int(
            options.get("listen_port", 4001), "proxy.options.listen_port", minimum=1, maximum=65535
        )
        cfg = {
            "listen": f"0.0.0.0:{listen_port}",
            "lb_method": strict_text(
                options.get("lb_method", "round_robin"),
                "proxy.options.lb_method",
                choices={"round_robin", "least_request"},
            ),
            "upstreams": upstreams,
            "threads": strict_int(
                options.get("threads", 0), "proxy.options.threads", minimum=0, maximum=4096
            ),
            "connect_timeout_ms": strict_int(
                options.get("connect_timeout_ms", 5000),
                "proxy.options.connect_timeout_ms",
                minimum=1,
                maximum=3_600_000,
            ),
            "request_timeout_ms": strict_int(
                options.get("request_timeout_ms", 330000),
                "proxy.options.request_timeout_ms",
                minimum=0,
                maximum=86_400_000,
            ),
            "health_check_interval_s": strict_int(
                options.get("health_check_interval_s", 5),
                "proxy.options.health_check_interval_s",
                minimum=0,
                maximum=3600,
            ),
        }

        config_path = output_dir / "pingora.yaml"
        from ..state.atomic import atomic_create_or_verify_yaml

        atomic_create_or_verify_yaml(config_path, cfg, default_flow_style=False, sort_keys=False)

        print(
            f"[PingoraProxy] Config written to {config_path} "
            f"({len(upstreams)} upstream(s), lb={cfg['lb_method']}, "
            f"threads={cfg['threads']})"
        )
        return config_path
