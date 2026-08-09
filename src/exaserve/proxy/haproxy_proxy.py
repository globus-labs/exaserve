"""
HAProxy backend implementation.

Generates an haproxy.cfg that balances across Ray Serve HTTP proxies, then
launches the haproxy binary.

HAProxy is the sole first-release production gateway for the trusted internal
Aurora envelope. It is a pure load balancer: ExaServe supplies lifecycle,
route/canary validation, body bounds, and a read-only loopback stats surface;
it does not provide public-Internet authentication, TLS termination, API-key
management, usage tracking, or JSON-body model routing.

haproxy must be installed and on PATH.
"""

import hashlib
import ipaddress
import re
from pathlib import Path
from textwrap import dedent

from .base import BackendEndpoint, ProxyBackend, reject_unknown_options


def _strict_opt_bool(value: object, path: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{path}: expected a boolean, got {value!r}")


def _strict_opt_int(value: object, path: str, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{path}: expected an integer, got {value!r}")
    if not minimum <= value <= maximum:
        raise ValueError(f"{path}: expected {minimum}..{maximum}, got {value}")
    return value


class HAProxyProxy(ProxyBackend):
    """Render the HAProxy artifact consumed by the composition root."""

    def generate_config(
        self,
        backends: list[BackendEndpoint],
        output_dir: Path,
        **options,
    ) -> Path:
        """
        Write haproxy.cfg to output_dir.

        Options (all optional):
            balance (str):       Load-balancing algorithm.
                                 "leastconn" | "roundrobin" | "random"
                                 Default: "leastconn".
            check_interval (int): Health check interval in ms. Default: 5000.
            check_fall (int):    Consecutive failures before marking backend down.
                                 Default: 3.
            check_rise (int):    Consecutive successes to mark backend up. Default: 2.
            stats_port (int):    HAProxy stats page port. Default: 9999.
                                 Set to 0 to disable stats.
            maxconn (int):       Max concurrent connections. Default: 8000,
                                 which fits Aurora's 16384-descriptor hard limit
                                 through the currently qualified 64-node tier.

        Note: HAProxy uses one backend *per unique model_id*. Nodes serving the
        same model are grouped together. If all backends serve the same model (the
        common case), there is one backend section named after that model.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        reject_unknown_options(
            options,
            {
                "abortonclose",
                "balance",
                "bind_target",
                "check_fall",
                "check_interval",
                "check_rise",
                "http_no_delay",
                "listen_port",
                "maxconn",
                "nbthread",
                "request_body_limit_bytes",
                "stats_admin",
                "stats_auth",
                "stats_bind",
                "stats_port",
            },
            "haproxy",
        )

        balance = options.get("balance", "leastconn")
        if balance not in {"leastconn", "roundrobin", "random"}:
            raise ValueError("proxy.options.balance is unsupported")
        check_interval = _strict_opt_int(
            options.get("check_interval", 5000),
            "proxy.options.check_interval",
            minimum=100,
            maximum=300000,
        )
        check_fall = _strict_opt_int(
            options.get("check_fall", 3), "proxy.options.check_fall", minimum=1, maximum=100
        )
        check_rise = _strict_opt_int(
            options.get("check_rise", 2), "proxy.options.check_rise", minimum=1, maximum=100
        )
        stats_port = _strict_opt_int(
            options.get("stats_port", 9999), "proxy.options.stats_port", minimum=0, maximum=65535
        )
        maxconn = _strict_opt_int(
            options.get("maxconn", 8000), "proxy.options.maxconn", minimum=1, maximum=10_000_000
        )
        request_body_limit_bytes = _strict_opt_int(
            options.get("request_body_limit_bytes", 16 << 20),
            "proxy.options.request_body_limit_bytes",
            minimum=1,
            maximum=1 << 40,
        )
        http_no_delay = _strict_opt_bool(
            options.get("http_no_delay", True), "proxy.options.http_no_delay"
        )
        # `option abortonclose` propagates a client close to the backend even
        # while the response is pending (zero bytes sent). Without it a wedged
        # stream is only reaped by `timeout server` (330s) and the engine keeps
        # decoding the abandoned request. Off by default to keep configs
        # byte-identical to prior runs; enable for saturation probing, where
        # zombie decodes deflate the measured ceiling.
        abortonclose = _strict_opt_bool(
            options.get("abortonclose", False), "proxy.options.abortonclose"
        )
        # HAProxy parallelism = threads (modern HAProxy is threaded, not multi-proc).
        # 0/unset -> omit nbthread (HAProxy auto-detects = bound CPUs). Set
        # options.nbthread (alias: num_workers) to pin more accept/processing threads
        # — relevant to the 256n connection-bound streaming case.
        nbthread = _strict_opt_int(
            options.get("nbthread", 0), "proxy.options.nbthread", minimum=0, maximum=4096
        )
        default_port = _strict_opt_int(
            options.get("listen_port", 4001), "proxy.options.listen_port", minimum=1, maximum=65535
        )
        bind_target = options.get("bind_target", f"*:{default_port}")
        if not isinstance(bind_target, str):
            raise ValueError("proxy.options.bind_target is invalid")
        port_match = re.fullmatch(r"\*:([1-9][0-9]{0,4})", bind_target)
        fd_match = re.fullmatch(r"fd@([0-9]+)", bind_target)
        if not port_match and not fd_match:
            raise ValueError("proxy.options.bind_target is invalid")
        if port_match and int(port_match.group(1)) > 65535:
            raise ValueError("proxy.options.bind_target port is out of range")

        # Group endpoints by model_id so each model gets its own backend section
        from collections import defaultdict

        by_model: dict[str, list[BackendEndpoint]] = defaultdict(list)
        for ep in backends:
            _validate_endpoint(ep)
            by_model[ep.model_id].append(ep)
        if not by_model:
            raise ValueError("HAProxy requires at least one backend endpoint")

        lines: list[str] = []

        # --- global section ---
        # `option http-no-delay` forwards each SSE token chunk immediately instead
        # of coalescing, so per-request TBT reflects true decode cadence (without
        # it HAProxy bursts the token stream ~230ms vs ~22ms/token). It lives in
        # `defaults`, so it also applies to NON-streaming traffic and disables
        # output coalescing for every response. Set `http_no_delay: false` to drop
        # it (e.g. non-streaming throughput tests, where coalescing matters at
        # scale). Default True preserves the streaming-SLO behavior.
        no_delay = "\n                option http-no-delay" if http_no_delay else ""
        abort_line = "\n                option abortonclose" if abortonclose else ""
        nbthread_line = f"\n                nbthread {nbthread}" if nbthread > 0 else ""
        lines.append(
            dedent(f"""\
            global
                maxconn {maxconn}{nbthread_line}
                log stdout format raw local0 info

            defaults
                mode http{no_delay}{abort_line}
                timeout connect 5s
                timeout client  330s
                timeout server  330s
                option http-server-close
                option forwardfor
                log global
            """)
        )

        # --- frontend ---
        # A single frontend receives all incoming OpenAI API requests.
        # For multi-model deployments, requests must already target the
        # per-model Ray Serve route prefix because HAProxy does not inspect
        # the OpenAI JSON body to recover the model name.
        if len(by_model) == 1:
            model_id = next(iter(by_model))
            safe_name = _safe_backend_name(model_id)
            lines.append(
                dedent(f"""\
                frontend openai_api
                    bind {bind_target}
                    acl body_method method POST PUT PATCH
                    acl has_length req.hdr(content-length) -m found
                    acl chunked_body req.hdr(transfer-encoding) -m sub chunked
                    acl body_too_large req.hdr_val(content-length) gt {request_body_limit_bytes}
                    http-request return status 413 content-type text/plain string "Chunked request bodies are unsupported" if body_method chunked_body
                    http-request return status 411 content-type text/plain string "Length Required" if body_method !has_length
                    http-request return status 413 content-type text/plain string "Request body too large" if body_method body_too_large
                    default_backend {safe_name}
                """)
            )
        else:
            fallback_backend = "exaserve_unknown_route"
            used_backend_names = {_safe_backend_name(model_id) for model_id in by_model}
            while fallback_backend in used_backend_names:
                fallback_backend += "_"
            lines.append("frontend openai_api")
            lines.append(f"    bind {bind_target}")
            lines.append("    acl body_method method POST PUT PATCH")
            lines.append("    acl has_length req.hdr(content-length) -m found")
            lines.append("    acl chunked_body req.hdr(transfer-encoding) -m sub chunked")
            lines.append(
                f"    acl body_too_large req.hdr_val(content-length) gt {request_body_limit_bytes}"
            )
            lines.append(
                "    http-request return status 413 content-type text/plain "
                'string "Chunked request bodies are unsupported" '
                "if body_method chunked_body"
            )
            lines.append(
                "    http-request return status 411 content-type text/plain "
                'string "Length Required" if body_method !has_length'
            )
            lines.append(
                "    http-request return status 413 content-type text/plain "
                'string "Request body too large" if body_method body_too_large'
            )
            for model_id, eps in by_model.items():
                safe_name = _safe_backend_name(model_id)
                path_prefix = _shared_path_prefix(eps)
                acl_name = f"is_{safe_name}"
                lines.append(f"    acl {acl_name} path {path_prefix}")
                lines.append(f"    acl {acl_name} path_beg {path_prefix}/")
                lines.append(f"    use_backend {safe_name} if {acl_name}")
            # HTTP request rules are evaluated before use_backend rules even
            # when written after them.  An unconditional fallback return here
            # therefore rejected *every* multi-model request.  Route unmatched
            # traffic to an explicit terminal backend instead.
            lines.append(f"    default_backend {fallback_backend}")
            lines.append("")
            lines.extend(
                [
                    f"backend {fallback_backend}",
                    "    http-request return status 404 content-type text/plain "
                    'lf-string "missing or unknown model route prefix\\n"',
                    "",
                ]
            )

        # --- backend section(s) ---
        if len(by_model) == 1:
            model_id, eps = next(iter(by_model.items()))
            safe_name = _safe_backend_name(model_id)
            path_prefix = _shared_path_prefix(eps)
            lines.append(
                _render_backend(
                    name=safe_name,
                    endpoints=eps,
                    path_prefix=path_prefix,
                    balance=balance,
                    check_interval=check_interval,
                    check_fall=check_fall,
                    check_rise=check_rise,
                )
            )
        else:
            for model_id, eps in by_model.items():
                safe_name = _safe_backend_name(model_id)
                path_prefix = _shared_path_prefix(eps)
                lines.append(
                    _render_backend(
                        name=safe_name,
                        endpoints=eps,
                        path_prefix=path_prefix,
                        balance=balance,
                        check_interval=check_interval,
                        check_fall=check_fall,
                        check_rise=check_rise,
                    )
                )

        # --- optional stats page ---
        # PR-010: `stats admin if TRUE` on a wildcard bind let ANY reachable
        # host enable/disable backends unauthenticated. Default to a
        # read-only page bound to loopback; admin and external binds are
        # explicit opt-ins and admin then REQUIRES auth.
        if stats_port > 0:
            stats_bind = options.get("stats_bind", "127.0.0.1")
            if not isinstance(stats_bind, str):
                raise ValueError("proxy.options.stats_bind must be an IP address")
            try:
                ipaddress.ip_address(stats_bind)
            except ValueError as exc:
                raise ValueError("proxy.options.stats_bind must be an IP address") from exc
            stats_admin = _strict_opt_bool(
                options.get("stats_admin", False), "proxy.options.stats_admin"
            )
            stats_auth = options.get("stats_auth")  # "user:password"
            if stats_auth is not None and (
                not isinstance(stats_auth, str)
                or ":" not in stats_auth
                or any(char in stats_auth for char in "\r\n")
            ):
                raise ValueError("proxy.options.stats_auth must be a single-line user:password")
            if stats_admin and not stats_auth:
                raise ValueError(
                    "proxy.options.stats_admin requires proxy.options.stats_auth "
                    "(user:password); refusing to emit an unauthenticated "
                    "HAProxy admin socket (PR-010)"
                )
            stats_lines = [
                "listen stats",
                f"    bind {stats_bind}:{stats_port}",
                "    stats enable",
                "    stats uri /stats",
                "    stats refresh 10s",
            ]
            if stats_auth:
                stats_lines.append(f"    stats auth {stats_auth}")
            if stats_admin:
                stats_lines.append("    stats admin if TRUE")
            lines.append("\n".join(stats_lines) + "\n")

        config_text = "\n".join(lines)

        config_path = output_dir / "haproxy.cfg"
        from ..state.atomic import atomic_create_or_verify_text

        atomic_create_or_verify_text(config_path, config_text)

        total_servers = sum(len(eps) for eps in by_model.values())
        print(
            f"[HAProxyProxy] Config written to {config_path} "
            f"({len(by_model)} model(s), {total_servers} server entries, balance={balance})"
        )
        return config_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_backend_name(model_id: str) -> str:
    """Return a bounded, collision-resistant HAProxy identifier.

    Character replacement alone maps distinct valid model IDs such as ``a/b``
    and ``a-b`` to the same backend name.  A short content hash preserves exact
    identity while the bounded stem keeps generated tokens within HAProxy's
    practical identifier limits.
    """
    stem = re.sub(r"[^A-Za-z0-9_]", "_", model_id).strip("_") or "backend"
    digest = hashlib.sha256(model_id.encode("utf-8")).hexdigest()[:12]
    return f"{stem[:48]}_{digest}"


def required_nofile(maxconn: int, backend_servers: int) -> int:
    """Conservative descriptor budget for a generated HAProxy process.

    HAProxy needs roughly two descriptors per client connection plus listeners,
    health-check sockets, peers, and internal pipes.  The fixed reserve is
    intentionally above the value reported by HAProxy 3.1 for the qualified
    config. Runtime verifies this against RLIMIT_NOFILE before launch.
    """
    if isinstance(maxconn, bool) or not isinstance(maxconn, int) or maxconn < 1:
        raise ValueError("HAProxy maxconn must be a positive integer")
    if (
        isinstance(backend_servers, bool)
        or not isinstance(backend_servers, int)
        or backend_servers < 1
    ):
        raise ValueError("HAProxy backend server count must be positive")
    return 2 * maxconn + backend_servers + 256


def _validate_endpoint(endpoint: BackendEndpoint) -> None:
    host = endpoint.host
    if (
        not isinstance(host, str)
        or len(host) > 253
        or not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?", host)
        or ".." in host
    ):
        raise ValueError(f"unsafe HAProxy backend host {host!r}")
    if (
        isinstance(endpoint.port, bool)
        or not isinstance(endpoint.port, int)
        or not 1 <= endpoint.port <= 65535
    ):
        raise ValueError(f"invalid HAProxy backend port {endpoint.port!r}")
    if (
        not isinstance(endpoint.model_id, str)
        or not endpoint.model_id
        or len(endpoint.model_id) > 1024
        or any(ord(char) < 32 for char in endpoint.model_id)
    ):
        raise ValueError("HAProxy backend model_id must be non-empty")
    if endpoint.path_prefix and not re.fullmatch(r"/[A-Za-z0-9_-]+", endpoint.path_prefix):
        raise ValueError(f"unsafe HAProxy route prefix {endpoint.path_prefix!r}")
    if (
        isinstance(endpoint.replica_routes, bool)
        or not isinstance(endpoint.replica_routes, int)
        or endpoint.replica_routes < 0
        or endpoint.replica_routes > 1_000_000
    ):
        raise ValueError(f"invalid HAProxy replica-route count {endpoint.replica_routes!r}")


def _render_backend(
    name: str,
    endpoints: list[BackendEndpoint],
    path_prefix: str,
    balance: str,
    check_interval: int,
    check_fall: int,
    check_rise: int,
) -> str:
    """Render a single HAProxy backend section.

    Bound replica routes (endpoints[].replica_routes > 0): the model is served as N
    node-pinned single-replica deployments at routes <path_prefix>_r{0..N-1}.
    Every node's Ray Serve proxy can route any replica route, so we keep the
    node servers for TCP spread and pick a replica per request by REWRITING the
    path to /<path_prefix>_r{rand}<orig-path>. Random selection (rand(N)) is
    statistically even and avoids a shared round-robin counter under concurrency.
    """
    replica_n = endpoints[0].replica_routes if endpoints else 0
    if any(endpoint.replica_routes != replica_n for endpoint in endpoints):
        raise ValueError(f"inconsistent replica-route counts for backend {name!r}")
    lines = [
        f"backend {name}",
        f"    balance {balance}",
    ]
    if replica_n > 0:
        # Every canonical replica is a node-pinned, single-replica application
        # at <path_prefix>_r{0..N-1}. Pick a replica per request (rand, even and
        # lock-free) and rewrite the path to its route; any node's Serve proxy then
        # routes it to that replica.
        lines += [
            f"    http-request set-var(txn.ridx) rand({replica_n})",
        ]
        if path_prefix:
            escaped = re.escape(path_prefix)
            lines.append(
                f"    http-request replace-path ^{escaped}(/.*)?$ "
                f"{path_prefix}_r%[var(txn.ridx)]\\1"
            )
            lines.append(
                f"    http-request set-path {path_prefix}_r%[var(txn.ridx)]%[path] "
                f"unless {{ path_beg {path_prefix}_r }}"
            )
    # Health-check the Ray Serve proxy's OWN liveness (/-/healthz) in ALL cases,
    # never a model route. /-/healthz is answered locally by each node's proxy
    # (router-ready / not-draining), independent of any replica's load, so a slow
    # or busy replica cannot fail the check. Layering: HAProxy owns "is this node's
    # proxy up?"; Ray Serve owns replica health (its own check_health RPC) and
    # routes around dead replicas via EveryNode. A per-replica health path instead
    # funnels EVERY server's check through one replica and flaps the whole backend
    # under load -- measured at 405B/256n with /<route>_r0/health: ~508 Layer7
    # timeouts, ~800 DOWN events, 20-37% client errors, while HAProxy itself sat at
    # 7-33% CPU. /-/healthz -> 0 Layer7 timeouts, 0 errors, throughput == direct.
    lines += [
        "    option httpchk GET /-/healthz",
        "    http-check expect status 200",
    ]
    for ep in endpoints:
        server_name = f"{_safe_backend_name(ep.host)}_{ep.port}"
        lines.append(
            f"    server {server_name} {ep.host}:{ep.port} "
            f"check inter {check_interval}ms fall {check_fall} rise {check_rise}"
        )
    lines.append("")  # blank line between sections
    return "\n".join(lines)


def _shared_path_prefix(endpoints: list[BackendEndpoint]) -> str:
    prefixes = {ep.path_prefix for ep in endpoints}
    if len(prefixes) != 1:
        raise ValueError(f"Inconsistent path_prefix values in backend set: {sorted(prefixes)!r}")
    return prefixes.pop()
