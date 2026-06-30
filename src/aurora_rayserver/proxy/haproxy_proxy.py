"""
HAProxy backend implementation.

Generates an haproxy.cfg that balances across Ray Serve HTTP proxies, then
launches the haproxy binary.

HAProxy is a pure load balancer -- it has no OpenAI awareness.
Use it as a performance baseline or when you only need L7 TCP/HTTP routing
without API key management, usage tracking, or model-aware routing.

If you need those features, use LiteLLMProxy instead.

haproxy must be installed and on PATH.
"""

import os
import signal
import socket
import subprocess
import time
from pathlib import Path
from textwrap import dedent

from .base import BackendEndpoint, ProxyBackend


class HAProxyProxy(ProxyBackend):
    """Manages an HAProxy process as the request router."""

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
            maxconn (int):       Max concurrent connections. Default: 50000.

        Note: HAProxy uses one backend *per unique model_id*. Nodes serving the
        same model are grouped together. If all backends serve the same model (the
        common case), there is one backend section named after that model.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        balance = options.get("balance", "leastconn")
        check_interval = int(options.get("check_interval", 5000))
        check_fall = int(options.get("check_fall", 3))
        check_rise = int(options.get("check_rise", 2))
        stats_port = int(options.get("stats_port", 9999))
        maxconn = int(options.get("maxconn", 50000))
        http_no_delay = bool(options.get("http_no_delay", True))
        # HAProxy parallelism = threads (modern HAProxy is threaded, not multi-proc).
        # 0/unset -> omit nbthread (HAProxy auto-detects = bound CPUs). Set
        # options.nbthread (alias: num_workers) to pin more accept/processing threads
        # — relevant to the 256n connection-bound streaming case.
        nbthread = int(options.get("nbthread", options.get("num_workers", 0)) or 0)

        # Group endpoints by model_id so each model gets its own backend section
        from collections import defaultdict
        by_model: dict[str, list[BackendEndpoint]] = defaultdict(list)
        for ep in backends:
            by_model[ep.model_id].append(ep)

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
        nbthread_line = f"\n                nbthread {nbthread}" if nbthread > 0 else ""
        lines.append(dedent(f"""\
            global
                maxconn {maxconn}{nbthread_line}
                log stdout format raw local0 info

            defaults
                mode http{no_delay}
                timeout connect 5s
                timeout client  330s
                timeout server  330s
                option http-server-close
                option forwardfor
                log global
            """))

        # --- frontend ---
        # A single frontend receives all incoming OpenAI API requests.
        # For multi-model deployments, requests must already target the
        # per-model Ray Serve route prefix because HAProxy does not inspect
        # the OpenAI JSON body to recover the model name.
        if len(by_model) == 1:
            model_id = next(iter(by_model))
            safe_name = _safe_backend_name(model_id)
            lines.append(dedent(f"""\
                frontend openai_api
                    bind *:{{PORT}}
                    default_backend {safe_name}
                """))
        else:
            lines.append("frontend openai_api")
            lines.append("    bind *:{PORT}")
            for model_id, eps in by_model.items():
                safe_name = _safe_backend_name(model_id)
                path_prefix = _shared_path_prefix(eps)
                acl_name = f"is_{safe_name}"
                lines.append(f"    acl {acl_name} path_beg {path_prefix} {path_prefix}/")
                lines.append(f"    use_backend {safe_name} if {acl_name}")
            lines.append(
                '    http-request return status 404 content-type text/plain '
                'lf-string "missing or unknown model route prefix\\n"'
            )
            lines.append("")

        # --- backend section(s) ---
        if len(by_model) == 1:
            model_id, eps = next(iter(by_model.items()))
            safe_name = _safe_backend_name(model_id)
            path_prefix = _shared_path_prefix(eps)
            lines.append(_render_backend(
                name=safe_name,
                endpoints=eps,
                path_prefix=path_prefix,
                balance=balance,
                check_interval=check_interval,
                check_fall=check_fall,
                check_rise=check_rise,
            ))
        else:
            for model_id, eps in by_model.items():
                safe_name = _safe_backend_name(model_id)
                path_prefix = _shared_path_prefix(eps)
                lines.append(_render_backend(
                    name=safe_name,
                    endpoints=eps,
                    path_prefix=path_prefix,
                    balance=balance,
                    check_interval=check_interval,
                    check_fall=check_fall,
                    check_rise=check_rise,
                ))

        # --- optional stats page ---
        if stats_port > 0:
            lines.append(dedent(f"""\
                listen stats
                    bind *:{stats_port}
                    stats enable
                    stats uri /stats
                    stats refresh 10s
                    stats admin if TRUE
                """))

        # Resolve {PORT} placeholder (frontend used it above)
        config_text = "\n".join(lines)

        config_path = output_dir / "haproxy.cfg"
        with open(config_path, "w") as f:
            f.write(config_text)

        total_servers = sum(len(eps) for eps in by_model.values())
        print(
            f"[HAProxyProxy] Config written to {config_path} "
            f"({len(by_model)} model(s), {total_servers} server entries, balance={balance})"
        )
        return config_path

    def start(self, config_path: Path, host: str, port: int, **kwargs) -> tuple[subprocess.Popen, int]:
        """
        Launch haproxy with the generated config.

        The {PORT} placeholder in the config is resolved here by re-writing
        the config with the actual port before launching.

        Returns (proc, port) to satisfy the ProxyBackend interface.
        """
        # Patch the port placeholder in the config file
        text = config_path.read_text()
        text = text.replace("{PORT}", str(port))
        config_path.write_text(text)

        cmd = ["haproxy", "-f", str(config_path)]
        print(f"[HAProxyProxy] Starting: {' '.join(cmd)}", flush=True)
        proc = subprocess.Popen(cmd)
        print(f"[HAProxyProxy] Process started (pid={proc.pid}, port={port})", flush=True)
        self._start_diag_sampler(proc.pid, config_path.parent)
        return proc, port

    def _start_diag_sampler(self, haproxy_pid: int, out_dir: Path) -> None:
        """Spawn a lightweight head-node sampler (opt out: AURORA_HAPROXY_DIAG=0).

        Every 3s while HAProxy is alive, append the TCP/socket + process counters
        that disambiguate why new connections get ECONNREFUSED under load:
          - TcpExtListenOverflows / ListenDrops / TCPReqQFullDoCookies: accept-queue
            overflow (HAProxy too CPU-busy to accept()) -> the classic ECONNREFUSED.
          - /proc/net/sockstat 'tw' + TCPTimeWait: TIME_WAIT / ephemeral-port churn
            from option http-server-close closing millions of short connections.
          - haproxy %cpu: is the proxy pegged at one core?
        Written to <proxy_out>/haproxy_diag.log so it is gathered with the run.
        """
        if os.environ.get("AURORA_HAPROXY_DIAG", "1") == "0":
            return
        diag_path = out_dir / "haproxy_diag.log"
        # 5s interval (vs 3s) keeps the sampler's own fork footprint small on a
        # resource-stressed head node — Aurora kills/refuses procs with EAGAIN
        # ("resource temporarily unavailable") under nproc/thread/fd/mem pressure,
        # and we don't want the sampler to be a contributor or a victim.
        script = (
            'echo "[diag] sampling haproxy pid={pid} every 5s -> $0"; '
            # one-time: the LIMITS that EAGAIN-kills hit, + HAProxy soft limits.
            'echo "[limits] threads-max=$(cat /proc/sys/kernel/threads-max 2>/dev/null)'
            ' pid_max=$(cat /proc/sys/kernel/pid_max 2>/dev/null)'
            ' file-max=$(cat /proc/sys/fs/file-max 2>/dev/null)"; '
            'grep -iE "Max processes|Max open files" /proc/{pid}/limits 2>/dev/null'
            '  | sed "s/^/[limits] haproxy /"; '
            'while kill -0 {pid} 2>/dev/null; do '
            '  echo "=== ts=$(date +%s) ==="; '
            '  ps -o pid=,%cpu=,%mem=,rss=,nlwp= -p {pid} 2>/dev/null '
            '    | sed "s/^/haproxy_proc: /"; '
            '  grep -E "TCP:|sockets:" /proc/net/sockstat 2>/dev/null; '
            '  nstat -as 2>/dev/null | grep -iE '
            '"ListenOverflow|ListenDrop|ReqQFull|BacklogDrop|Syncookie|TimeWaitOverflow|RetransSegs|TCPAbort"; '
            '  ss -s 2>/dev/null | head -2; '
            # NIC-level drops/errors via sysfs (no privileges, unlike ethtool -S on HSN).
            '  for IF in $(ls /sys/class/net | grep -E "hsn"); do echo -n "nic $IF: "; '
            '    for k in rx_dropped tx_dropped rx_errors rx_missed_errors rx_fifo_errors rx_over_errors; do '
            '      echo -n "$k=$(cat /sys/class/net/$IF/statistics/$k 2>/dev/null) "; done; echo; done; '
            # RESOURCE-EXHAUSTION evidence (the EAGAIN-kill hypothesis): node memory,
            # system-wide threads/procs vs limit, open-fd vs limit, HAProxy fd count.
            '  echo "res: $(awk \'/^MemAvailable|^MemFree/{print $1$2}\' /proc/meminfo 2>/dev/null | tr \'\\n\' \' \')'
            'loadavg=$(cut -d\' \' -f1-3,4 /proc/loadavg 2>/dev/null) '
            'sys_threads=$(cat /proc/sys/kernel/threads-max 2>/dev/null)/used=$(ls /proc 2>/dev/null | grep -c \'^[0-9]\') '
            'file_nr=$(cat /proc/sys/fs/file-nr 2>/dev/null) '
            'ha_fds=$(ls /proc/{pid}/fd 2>/dev/null | wc -l)"; '
            # CANARY: try a trivial fork; if it returns EAGAIN, log it — direct proof
            # the node is refusing new procs ("resource temporarily unavailable").
            '  ( /bin/true ) 2>/tmp/.diag_fork_$$ || echo "FORK-CANARY-FAILED: $(cat /tmp/.diag_fork_$$ 2>/dev/null)"; '
            '  sleep 5; '
            'done; '
            'echo "[diag] haproxy pid {pid} gone at ts=$(date +%s)"; '
            # smoking gun on death: OOM-killer / kill evidence from the kernel ring buffer.
            'echo "[diag] dmesg tail (OOM/kill evidence, may be empty w/o priv):"; '
            'dmesg -T 2>/dev/null | tail -25 | grep -iE "oom|kill|haproxy|memory|fork|cannot" | sed "s/^/[dmesg] /" || true'
        ).replace("{pid}", str(haproxy_pid))  # NOT .format(): the script has literal awk {…} braces
        try:
            with open(diag_path, "a") as fh:
                subprocess.Popen(
                    ["bash", "-c", script, str(diag_path)],
                    stdout=fh, stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            print(f"[HAProxyProxy] Diag sampler -> {diag_path}", flush=True)
        except Exception as exc:  # diagnostics must never break the run
            print(f"[HAProxyProxy] Diag sampler failed to start: {exc}", flush=True)

    def health_check(
        self,
        host: str,
        port: int,
        timeout: float = 30.0,
        process: subprocess.Popen | None = None,
    ) -> bool:
        """
        Poll TCP connect to host:port until it accepts connections or timeout.

        Returns False immediately if the proxy process has already exited.
        """
        check_host = "127.0.0.1" if host in ("0.0.0.0", "") else host
        deadline = time.monotonic() + timeout
        attempt = 0
        while time.monotonic() < deadline:
            if process is not None and process.poll() is not None:
                print(
                    f"[HAProxyProxy] Process {self._describe_exit(process.returncode)} "
                    f"before becoming healthy.",
                    flush=True,
                )
                return False

            attempt += 1
            try:
                with socket.create_connection((check_host, port), timeout=2):
                    print(
                        f"[HAProxyProxy] Healthy after {attempt} attempt(s) "
                        f"(port {port})",
                        flush=True,
                    )
                    return True
            except OSError:
                pass
            time.sleep(1)

        print(
            f"[HAProxyProxy] Health check timed out after {timeout}s",
            flush=True,
        )
        return False

    @staticmethod
    def _describe_exit(rc) -> str:
        """Human-readable cause from a Popen returncode. A negative code means the
        OS killed the proxy with that signal -- the smoking gun for the 256n death:
        SIGKILL=OOM-killer/external resource kill ('resource temporarily unavailable'),
        SIGSEGV=crash. A non-negative code means HAProxy exited on its own."""
        if rc is None:
            return "still running"
        if rc < 0:
            try:
                name = signal.Signals(-rc).name
            except (ValueError, AttributeError):
                name = f"signal {-rc}"
            hint = {
                signal.SIGKILL: " (OOM-killer or external/resource kill, e.g. EAGAIN 'resource temporarily unavailable')",
                signal.SIGSEGV: " (segfault/crash)",
                signal.SIGABRT: " (abort -- fatal internal error)",
                signal.SIGBUS: " (bus error)",
            }.get(-rc, "")
            return f"KILLED BY {name}{hint} (returncode={rc})"
        return f"exited with code {rc}" + (" (clean)" if rc == 0 else " (self-terminated/error)")

    def stop(self, process: subprocess.Popen) -> None:
        """Send SIGTERM for graceful drain, then SIGKILL after 15s."""
        rc = process.poll()
        if rc is not None:
            # The proxy already exited BEFORE teardown -> it died DURING the run.
            # Log HOW (the signal is the smoking gun). This used to return silently,
            # which is exactly why every 256n proxy death had no recorded cause.
            print(
                f"[HAProxyProxy] *** PROXY DIED DURING RUN: pid={process.pid} "
                f"{self._describe_exit(rc)} ***",
                flush=True,
            )
            return
        print(f"[HAProxyProxy] Stopping proxy (pid={process.pid})", flush=True)
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            print("[HAProxyProxy] SIGTERM timed out, sending SIGKILL", flush=True)
            process.kill()
            process.wait()
        print("[HAProxyProxy] Proxy stopped.", flush=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_backend_name(model_id: str) -> str:
    """Convert a model_id to an HAProxy-safe identifier (no slashes or dots)."""
    return model_id.replace("/", "_").replace(".", "_").replace("-", "_")


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

    Shard-aware PP (endpoints[].shard_replicas > 0): the model is served as N
    node-pinned single-replica deployments at routes <path_prefix>_r{0..N-1}.
    Every node's Ray Serve proxy can route any replica route, so we keep the
    node servers for TCP spread and pick a replica per request by REWRITING the
    path to /<path_prefix>_r{rand}<orig-path>. Random selection (rand(N)) is
    statistically even and avoids a shared round-robin counter under concurrency.
    """
    shard_n = endpoints[0].shard_replicas if endpoints else 0
    if shard_n > 0:
        health_path = f"{path_prefix}_r0/health"
        lines = [
            f"backend {name}",
            f"    balance {balance}",
            # pick a replica index 0..N-1 and prepend its route to the path
            f"    http-request set-var(txn.ridx) rand({shard_n})",
            f"    http-request set-path {path_prefix}_r%[var(txn.ridx)]%[path]",
            f"    option httpchk GET {health_path}",
            "    http-check expect status 200",
        ]
    else:
        health_path = f"{path_prefix}/health" if path_prefix else "/health"
        lines = [
            f"backend {name}",
            f"    balance {balance}",
            f"    option httpchk GET {health_path}",
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
