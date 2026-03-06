"""
LiteLLM Proxy backend implementation.

Generates a LiteLLM config YAML listing every Ray Serve node as an
OpenAI-compatible backend, then launches `litellm --config <path>`.

Features provided by LiteLLM (all managed at the proxy layer, zero changes
to Ray serving code):
  - API key management and authentication
  - Per-user/per-key rate limiting (TPM, RPM, parallel requests)
  - Model-aware routing (different node pools per model)
  - Token usage and cost tracking
  - Request logging and callbacks
  - Automatic health checks and failover
  - Retry on backend errors
"""

import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import yaml

from proxy.base import BackendEndpoint, ProxyBackend


class LiteLLMProxy(ProxyBackend):
    """Manages a LiteLLM proxy process (launched as a subprocess)."""

    # Default routing and reliability settings
    _DEFAULT_ROUTING_STRATEGY = "least-busy"
    _DEFAULT_NUM_RETRIES = 2
    _DEFAULT_TIMEOUT = 300          # seconds; matches Ray Serve's 5-minute deadline

    def generate_config(
        self,
        backends: list[BackendEndpoint],
        output_dir: Path,
        **options,
    ) -> Path:
        """
        Write a litellm_config.yaml to output_dir.

        Options (all optional):
            python_path (str):         Path to the Python interpreter to use for
                                       launching litellm.  Use this when litellm is
                                       installed in a separate venv from Ray/vLLM.
                                       Default: sys.executable (current interpreter).
            master_key (str):          Bearer token required from callers.
                                       Default: "sk-aurora" (set a real secret in prod).
            routing_strategy (str):    LiteLLM router strategy.
                                       "least-busy" | "simple-shuffle" | "latency-based-routing"
                                       Default: "least-busy".
            num_retries (int):         Retries on backend failure. Default: 2.
            timeout (int):             Per-request timeout in seconds. Default: 300.
            db_url (str):              SQLAlchemy URL for usage DB.
                                       Default: "sqlite:///litellm_usage.db" (local file).
            extra_general (dict):      Merged verbatim into general_settings.
            extra_router (dict):       Merged verbatim into router_settings.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Stash python_path for use in start()
        self._python_path = options.get("python_path", sys.executable)

        master_key = options.get("master_key", "")
        routing_strategy = options.get("routing_strategy", self._DEFAULT_ROUTING_STRATEGY)
        num_retries = int(options.get("num_retries", self._DEFAULT_NUM_RETRIES))
        timeout = int(options.get("timeout", self._DEFAULT_TIMEOUT))
        db_url = options.get("db_url", "")  # empty = no DB (no Prisma needed)

        # Build the model_list -- one entry per (node, model) pair.
        # LiteLLM groups entries with the same model_name and load-balances
        # across them, which is exactly the semantics we want.
        model_list = []
        for ep in backends:
            model_list.append({
                "model_name": ep.model_id,
                "litellm_params": {
                    # "openai/" prefix tells LiteLLM the backend speaks the
                    # OpenAI API protocol (which Ray Serve does).
                    "model": f"openai/{ep.model_id}",
                    "api_base": f"http://{ep.host}:{ep.port}/v1",
                    # Ray Serve doesn't require auth; LiteLLM needs a non-empty value.
                    "api_key": "dummy",
                },
            })

        router_settings: dict = {
            "routing_strategy": routing_strategy,
            "num_retries": num_retries,
            "timeout": timeout,
        }
        router_settings.update(options.get("extra_router", {}))

        general_settings: dict = {}
        if master_key:
            general_settings["master_key"] = master_key
        if db_url:
            general_settings["database_url"] = db_url
        general_settings.update(options.get("extra_general", {}))

        config = {
            "model_list": model_list,
            "router_settings": router_settings,
            "general_settings": general_settings,
        }

        config_path = output_dir / "litellm_config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

        print(
            f"[LiteLLMProxy] Config written to {config_path} "
            f"({len(model_list)} backend entries, strategy={routing_strategy})"
        )
        return config_path

    def start(self, config_path: Path, host: str, port: int, num_workers: int = 8) -> subprocess.Popen:
        """
        Launch the LiteLLM proxy via its ``litellm`` console script.

        Derives the script path from self._python_path so it uses the
        correct venv (e.g. /path/to/venv/bin/python3 → /path/to/venv/bin/litellm).

        Port and host are set via environment variables because some litellm
        versions ignore the CLI --port/--host flags.
        """
        python = getattr(self, "_python_path", sys.executable)
        litellm_bin = str(Path(python).parent / "litellm")
        cmd = [
            litellm_bin,
            "--config", str(config_path),
            "--port", str(port),
            "--host", host,
            "--num_workers", str(num_workers),
        ]

        env = os.environ.copy()
        env["LITELLM_PORT"] = str(port)
        env["LITELLM_HOST"] = host
        env["UVICORN_PORT"] = str(port)
        env["UVICORN_HOST"] = host
        env["PORT"] = str(port)
        env["HOST"] = host

        # Disable HTTP proxy for backend requests.  The litellm process
        # talks to Ray Serve nodes on the HSN which must not go through
        # ALCF's Squid proxy.
        env.pop("HTTP_PROXY", None)
        env.pop("HTTPS_PROXY", None)
        env.pop("http_proxy", None)
        env.pop("https_proxy", None)
        env["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"

        # Strip Intel oneAPI / Level Zero / SYCL env vars.  These cause
        # uvicorn worker children to segfault after fork because Level Zero
        # runtime state is not fork-safe.  LiteLLM is a pure-Python HTTP
        # proxy and never needs GPU access.
        _GPU_ENV_PREFIXES = (
            "ZE_", "ONEAPI_", "SYCL_", "CCL_", "I_MPI_", "FI_",
            "INTEL_", "LIBOMPTARGET_", "NEOReadDebugKeys",
        )
        _GPU_ENV_EXACT = {
            "RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR",
            "ZE_FLAT_DEVICE_HIERARCHY",
            "ZE_AFFINITY_MASK",
        }
        for key in list(env):
            if key in _GPU_ENV_EXACT or key.startswith(_GPU_ENV_PREFIXES):
                del env[key]

        # Also scrub LD_LIBRARY_PATH of Intel/oneAPI shared-lib dirs so the
        # forked workers don't load Level Zero or SYCL runtimes at all.
        ld_path = env.get("LD_LIBRARY_PATH", "")
        if ld_path:
            clean = [p for p in ld_path.split(":") if "/oneapi/" not in p and "/intel/" not in p.lower()]
            env["LD_LIBRARY_PATH"] = ":".join(clean)

        print(f"[LiteLLMProxy] Starting: {' '.join(cmd)}", flush=True)
        print(f"[LiteLLMProxy] Port forced via env: LITELLM_PORT={port}, UVICORN_PORT={port}", flush=True)
        proc = subprocess.Popen(cmd, env=env)
        print(f"[LiteLLMProxy] Process started (pid={proc.pid})", flush=True)
        return proc

    def health_check(
        self,
        host: str,
        port: int,
        timeout: float = 30.0,
        process: subprocess.Popen | None = None,
    ) -> bool:
        """
        Poll GET http://{host}:{port}/health until 200 or timeout.

        Returns False immediately if the proxy process has already exited.
        """
        url = f"http://{host}:{port}/health/liveliness"
        deadline = time.monotonic() + timeout
        attempt = 0
        while time.monotonic() < deadline:
            # Fast-fail if the process crashed
            if process is not None and process.poll() is not None:
                print(
                    f"[LiteLLMProxy] Process exited with code {process.returncode} "
                    f"before becoming healthy.",
                    flush=True,
                )
                return False

            attempt += 1
            try:
                with urllib.request.urlopen(url, timeout=3) as resp:
                    if resp.status == 200:
                        print(
                            f"[LiteLLMProxy] Healthy after {attempt} attempt(s) "
                            f"(port {port})",
                            flush=True,
                        )
                        return True
            except (urllib.error.URLError, OSError):
                pass
            time.sleep(2)

        print(
            f"[LiteLLMProxy] Health check timed out after {timeout}s",
            flush=True,
        )
        return False

    def stop(self, process: subprocess.Popen) -> None:
        """Send SIGTERM, wait up to 10 s, then SIGKILL."""
        if process.poll() is not None:
            return
        print(f"[LiteLLMProxy] Stopping proxy (pid={process.pid})", flush=True)
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print("[LiteLLMProxy] SIGTERM timed out, sending SIGKILL", flush=True)
            process.kill()
            process.wait()
        print("[LiteLLMProxy] Proxy stopped.", flush=True)
