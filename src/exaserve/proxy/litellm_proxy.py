"""
LiteLLM Proxy backend implementation.

Generates a LiteLLM config YAML listing every Ray Serve node as an
OpenAI-compatible backend.  The composition root owns process launch.

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

from pathlib import Path

from .base import (
    BackendEndpoint,
    ProxyBackend,
    reject_unknown_options,
    strict_int,
    strict_text,
    validate_endpoint,
)


class LiteLLMProxy(ProxyBackend):
    """Render the LiteLLM artifact consumed by the composition root."""

    # Default routing and reliability settings
    _DEFAULT_ROUTING_STRATEGY = "least-busy"
    _DEFAULT_NUM_RETRIES = 2
    _DEFAULT_TIMEOUT = 300  # seconds; matches Ray Serve's 5-minute deadline

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
        reject_unknown_options(
            options,
            {
                "extra_general",
                "extra_router",
                "num_retries",
                "routing_strategy",
                "timeout",
            },
            "litellm",
        )

        routing_strategy = strict_text(
            options.get("routing_strategy", self._DEFAULT_ROUTING_STRATEGY),
            "proxy.options.routing_strategy",
            choices={"least-busy", "simple-shuffle", "latency-based-routing"},
        )
        num_retries = strict_int(
            options.get("num_retries", self._DEFAULT_NUM_RETRIES),
            "proxy.options.num_retries",
            minimum=0,
            maximum=100,
        )
        timeout = strict_int(
            options.get("timeout", self._DEFAULT_TIMEOUT),
            "proxy.options.timeout",
            minimum=1,
            maximum=86400,
        )

        # Build the model_list -- one entry per (node, model) pair.
        # LiteLLM groups entries with the same model_name and load-balances
        # across them, which is exactly the semantics we want.
        model_list = []
        for ep in backends:
            validate_endpoint(ep, "litellm")
            api_base = f"http://{ep.host}:{ep.port}{ep.path_prefix}/v1"
            model_list.append(
                {
                    "model_name": ep.model_id,
                    "litellm_params": {
                        # "openai/" prefix tells LiteLLM the backend speaks the
                        # OpenAI API protocol (which Ray Serve does).
                        "model": f"openai/{ep.model_id}",
                        "api_base": api_base,
                        # Ray Serve doesn't require auth; LiteLLM needs a non-empty value.
                        "api_key": "dummy",
                    },
                }
            )
        if not model_list:
            raise ValueError("LiteLLM requires at least one backend endpoint")

        extra_router = options.get("extra_router", {})
        extra_general = options.get("extra_general", {})
        if not isinstance(extra_router, dict) or not isinstance(extra_general, dict):
            raise ValueError("LiteLLM extra_router/extra_general must be mappings")
        sensitive = {"master_key", "database_url", "database_connection_pool_limit"}
        if sensitive & set(extra_general):
            raise ValueError("LiteLLM secrets/database settings must use GatewayPlan secret_ref")

        router_settings: dict = {
            "routing_strategy": routing_strategy,
            "num_retries": num_retries,
            "timeout": timeout,
        }
        router_settings.update(extra_router)

        general_settings: dict = {}
        general_settings.update(extra_general)

        config = {
            "model_list": model_list,
            "router_settings": router_settings,
            "general_settings": general_settings,
        }

        config_path = output_dir / "litellm_config.yaml"
        from exaserve.state.atomic import atomic_create_or_verify_yaml

        atomic_create_or_verify_yaml(config_path, config, default_flow_style=False)

        print(
            f"[LiteLLMProxy] Config written to {config_path} "
            f"({len(model_list)} backend entries, strategy={routing_strategy})"
        )
        return config_path
