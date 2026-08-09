# Engine and gateway interfaces

Status: implemented. Production qualification is narrower than the registry.

## Engine boundary

`EngineWorker` owns the OpenAI-compatible HTTP surface, Ray Serve lifecycle,
placement, health, and per-replica reporting. An `EngineBackend` owns only the
engine-specific implementation:

- `create(EngineSpec)`
- `build_chat_prompt(...)`
- `generate(...)` and `generate_stream(...)`
- `warmup()`, statistics, and bounded shutdown

`EngineSpec` includes the plan-derived vendor identity. vLLM and SGLang import
their heavyweight dependencies only inside `create()`, preventing their
dependency pins from colliding at module import. Local backend-module import
failures retain their original cause; they are not silently reported as an
unknown engine.

The verified `DeploymentPlan.engine` selects the backend. Null compute is a
typed `DeploymentPlan.runtime` policy and never an ambient public switch.

Built-in implementations are vLLM, SGLang, and null. The default Aurora
`SiteProfile` qualifies only vLLM; SGLang and null-compute runs are validation
work and must use a profile/envelope that says so.

## Gateway boundary

Gateway backends render configuration from already resolved
`BackendEndpoint`s. They do not discover allocation nodes, compile deployment
topology, launch processes, or decide readiness. The composition root owns:

- executable and option preflight;
- the inherited listener or other explicit port handoff;
- process-group supervision and cleanup;
- health observations and per-model canaries;
- durable gateway configuration evidence.

HAProxy is the first-release production gateway and is restricted to the
trusted allocation boundary. Its file-descriptor requirement is checked against
the live hard `RLIMIT_NOFILE` before launch. LiteLLM, NGINX, Envoy, and Pingora
are validation-only in the default profile.

## Adding an implementation

Adding a registry class is not enough to advertise support. A new engine or
gateway also needs:

1. strict plan schema and capability entries;
2. immutable compatibility identity where applicable;
3. lifecycle, cleanup, receipt, and failure-injection tests;
4. a named scale envelope and fresh qualification evidence;
5. an explicit support-matrix/documentation update.
