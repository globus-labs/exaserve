"""ExaServe distributed control plane (plan §3.1–§3.3).

Modules:
- ``contracts``: lifecycle enums, observations, protocol envelopes, errors,
  serialization, schema validation. No Ray imports, no I/O.
- ``transport``: authenticated length-prefixed framing and session handling.
  No readiness policy.

Later work packages add ``supervisor``, ``rank_launcher``,
``node_supervisor``, and ``readiness`` per the normative code-ownership table.
"""
