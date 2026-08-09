"""The saturation-result transport is exact and finite."""

from __future__ import annotations

import pytest

from eval.lib.saturation import validate_saturation_output, validate_step_result


def _step() -> dict:
    return {
        "target_rate": 10,
        "achieved_rate": 9.0,
        "duration_s": 1.0,
        "completed": 9,
        "failed": 1,
        "error_rate": 0.1,
        "p50_latency_s": 0.1,
        "p99_latency_s": 0.2,
        "mean_latency_s": 0.12,
        "new_connections": 1,
        "reused_connections": 9,
        "max_observed_active": 2,
        "healthy": False,
        "fail_reasons": ["error SLO"],
    }


def test_saturation_output_contract_round_trips():
    payload = {
        "mode": "binary",
        "saturation_rate": 9,
        "tolerance": 0.05,
        "slo": {"max_error_rate": 0.01, "plateau_ratio": 0.95},
        "steps": [_step()],
    }
    assert validate_saturation_output(payload) is payload


def test_saturation_step_rejects_coercible_counts_and_inconsistent_rate():
    step = _step()
    step["completed"] = "9"
    with pytest.raises(ValueError, match="completed"):
        validate_step_result(step)

    step = _step()
    step["error_rate"] = 0.2
    with pytest.raises(ValueError, match="disagrees"):
        validate_step_result(step)
