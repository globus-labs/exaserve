"""WP13: the old marker-parsing driver is a one-way alias only."""

from __future__ import annotations

import inspect


def test_driver_contains_no_lifecycle_or_readiness_marker_parser():
    from exaserve import driver

    source = inspect.getsource(driver)
    for forbidden in (
        "CLUSTER FULLY READY",
        "Popen",
        "ready_marker",
        "ray start",
        "start_proxy",
        "wait_for_process",
    ):
        assert forbidden not in source
    assert "launcher_main(argv)" in source


def test_rank_identity_is_fail_closed(monkeypatch):
    from exaserve.control.ray_runtime import get_rank

    for name in (
        "PALS_RANKID",
        "PMI_RANK",
        "PMI_ID",
        "ALPS_APP_PE",
        "SLURM_PROCID",
        "OMPI_COMM_WORLD_RANK",
        "EXASERVE_TEST_LOCAL_RANK",
    ):
        monkeypatch.delenv(name, raising=False)
    import pytest

    with pytest.raises(RuntimeError, match="refusing to guess rank zero"):
        get_rank()
    monkeypatch.setenv("PALS_RANKID", "3")
    assert get_rank() == 3
