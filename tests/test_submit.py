from exaserve.submit import submit_serve


def test_submit_serve_dry_run_renders_self_contained_pbs(tmp_path):
    config_path = tmp_path / "deploy.yaml"
    config_path.write_text(
        """
num_nodes: 1
local_stage_path: /tmp/aurora-models
model_configs:
  - model_id: test/model
    tensor_parallel_size: 1
    max_model_len: 128
    size: 1
""".lstrip(),
        encoding="utf-8",
    )

    # PR-027 context: the DEFAULT backend is PSI/J; the PBS renderer is the
    # explicit native fallback. Test both contracts (the old version of this
    # test assumed PBS was still the default).
    import os as _os

    log_dir = tmp_path / "pbs_logs"
    old_env = _os.environ.get("EXASERVE_SCHEDULER")
    try:
        _os.environ["EXASERVE_SCHEDULER"] = "pbs"
        job_id = submit_serve(config_path, log_dir=log_dir, dry_run=True)
    finally:
        if old_env is None:
            _os.environ.pop("EXASERVE_SCHEDULER", None)
        else:
            _os.environ["EXASERVE_SCHEDULER"] = old_env

    assert job_id == ""
    pbs_text = (log_dir / "deploy.pbs").read_text(encoding="utf-8")
    assert 'source "$HOME/script/env_aurora"' in pbs_text
    assert "EXASERVE_PACKAGE_ROOT=" in pbs_text
    assert "EXASERVE_PACKAGE_PARENT=" in pbs_text
    assert "exec bash " in pbs_text
    assert "exaserve-launch-cluster" not in pbs_text


def test_submit_serve_dry_run_default_backend_renders_psij(tmp_path, monkeypatch):
    monkeypatch.delenv("EXASERVE_SCHEDULER", raising=False)
    config_path = tmp_path / "deploy.yaml"
    config_path.write_text(
        """
num_nodes: 1
local_stage_path: /tmp/aurora-models
model_configs:
  - model_id: test/model
    tensor_parallel_size: 1
    max_model_len: 128
    size: 1
""".lstrip(),
        encoding="utf-8",
    )
    log_dir = tmp_path / "pbs_logs"
    job_id = submit_serve(config_path, log_dir=log_dir, dry_run=True)
    assert job_id == ""
    assert (log_dir / "deploy.psij.sh").exists()  # PSI/J is the default
