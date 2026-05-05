from aurora_rayserver.submit import submit_serve


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

    log_dir = tmp_path / "pbs_logs"
    job_id = submit_serve(config_path, log_dir=log_dir, dry_run=True)

    assert job_id == ""
    pbs_text = (log_dir / "deploy.pbs").read_text(encoding="utf-8")
    assert 'source "$HOME/script/env_aurora"' in pbs_text
    assert "AURORA_RAYSERVER_PACKAGE_ROOT=" in pbs_text
    assert "AURORA_RAYSERVER_PACKAGE_PARENT=" in pbs_text
    assert "exec bash " in pbs_text
    assert "aurora-launch-cluster" not in pbs_text
