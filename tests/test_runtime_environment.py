"""Node-local runtime capsule and closed child-environment contract."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from exaserve.plan.runtime_environment import (
    COMPAT_SOURCE_MANIFEST_ENV,
    COMPAT_SOURCE_PROFILE_ENV,
    QUALIFIED_PYTHON_ENV,
    QUALIFIED_PYTHON_HASH_ENV,
    QUALIFIED_PYTHON_PROFILE_ENV,
    RuntimePathError,
    RuntimePaths,
    assert_worker_launch_is_local,
    closed_runtime_environment,
    filesystem_identity,
    parse_filesystem_expectation,
    require_contained_local_path,
    staged_pythonpath,
)


def _plan():
    return SimpleNamespace(
        deployment_id="capsule-test",
        site_profile_id="alcf-aurora",
        site_profile_hash="c" * 64,
        compatibility_profile_hash="a" * 64,
        manifest_hash="b" * 64,
        vendor="xpu",
        engine="vllm",
        model_storage_path="/lus/flare/models",
        models=(SimpleNamespace(pipeline_parallel_size=1, tensor_parallel_size=1),),
        collect_stats=False,
        runtime=SimpleNamespace(
            null_compute=False,
            null_compute_latency_s=0.0,
            pp_shard_aware=False,
            clean_stage=False,
            instrumentation=False,
        ),
        readiness=SimpleNamespace(
            serve_proxy_health_check_timeout_s=10.0,
            serve_proxy_ready_check_timeout_s=11.0,
            serve_start_proxy_timeout_s=12.0,
        ),
    )


def _paths(tmp_path):
    runtime = tmp_path / "runtime"
    for child in ("python", "run", "bin"):
        (runtime / child).mkdir(parents=True, exist_ok=True)
    return RuntimePaths.from_roots(runtime, tmp_path / "state", policy=_plan())


def test_filesystem_expectation_and_longest_mount_identity_are_typed(tmp_path):
    expectation = parse_filesystem_expectation("node_local;fstype=tmpfs;readonly=false")
    assert (expectation.kind, expectation.fstype, expectation.readonly) == (
        "node_local",
        "tmpfs",
        False,
    )
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        f"1 0 0:1 / / rw - overlay overlay rw\n2 1 0:2 / {tmp_path} rw,nosuid - tmpfs tmpfs rw\n",
        encoding="utf-8",
    )
    identity = filesystem_identity(tmp_path, mountinfo_path=str(mountinfo))
    assert identity.mount_point == tmp_path
    assert identity.fstype == "tmpfs"
    assert identity.readonly is False


def test_head_qualifies_every_declared_shared_root(monkeypatch):
    from pathlib import Path

    from exaserve import site as site_module
    from exaserve.plan import runtime_environment

    profile = SimpleNamespace(
        filesystem_semantics=(
            ("local_root:/tmp", "node_local;fstype=tmpfs;readonly=false"),
            ("shared_root:/home", "shared;fstype=lustre;readonly=false"),
            ("shared_root:/lus/flare", "shared;fstype=lustre;readonly=false"),
            (
                "site_root:/opt/aurora",
                "immutable_read_only_site;fstype=squashfs;readonly=true",
            ),
        ),
        model_storage_path="/lus/flare/models",
        local_stage_path="/tmp/models",
    )
    seen = []

    def qualify(path, *, policy, root_kind):
        seen.append((path, policy, root_kind))
        return runtime_environment.FilesystemIdentity(Path(path), "lustre", False, 7)

    monkeypatch.setattr(runtime_environment, "validate_declared_filesystem", qualify)
    evidence = site_module.qualify_declared_shared_filesystems(profile)
    assert [item["root"] for item in evidence] == ["/home", "/lus/flare"]
    assert [item[0] for item in seen] == ["/home", "/lus/flare"]


def test_head_rejects_a_shared_root_with_wrong_runtime_identity(monkeypatch):
    from exaserve import site as site_module
    from exaserve.plan import runtime_environment

    profile = SimpleNamespace(
        filesystem_semantics=(
            ("local_root:/tmp", "node_local;fstype=tmpfs;readonly=false"),
            ("shared_root:/home", "shared;fstype=lustre;readonly=false"),
            (
                "site_root:/opt/aurora",
                "immutable_read_only_site;fstype=squashfs;readonly=true",
            ),
        ),
        model_storage_path="/home/models",
        local_stage_path="/tmp/models",
    )

    def reject(*_args, **_kwargs):
        raise RuntimePathError("filesystem type is tmpfs, expected lustre")

    monkeypatch.setattr(runtime_environment, "validate_declared_filesystem", reject)
    with pytest.raises(RuntimeError, match="shared filesystem qualification failed"):
        site_module.qualify_declared_shared_filesystems(profile)


def test_legacy_filesystem_semantics_remain_readable_but_not_executable(tmp_path):
    from exaserve.site import require_complete_filesystem_policy
    from exaserve.source_staging import SourceStagingError, qualify_runtime_staging_base

    legacy = SimpleNamespace(
        filesystem_semantics=(("shared", "lustre"), ("local_stage", "node_local")),
        model_storage_path="/lus/flare/models",
        local_stage_path="/tmp/models",
    )
    with pytest.raises(RuntimeError, match="missing hash-bearing filesystem roots"):
        require_complete_filesystem_policy(legacy)
    with pytest.raises(SourceStagingError, match="missing hash-bearing filesystem roots"):
        qualify_runtime_staging_base(tmp_path, legacy)
    with pytest.raises(RuntimeError, match="missing hash-bearing filesystem roots"):
        RuntimePaths.from_roots(tmp_path, tmp_path / "state", policy=legacy)


def test_current_aurora_profile_qualifies_its_local_staging_base():
    from exaserve.site import default_site_profile, require_complete_filesystem_policy
    from exaserve.source_staging import qualify_runtime_staging_base

    profile = default_site_profile()
    require_complete_filesystem_policy(profile)
    assert qualify_runtime_staging_base(Path("/tmp"), profile) == Path("/tmp").resolve()


def test_source_staging_rejects_local_base_with_wrong_declared_fstype():
    from exaserve.source_staging import SourceStagingError, qualify_runtime_staging_base

    profile = SimpleNamespace(
        filesystem_semantics=(
            (
                "local_root:/tmp",
                "node_local;fstype=definitely-not-tmpfs;readonly=false",
            ),
            ("shared_root:/home", "shared;fstype=lustre;readonly=false"),
            (
                "site_root:/opt/aurora",
                "immutable_read_only_site;fstype=squashfs;readonly=true",
            ),
        ),
        model_storage_path="/home/models",
        local_stage_path="/tmp/models",
    )
    with pytest.raises(SourceStagingError, match="filesystem type"):
        qualify_runtime_staging_base(Path("/tmp"), profile)


def test_staged_pythonpath_is_closed_and_discards_inherited_shared_paths(tmp_path):
    plan = _plan()
    paths = _paths(tmp_path)
    value = staged_pythonpath(
        plan,
        "/home/user/repo:/lus/flare/project:/unrelated",
        runtime_root=str(paths.root),
    )
    entries = value.split(os.pathsep)
    assert entries == [
        str(paths.python_root / "exaserve" / "_compat_runtime" / ("a" * 64)),
        str(paths.python_root),
    ]


def test_closed_environment_redirects_every_mutable_root(tmp_path):
    plan = _plan()
    paths = _paths(tmp_path)
    paths.prepare_state(policy=plan)
    python = tmp_path / "site-python"
    python.write_bytes(b"python")
    python.chmod(0o755)
    env = closed_runtime_environment(
        plan,
        paths=paths,
        policy=plan,
        base_environment={
            QUALIFIED_PYTHON_ENV: str(python),
            QUALIFIED_PYTHON_HASH_ENV: "d" * 64,
            QUALIFIED_PYTHON_PROFILE_ENV: plan.site_profile_hash,
            COMPAT_SOURCE_PROFILE_ENV: plan.compatibility_profile_hash,
            COMPAT_SOURCE_MANIFEST_ENV: plan.manifest_hash,
            "PATH": "/home/user/bin:/opt/aurora/bin:/usr/bin",
            "PYTHONPATH": "/home/user/repo",
            "HOME": "/home/user",
            "PYTHONUSERBASE": "/home/user/.local",
            "PYTHONSTARTUP": "/home/user/.pythonrc",
            "IPYTHONDIR": "/home/user/.ipython",
            "JUPYTER_CONFIG_DIR": "/home/user/.jupyter",
            "EXASERVE_RUN_PLAN_PATH": "/lus/flare/run.plan.json",
            "ONEAPI_DEVICE_SELECTOR": "level_zero:0",
        },
    )
    assert env["PYTHONNOUSERSITE"] == "1"
    assert env["PYTHONSAFEPATH"] == "1"
    assert env["HOME"] == str(paths.home)
    assert env["TMPDIR"] == str(paths.tmp)
    assert env["XDG_CONFIG_HOME"] == str(paths.state_root / "config")
    assert env["XDG_DATA_HOME"] == str(paths.state_root / "data")
    assert env["NUMBA_CACHE_DIR"] == str(paths.cache / "numba")
    assert env["TORCH_EXTENSIONS_DIR"] == str(paths.cache / "torch_extensions")
    assert env["HF_HUB_OFFLINE"] == "1"
    assert env["TRANSFORMERS_OFFLINE"] == "1"
    assert "PYTHONUSERBASE" not in env
    assert "PYTHONSTARTUP" not in env
    assert env["EXASERVE_RUN_LOG_DIR"] == str(paths.logs)
    assert env["EXASERVE_PLAN_PATH"] == str(paths.plan_path)
    assert env["EXASERVE_LOCAL_PLAN_PATH"] == str(paths.plan_path)
    assert "/home/user/bin" not in env["PATH"]
    assert "EXASERVE_RUN_PLAN_PATH" not in env
    assert "ONEAPI_DEVICE_SELECTOR" not in env
    assert_worker_launch_is_local(
        argv=(str(python), "-m", "exaserve.rank_main", "--plan", str(paths.plan_path)),
        cwd=paths.python_root,
        environment=env,
        policy=plan,
    )


def test_prepare_state_rejects_home_symlink_before_chmod_or_child_creation(tmp_path):
    paths = _paths(tmp_path)
    paths.state_root.mkdir()
    paths.home.symlink_to("/home", target_is_directory=True)

    with pytest.raises(RuntimePathError, match="symlink|shared root"):
        paths.prepare_state(policy=_plan())

    assert paths.home.is_symlink()


def test_closed_environment_rejects_shared_path_hidden_in_unknown_key(tmp_path):
    plan = _plan()
    paths = _paths(tmp_path)
    python = tmp_path / "site-python"
    python.write_bytes(b"python")
    python.chmod(0o755)
    with pytest.raises(RuntimePathError, match="ODD_SETTING"):
        closed_runtime_environment(
            plan,
            paths=paths,
            policy=plan,
            base_environment={
                QUALIFIED_PYTHON_ENV: str(python),
                QUALIFIED_PYTHON_HASH_ENV: "d" * 64,
                QUALIFIED_PYTHON_PROFILE_ENV: plan.site_profile_hash,
                COMPAT_SOURCE_PROFILE_ENV: plan.compatibility_profile_hash,
                COMPAT_SOURCE_MANIFEST_ENV: plan.manifest_hash,
                "ODD_SETTING": "/home/user/hidden-input",
            },
        )


def test_closed_environment_discards_lmod_bookkeeping_before_shared_path_scan(tmp_path):
    plan = _plan()
    paths = _paths(tmp_path)
    python = tmp_path / "site-python"
    python.write_bytes(b"python")
    python.chmod(0o755)
    bookkeeping = {
        "LMOD_CMD": "/home/user/lmod/libexec/lmod",
        "__LMOD_REF_COUNT_PATH": "/opt/aurora/bin:1;/home/user/bin:1",
        "_ModuleTable001_": "encoded-module-table",
        "_ModuleTable_Sz_": "1",
        "MODULEPATH": "/home/user/modulefiles:/opt/aurora/modulefiles",
        "MODULEPATH_ROOT": "/home/user/modulefiles",
        "MODULESHOME": "/home/user/lmod",
        "LOADEDMODULES": "frameworks/2025.3.1",
        "_LMFILES_": "/home/user/modulefiles/frameworks.lua",
        "BASH_FUNC_module%%": "() { source /home/user/module.sh; }",
        "BASH_FUNC_ml%%": "() { source /home/user/ml.sh; }",
    }
    env = closed_runtime_environment(
        plan,
        paths=paths,
        policy=plan,
        base_environment={
            QUALIFIED_PYTHON_ENV: str(python),
            QUALIFIED_PYTHON_HASH_ENV: "d" * 64,
            QUALIFIED_PYTHON_PROFILE_ENV: plan.site_profile_hash,
            COMPAT_SOURCE_PROFILE_ENV: plan.compatibility_profile_hash,
            COMPAT_SOURCE_MANIFEST_ENV: plan.manifest_hash,
            **bookkeeping,
        },
    )

    assert not bookkeeping.keys() & env.keys()


def test_closed_environment_drops_unrecognized_ambient_behavior(tmp_path):
    plan = _plan()
    paths = _paths(tmp_path)
    python = tmp_path / "site-python"
    python.write_bytes(b"python")
    python.chmod(0o755)
    env = closed_runtime_environment(
        plan,
        paths=paths,
        policy=plan,
        base_environment={
            QUALIFIED_PYTHON_ENV: str(python),
            QUALIFIED_PYTHON_HASH_ENV: "d" * 64,
            QUALIFIED_PYTHON_PROFILE_ENV: plan.site_profile_hash,
            COMPAT_SOURCE_PROFILE_ENV: plan.compatibility_profile_hash,
            COMPAT_SOURCE_MANIFEST_ENV: plan.manifest_hash,
            "BASH_ENV": "/tmp/inject.sh",
            "LD_PRELOAD": "/tmp/inject.so",
            "PYTHONWARNINGS": "error",
            "ODD_SETTING": "ambient-toggle",
        },
    )
    assert not {"BASH_ENV", "LD_PRELOAD", "PYTHONWARNINGS", "ODD_SETTING"} & set(env)


def test_closed_application_environment_discards_all_pbs_state(tmp_path):
    plan = _plan()
    paths = _paths(tmp_path)
    python = tmp_path / "site-python"
    python.write_bytes(b"python")
    python.chmod(0o755)
    env = closed_runtime_environment(
        plan,
        paths=paths,
        policy=plan,
        base_environment={
            QUALIFIED_PYTHON_ENV: str(python),
            QUALIFIED_PYTHON_HASH_ENV: "d" * 64,
            QUALIFIED_PYTHON_PROFILE_ENV: plan.site_profile_hash,
            COMPAT_SOURCE_PROFILE_ENV: plan.compatibility_profile_hash,
            COMPAT_SOURCE_MANIFEST_ENV: plan.manifest_hash,
            "PBS_JOBID": "123.aurora",
            "PBS_NODEFILE": "/var/spool/pbs/aux/123",
            "PBS_O_WORKDIR": "/home/user/project",
            "PBS_O_HOME": "/home/user",
            "PBS_O_PATH": "/home/user/bin:/usr/bin",
        },
    )
    assert not any(key.startswith("PBS_") for key in env)


def test_closed_rank_environment_projects_only_hash_bound_pmix_controls(tmp_path):
    from exaserve.site import AURORA_PMIX_PREPARED_ENVIRONMENT

    plan = _plan()
    paths = _paths(tmp_path)
    python = tmp_path / "site-python"
    python.write_bytes(b"python")
    python.chmod(0o755)
    policy = SimpleNamespace(
        site_id="alcf-aurora",
        model_storage_path="/lus/flare/models",
        filesystem_semantics=(
            ("shared_root:/home", "shared;fstype=lustre;readonly=false"),
            ("shared_root:/lus/flare", "shared;fstype=lustre;readonly=false"),
        ),
        prepared_environment=AURORA_PMIX_PREPARED_ENVIRONMENT,
        environment_unset=(),
    )
    env = closed_runtime_environment(
        plan,
        paths=paths,
        policy=policy,
        base_environment={
            QUALIFIED_PYTHON_ENV: str(python),
            QUALIFIED_PYTHON_HASH_ENV: "d" * 64,
            QUALIFIED_PYTHON_PROFILE_ENV: plan.site_profile_hash,
            COMPAT_SOURCE_PROFILE_ENV: plan.compatibility_profile_hash,
            COMPAT_SOURCE_MANIFEST_ENV: plan.manifest_hash,
            "PMIX_RANK": "forged-rank",
            "PALS_RANKID": "forged-rank",
            "PMIX_MCA_mca_base_param_files": "/home/user/.pmix/mca-params.conf",
            "PMIX_MCA_mca_base_component_path": "/home/user/.pmix/components",
        },
    )
    assert {key: env[key] for key, _value in AURORA_PMIX_PREPARED_ENVIRONMENT} == dict(
        AURORA_PMIX_PREPARED_ENVIRONMENT
    )
    assert "PMIX_RANK" not in env
    assert "PALS_RANKID" not in env
    assert_worker_launch_is_local(
        argv=(str(python), "-m", "exaserve.rank_main"),
        cwd=paths.python_root,
        environment=env,
        policy=policy,
    )


def test_local_containment_rejects_a_symlink_escape_to_shared_storage(tmp_path):
    local = tmp_path / "local"
    local.mkdir()
    (local / "escape").symlink_to("/home")
    with pytest.raises(RuntimePathError, match="shared root|symlink"):
        require_contained_local_path(
            local / "escape" / "user",
            local,
            policy=_plan(),
            require_exists=False,
        )


def test_worker_descriptor_rejects_shared_argv_and_missing_no_user_site(tmp_path):
    paths = _paths(tmp_path)
    with pytest.raises(RuntimePathError, match="argv.*shared storage"):
        assert_worker_launch_is_local(
            argv=("/lus/flare/project/python",),
            cwd=paths.python_root,
            environment={"PYTHONNOUSERSITE": "1"},
            policy=_plan(),
        )
    with pytest.raises(RuntimePathError, match="PYTHONNOUSERSITE"):
        assert_worker_launch_is_local(
            argv=("/usr/bin/python3",),
            cwd=paths.python_root,
            environment={},
            policy=_plan(),
        )
    with pytest.raises(RuntimePathError, match="launcher-only scheduler state"):
        assert_worker_launch_is_local(
            argv=("/usr/bin/python3",),
            cwd=paths.python_root,
            environment={
                "PYTHONNOUSERSITE": "1",
                "PYTHONSAFEPATH": "1",
                "PBS_NODEFILE": "/home/user/.aurora_leases/nodes",
            },
            policy=_plan(),
        )


def test_no_user_site_prevents_usercustomize_during_first_interpreter_start(tmp_path):
    custom = tmp_path / "usercustomize.py"
    marker = tmp_path / "loaded"
    custom.write_text(f"from pathlib import Path\nPath({str(marker)!r}).touch()\n")
    env = dict(os.environ)
    env.update(
        {
            "PYTHONNOUSERSITE": "1",
            "PYTHONSAFEPATH": "1",
            "PYTHONPATH": str(tmp_path),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    completed = subprocess.run(  # noqa: S603 - exact current interpreter
        [sys.executable, "-c", "import sys; assert 'usercustomize' not in sys.modules"],
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
    assert not marker.exists()
