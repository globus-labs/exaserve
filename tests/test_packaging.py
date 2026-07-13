from importlib import resources

from exaserve.model_bcast import prepare_bcast_tools


def test_packaged_runtime_resources_are_present():
    resource_root = resources.files("exaserve.resources")

    for name in (
        "launch_cluster.sh",
        "distribute_to_nodes.sh",
        "bcast.c",
        "bcast.Makefile",
    ):
        assert (resource_root / name).is_file()


def test_bcast_sources_materialize_to_writable_build_dir(tmp_path):
    tools_dir = prepare_bcast_tools(tmp_path / "bcast")

    assert (tools_dir / "bcast.c").is_file()
    assert (tools_dir / "Makefile").is_file()
