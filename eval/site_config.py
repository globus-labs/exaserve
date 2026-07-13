import argparse
import importlib.util
import json
import os
from collections import OrderedDict
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Optional


_FIELD_DEFAULTS = OrderedDict(
    [
        ("project_root", "/lus/flare/projects/AuroraGPT"),
        ("user_data_root", ""),
        ("model_storage_path", ""),
        ("input_trace_path", ""),
        ("input_prompt_path", ""),
        ("output_trace_dir", ""),
        ("experiments_root", ""),
        ("litellm_python_path", ""),
        ("snapshot_dir", ""),
        ("bench_results_dir", ""),
        ("pbs_mail_user", ""),
        ("env_script_aurora", ""),
        ("env_script_litellm", ""),
        ("num_gpus_per_node", 12),
        ("local_stage_path", "/tmp/hf_home"),
        # Python for the SGLang serving stack (frameworks-inheriting venv with SGLang
        # added). Used when a spec sets deployment.engine: sglang. Empty -> frameworks.
        ("sglang_python_path", "/home/wenyiw/sglang_test/fwvenv/bin/python"),
    ]
)
_FIELD_NAMES = set(_FIELD_DEFAULTS)
_FIELD_TYPES = {
    "num_gpus_per_node": int,
}
_PATH_FIELDS = {
    "project_root",
    "user_data_root",
    "model_storage_path",
    "input_trace_path",
    "input_prompt_path",
    "output_trace_dir",
    "experiments_root",
    "litellm_python_path",
    "snapshot_dir",
    "bench_results_dir",
    "env_script_aurora",
    "env_script_litellm",
    "local_stage_path",
}
_CACHE = None


class SiteConfig(object):
    __slots__ = tuple(_FIELD_DEFAULTS.keys())

    def __init__(self, **kwargs: Any) -> None:
        unknown = sorted(set(kwargs) - _FIELD_NAMES)
        if unknown:
            raise KeyError("Unknown site config field(s): {0}".format(", ".join(unknown)))
        for field_name, default_value in _FIELD_DEFAULTS.items():
            setattr(self, field_name, kwargs.get(field_name, default_value))

    def to_dict(self) -> Dict[str, Any]:
        return {field_name: getattr(self, field_name) for field_name in _FIELD_DEFAULTS}

    def with_updates(self, **updates: Any) -> "SiteConfig":
        data = self.to_dict()
        data.update(updates)
        return SiteConfig(**data)

    def __repr__(self) -> str:
        field_parts = [
            "{0}={1!r}".format(field_name, getattr(self, field_name))
            for field_name in _FIELD_DEFAULTS
        ]
        return "SiteConfig({0})".format(", ".join(field_parts))


def clear_site_config_cache() -> None:
    global _CACHE
    _CACHE = None


def _coerce_field_value(field_name: str, raw_value: Any) -> Any:
    field_type = _FIELD_TYPES.get(field_name)
    if field_type is int:
        return int(raw_value)
    value = str(raw_value)
    if field_name in _PATH_FIELDS and value:
        return os.path.expanduser(value)
    return value


def _load_local_override_module(path: Path) -> Optional[ModuleType]:
    if not path.is_file():
        return None
    spec = importlib.util.spec_from_file_location("_exaserve_site_config_local", str(path))
    if spec is None or spec.loader is None:
        raise ImportError("Could not load site config overrides from {0}".format(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_local_overrides() -> Dict[str, Any]:
    override_path = os.environ.get("EXASERVE_SITE_CONFIG_LOCAL")
    if override_path:
        path = Path(os.path.expanduser(override_path))
    else:
        path = Path(__file__).with_name("site_config_local.py")
    module = _load_local_override_module(path)
    if module is None:
        return {}
    overrides = getattr(module, "SITE_OVERRIDES", {})
    if not isinstance(overrides, dict):
        raise TypeError("SITE_OVERRIDES in {0} must be a dict".format(path))
    unknown = sorted(set(overrides) - _FIELD_NAMES)
    if unknown:
        raise KeyError("Unknown site config override field(s): {0}".format(", ".join(unknown)))
    return overrides


def _load_env_overrides() -> Dict[str, Any]:
    overrides = {}
    for field_name in _FIELD_NAMES:
        env_name = "EXASERVE_{0}".format(field_name.upper())
        if env_name in os.environ:
            overrides[field_name] = os.environ[env_name]
    return overrides


def _apply_overrides(config: SiteConfig, overrides: Dict[str, Any]) -> SiteConfig:
    if not overrides:
        return config
    normalized = {
        key: _coerce_field_value(key, value)
        for key, value in overrides.items()
        if value is not None
    }
    return config.with_updates(**normalized)


def _normalize_config(config: SiteConfig) -> SiteConfig:
    current_user = os.environ.get("USER") or Path.home().name or "user"
    home_dir = str(Path.home())

    project_root = _coerce_field_value("project_root", config.project_root)
    user_data_root = config.user_data_root or os.path.join(project_root, current_user, "data")
    model_storage_path = config.model_storage_path or os.path.join(project_root, current_user, "models")
    input_trace_path = config.input_trace_path or os.path.join(
        user_data_root,
        "input_traces",
        "AzureLLMInferenceTrace_code_1week.csv",
    )
    input_prompt_path = config.input_prompt_path or os.path.join(
        user_data_root,
        "input_traces",
        "ShareGPT_V3_unfiltered_cleaned_split_no_imsorry.json",
    )
    output_trace_dir = config.output_trace_dir or os.path.join(user_data_root, "output_traces")
    experiments_root = config.experiments_root or os.path.join(user_data_root, "experiments")
    litellm_python_path = config.litellm_python_path or os.path.join(
        home_dir,
        "agpt",
        "venv",
        "litellm",
        "bin",
        "python3",
    )
    snapshot_dir = config.snapshot_dir or os.path.join(home_dir, "agpt", "data", "snapshots")
    bench_results_dir = config.bench_results_dir or os.path.join(
        home_dir,
        "agpt",
        "data",
        "bench_results",
    )
    env_script_aurora = config.env_script_aurora or os.path.join(home_dir, "script", "env_aurora")
    env_script_litellm = config.env_script_litellm or os.path.join(home_dir, "script", "env_litellm")

    return config.with_updates(
        project_root=project_root,
        user_data_root=_coerce_field_value("user_data_root", user_data_root),
        model_storage_path=_coerce_field_value("model_storage_path", model_storage_path),
        input_trace_path=_coerce_field_value("input_trace_path", input_trace_path),
        input_prompt_path=_coerce_field_value("input_prompt_path", input_prompt_path),
        output_trace_dir=_coerce_field_value("output_trace_dir", output_trace_dir),
        experiments_root=_coerce_field_value("experiments_root", experiments_root),
        litellm_python_path=_coerce_field_value("litellm_python_path", litellm_python_path),
        snapshot_dir=_coerce_field_value("snapshot_dir", snapshot_dir),
        bench_results_dir=_coerce_field_value("bench_results_dir", bench_results_dir),
        env_script_aurora=_coerce_field_value("env_script_aurora", env_script_aurora),
        env_script_litellm=_coerce_field_value("env_script_litellm", env_script_litellm),
        local_stage_path=_coerce_field_value("local_stage_path", config.local_stage_path),
    )


def get_site_config() -> SiteConfig:
    global _CACHE
    if _CACHE is not None:
        return _CACHE

    config = SiteConfig()
    config = _apply_overrides(config, _load_local_overrides())
    config = _apply_overrides(config, _load_env_overrides())
    _CACHE = _normalize_config(config)
    return _CACHE


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect shared Aurora site config defaults.")
    subparsers = parser.add_subparsers(dest="command")

    get_parser = subparsers.add_parser("get", help="Print one config field.")
    get_parser.add_argument("field", choices=sorted(_FIELD_NAMES))

    subparsers.add_parser("json", help="Print the full config as JSON.")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    config = get_site_config()

    if args.command == "get":
        print(getattr(config, args.field))
        return 0

    print(json.dumps(config.to_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
