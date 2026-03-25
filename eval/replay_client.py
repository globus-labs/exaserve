from __future__ import annotations

import argparse
import asyncio

try:
    from .lib.replay_engine import replay_from_manifest
except ImportError:  # pragma: no cover - script-mode fallback
    try:
        from eval.lib.replay_engine import replay_from_manifest
    except ImportError:
        from lib.replay_engine import replay_from_manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay pre-built eval traces against a running backend.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--include-tp", action="store_true")
    parser.add_argument("--early-stop", type=float, default=None)
    parser.add_argument("--num-runs", type=int, default=None)
    parser.add_argument("--dest", "--destination", dest="dest", choices=["proxy", "direct"], default=None)
    parser.add_argument("--proxy-port", type=int, default=None, dest="proxy_port")
    parser.add_argument("--base-urls", type=str, default=None, dest="base_urls")
    parser.add_argument("--cpuprofile-dir", type=str, default="", dest="cpuprofile_dir")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    asyncio.run(
        replay_from_manifest(
            args.config,
            include_tp_override=(True if args.include_tp else None),
            early_stop_override=args.early_stop,
            num_runs_override=args.num_runs,
            dest_override=args.dest,
            proxy_port=args.proxy_port,
            base_urls_override=args.base_urls,
            cpuprofile_dir=args.cpuprofile_dir,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
