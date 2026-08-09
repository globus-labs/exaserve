#!/usr/bin/env python3
"""Own one TCP listener for an external qualification fault injection.

The final two-node harness starts this helper through SSH on an allocated
worker.  A machine-readable handshake gives the harness the exact remote PID
it must terminate; no process-name matching or allocation-wide cleanup is
used.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be in 1..65535")

    stopping = False

    def request_stop(_signum, _frame) -> None:
        nonlocal stopping
        stopping = True

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, request_stop)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind((args.host, args.port))
        listener.listen(8)
        listener.settimeout(0.5)
        print(
            json.dumps(
                {
                    "schema_version": 1,
                    "state": "READY",
                    "hostname": socket.gethostname(),
                    "bind_host": args.host,
                    "port": args.port,
                    "pid": os.getpid(),
                },
                sort_keys=True,
            ),
            flush=True,
        )
        while not stopping:
            try:
                connection, _peer = listener.accept()
            except TimeoutError:
                continue
            with connection:
                # Accept and close probes.  This is deliberately not an HTTP
                # health endpoint, so it cannot impersonate a healthy proxy.
                pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
