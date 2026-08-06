import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple


class NetstatsProcess:
    def __init__(self, *, output_path: str, interval_s: float = 1.0, interfaces: str = "", hostname: Optional[str] = None) -> None:
        script = Path(__file__).resolve().parents[2] / "benchmarks" / "netstats.py"
        cmd = [
            sys.executable,
            str(script),
            "--interval",
            str(interval_s),
            "--output",
            output_path,
        ]
        if interfaces:
            cmd.extend(["--interfaces", interfaces])
        if hostname:
            cmd.extend(["--hostname", hostname])
        self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    def stop(self) -> Tuple[str, str]:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        stdout = self.process.stdout.read() if self.process.stdout else ""
        stderr = self.process.stderr.read() if self.process.stderr else ""
        return stdout, stderr


def stop_remote_netstats(node: str) -> None:
    # PR-030: argument-vector SSH like every other remote call here; no
    # shell-string interpolation of the node value.
    subprocess.run(
        ["ssh", node, "pkill", "-f", "netstats.py"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
