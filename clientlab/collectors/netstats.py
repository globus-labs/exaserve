import sys
from pathlib import Path
from typing import Optional, Tuple

from exaserve.control.supervisor import BoundedOutputCapture, ManagedComponent


class NetstatsProcess:
    def __init__(
        self,
        *,
        output_path: str,
        interval_s: float = 1.0,
        interfaces: str = "",
        hostname: Optional[str] = None,
    ) -> None:
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
        self.capture = BoundedOutputCapture(max_bytes=64 << 10)
        self.component = ManagedComponent(
            component_id="clientlab-netstats",
            argv=cmd,
            long_lived=True,
            output_capture=self.capture,
        )
        self.component.start()

    def stop(self) -> Tuple[str, str]:
        self.component.stop("collector complete", deadline_s=10.0)
        return self.capture.snapshot()["tail"], ""
