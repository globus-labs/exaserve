import json
import math
import threading
import time
from pathlib import Path


EPHEMERAL_MIN = 32768
EPHEMERAL_MAX = 60999
EPHEMERAL_RANGE = EPHEMERAL_MAX - EPHEMERAL_MIN + 1
TCP_STATES = {
    "01": "ESTABLISHED",
    "06": "TIME_WAIT",
    "02": "SYN_SENT",
    "08": "CLOSE_WAIT",
}


def take_snapshot():
    counts = {"ESTABLISHED": 0, "TIME_WAIT": 0, "SYN_SENT": 0, "CLOSE_WAIT": 0}
    ephemeral = 0
    for path in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(path, "r") as handle:
                for line in handle:
                    line = line.strip()
                    if not line or line.startswith("sl"):
                        continue
                    parts = line.split()
                    if len(parts) < 4:
                        continue
                    state = TCP_STATES.get(parts[3].upper())
                    if state:
                        counts[state] += 1
                    local = parts[1]
                    if ":" in local and state in ("ESTABLISHED", "TIME_WAIT", "SYN_SENT"):
                        try:
                            local_port = int(local.split(":")[1], 16)
                        except ValueError:
                            continue
                        if EPHEMERAL_MIN <= local_port <= EPHEMERAL_MAX:
                            ephemeral += 1
        except IOError:
            continue
    return {
        "timestamp": time.time(),
        "established": counts["ESTABLISHED"],
        "time_wait": counts["TIME_WAIT"],
        "syn_sent": counts["SYN_SENT"],
        "close_wait": counts["CLOSE_WAIT"],
        "ephemeral_in_use": ephemeral,
    }


class PortCollector(object):
    def __init__(self, interval_s=1.0):
        if (
            isinstance(interval_s, bool)
            or not isinstance(interval_s, (int, float))
            or not math.isfinite(float(interval_s))
            or interval_s <= 0
        ):
            raise ValueError("port collector interval must be finite and positive")
        self.interval_s = float(interval_s)
        self.samples = []
        self._stop = threading.Event()
        self._thread = None
        self._failure = None
        self._failure_lock = threading.Lock()

    def start(self):
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("port collector is already running")
        self._stop.clear()
        with self._failure_lock:
            self._failure = None
        self._thread = threading.Thread(target=self._run, name="clientlab-port-collector")
        self._thread.daemon = True
        self._thread.start()

    def _run(self):
        try:
            while not self._stop.is_set():
                self.samples.append(take_snapshot())
                self._stop.wait(self.interval_s)
        except BaseException as exc:
            with self._failure_lock:
                self._failure = exc
            self._stop.set()

    def stop(self, timeout_s=None):
        timeout_s = self.interval_s + 2.0 if timeout_s is None else timeout_s
        if (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or not math.isfinite(float(timeout_s))
            or timeout_s < 0
        ):
            raise ValueError("port collector stop timeout must be finite and nonnegative")
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=float(timeout_s))
            if self._thread.is_alive():
                raise RuntimeError("port collector thread did not stop by its deadline")
        with self._failure_lock:
            failure = self._failure
        if failure is not None:
            raise RuntimeError(
                f"port collector failed: {type(failure).__name__}: {failure}"
            ) from failure
        ephem = [
            sample["ephemeral_in_use"] for sample in self.samples if sample["ephemeral_in_use"] >= 0
        ]
        return {
            "ephemeral_range": EPHEMERAL_RANGE,
            "sample_count": len(self.samples),
            "max_established": max([sample["established"] for sample in self.samples] or [0]),
            "max_time_wait": max([sample["time_wait"] for sample in self.samples] or [0]),
            "max_ephemeral_in_use": max(ephem or [-1]),
            "samples": list(self.samples),
        }


def write_port_metrics(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    # PR-035: atomic publish so a polling reader never sees a partial file.
    from ..utils import atomic_write_text

    atomic_write_text(target, json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n")
