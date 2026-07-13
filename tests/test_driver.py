import subprocess
import sys
import textwrap

from exaserve.driver import (
    EXASERVE_SERVE_READY_MARKER,
    ProcessOutputRelay,
    wait_for_process_ready_marker,
)


def _start_output_process(lines: list[str], delay_s: float = 0.05) -> subprocess.Popen[str]:
    script = textwrap.dedent(
        f"""
        import time

        lines = {lines!r}
        for line in lines:
            print(line, flush=True)
            time.sleep({delay_s})
        """
    )
    return subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )


def test_wait_for_process_ready_marker_detects_cluster_ready():
    process = _start_output_process(
        [
            "[ExaServe] Stage 1: Initializing Ray cluster...",
            f"{EXASERVE_SERVE_READY_MARKER} Total time: 12.34s",
        ]
    )
    relay = ProcessOutputRelay(
        process=process,
        ready_marker=EXASERVE_SERVE_READY_MARKER,
        label="ExaServe",
    ).start()

    try:
        assert wait_for_process_ready_marker(relay, timeout=2.0) is True
    finally:
        process.wait(timeout=2.0)
        relay.close()


def test_wait_for_process_ready_marker_fails_when_process_exits_early():
    process = _start_output_process(
        [
            "[ExaServe] Stage 1: Initializing Ray cluster...",
            "[ExaServe] Stage 3: Deploying model services...",
        ]
    )
    relay = ProcessOutputRelay(
        process=process,
        ready_marker=EXASERVE_SERVE_READY_MARKER,
        label="ExaServe",
    ).start()

    try:
        assert wait_for_process_ready_marker(relay, timeout=2.0) is False
        process.wait(timeout=2.0)
        if relay._thread is not None:
            relay._thread.join(timeout=1.0)
        assert list(relay.recent_lines) == [
            "[ExaServe] Stage 1: Initializing Ray cluster...",
            "[ExaServe] Stage 3: Deploying model services...",
        ]
    finally:
        relay.close()
