#!/usr/bin/env python3
"""Declared HAProxy no-delay characterization for AC-PROXY-01.

This harness exercises the immutable packaged ExaServe candidate through its
canonical composition root.  It deliberately reuses the already-qualified
lifecycle harness's process/status helpers, and binds that support file by
path and hash in the experiment declaration.  The workload is bounded by
duration, worker count, request count, and per-request timeout.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request

from scripts.hardening import run_final_null_qualification as lifecycle


_PLAN_FIELDS = {"schema_version", "created_at", "candidate", "harness", "support", "gates"}
_CANDIDATE_FIELDS = {
    "release_path",
    "artifact_manifest_path",
    "artifact_manifest_sha256",
    "wheel_path",
    "wheel_sha256",
    "sdist_path",
    "sdist_sha256",
    "bootstrap_path",
    "site_profile_hash",
    "compatibility_profile_hash",
    "compatibility_manifest_hash",
}
_CODE_FIELDS = {"path", "sha256"}
_GATE_FIELDS = {
    "gate_id",
    "lane",
    "logical_nodes",
    "physical_allocation_nodes",
    "acquisition_source",
    "queue",
    "lease_ttl",
    "expected_runtime",
    "node_hours",
    "attempt_limit",
    "attempt",
    "output_path",
    "ready_timeout_s",
    "config_path",
    "config_sha256",
    "deployment_plan_path",
    "deployment_plan_sha256",
    "site_profile_path",
    "site_profile_sha256",
    "http_no_delay",
    "client_duration_s",
    "client_workers",
    "client_max_requests",
    "max_tokens",
    "request_timeout_s",
    "sample_interval_s",
    "clean_state_reset_method",
    "retry_reason_policy",
    "expected_observations",
}
_EXPECTED_OBSERVATIONS = [
    "fresh generation reaches canonical READY with exact candidate receipts",
    "advertised HAProxy endpoint returns a typed completion",
    "bounded streaming clients complete through the declared no-delay arm",
    "every request and the qualification run have unique durable identities",
    "gateway PID and Linux start ticks remain exact for every sample",
    "samples record gateway process state, CPU, threads, file descriptors, TCP retransmits, and connection states",
    "SIGTERM drains and publishes STOPPED with exit 143 and bounded cleanup",
]
_TCP_STATES = {
    "01": "ESTABLISHED",
    "02": "SYN_SENT",
    "03": "SYN_RECV",
    "04": "FIN_WAIT1",
    "05": "FIN_WAIT2",
    "06": "TIME_WAIT",
    "07": "CLOSE",
    "08": "CLOSE_WAIT",
    "09": "LAST_ACK",
    "0A": "LISTEN",
    "0B": "CLOSING",
}


def _load_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"expected a JSON object: {path}")
    return value


def _exact_object(value: object, fields: set[str], context: str) -> dict:
    if not isinstance(value, dict):
        raise RuntimeError(f"{context} must be an object")
    if set(value) != fields:
        raise RuntimeError(
            f"{context} shape mismatch: unknown={sorted(set(value) - fields)}, "
            f"missing={sorted(fields - set(value))}"
        )
    return value


def _declared_path(root: Path, value: object, context: str, *, kind: str) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise RuntimeError(f"{context} must be a non-empty repository-relative path")
    path = (root / value).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeError(f"{context} escapes the repository") from exc
    if kind == "file" and not path.is_file():
        raise RuntimeError(f"{context} is not a file: {path}")
    if kind == "directory" and not path.is_dir():
        raise RuntimeError(f"{context} is not a directory: {path}")
    if kind not in {"file", "directory", "output"}:
        raise AssertionError(f"unsupported path kind {kind}")
    return path


def _finite_number(value: object, context: str, *, minimum: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < minimum
    ):
        raise RuntimeError(f"{context} must be finite and >= {minimum}")
    return float(value)


def _positive_integer(value: object, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise RuntimeError(f"{context} must be a positive integer")
    return value


def _load_gate(experiment_path: Path, gate_id: str) -> tuple[dict, dict, dict[str, Path]]:
    root = Path(__file__).resolve().parents[2]
    document = _exact_object(_load_json(experiment_path), _PLAN_FIELDS, "experiment plan")
    if document["schema_version"] != 1:
        raise RuntimeError("proxy experiment plan schema_version must be 1")
    if not isinstance(document["created_at"], str) or not document["created_at"]:
        raise RuntimeError("experiment plan created_at must be non-empty text")
    candidate = _exact_object(document["candidate"], _CANDIDATE_FIELDS, "candidate")
    harness = _exact_object(document["harness"], _CODE_FIELDS, "harness")
    support = _exact_object(document["support"], _CODE_FIELDS, "support")
    harness_path = _declared_path(root, harness["path"], "harness.path", kind="file")
    support_path = _declared_path(root, support["path"], "support.path", kind="file")
    if harness_path != Path(__file__).resolve():
        raise RuntimeError("experiment plan does not name the running proxy harness")
    if support_path != Path(lifecycle.__file__).resolve():
        raise RuntimeError("experiment plan does not name the imported lifecycle support")
    for declaration, path, context in (
        (harness, harness_path, "harness"),
        (support, support_path, "support"),
    ):
        if lifecycle._sha256_file(path) != declaration["sha256"]:
            raise RuntimeError(f"{context} bytes changed after experiment declaration")

    release = _declared_path(
        root, candidate["release_path"], "candidate.release_path", kind="directory"
    )
    paths = {
        "release": release,
        "artifact_manifest": _declared_path(
            root,
            candidate["artifact_manifest_path"],
            "candidate.artifact_manifest_path",
            kind="file",
        ),
        "wheel": _declared_path(root, candidate["wheel_path"], "candidate.wheel_path", kind="file"),
        "sdist": _declared_path(root, candidate["sdist_path"], "candidate.sdist_path", kind="file"),
        "bootstrap": _declared_path(
            root, candidate["bootstrap_path"], "candidate.bootstrap_path", kind="directory"
        ),
    }
    for field, path in (
        ("artifact_manifest_sha256", paths["artifact_manifest"]),
        ("wheel_sha256", paths["wheel"]),
        ("sdist_sha256", paths["sdist"]),
    ):
        if lifecycle._sha256_file(path) != candidate[field]:
            raise RuntimeError(f"candidate bytes changed for {field}")
    if paths["artifact_manifest"].parent != release or paths["wheel"].parent != release:
        raise RuntimeError("candidate artifacts are outside the declared release directory")

    gates = document["gates"]
    if not isinstance(gates, list) or any(not isinstance(row, dict) for row in gates):
        raise RuntimeError("experiment plan gates must be a list of objects")
    gate_ids = [row.get("gate_id") for row in gates]
    if len(gate_ids) != len(set(gate_ids)):
        raise RuntimeError("experiment plan gate IDs must be unique")
    matches = [row for row in gates if row.get("gate_id") == gate_id]
    if len(matches) != 1:
        raise RuntimeError(f"experiment plan does not contain exactly one gate {gate_id!r}")
    gate = _exact_object(matches[0], _GATE_FIELDS, f"gate {gate_id}")
    if (
        gate["lane"] != "FINAL"
        or gate["logical_nodes"] != 1
        or gate["physical_allocation_nodes"] != 1
        or gate["acquisition_source"] != "subjob"
        or gate["queue"] != "capacity"
        or gate["attempt"] != 1
        or gate["attempt_limit"] != 1
        or not isinstance(gate["http_no_delay"], bool)
        or gate["expected_observations"] != _EXPECTED_OBSERVATIONS
    ):
        raise RuntimeError(f"gate {gate_id} is outside the exact proxy qualification contract")
    _finite_number(gate["ready_timeout_s"], "ready_timeout_s", minimum=1)
    _finite_number(gate["client_duration_s"], "client_duration_s", minimum=5)
    _finite_number(gate["request_timeout_s"], "request_timeout_s", minimum=1)
    _finite_number(gate["sample_interval_s"], "sample_interval_s", minimum=0.1)
    for field in ("client_workers", "client_max_requests", "max_tokens"):
        _positive_integer(gate[field], field)
    if gate["client_max_requests"] < gate["client_workers"]:
        raise RuntimeError("client_max_requests must permit at least one request per worker")

    paths.update(
        {
            "output": _declared_path(root, gate["output_path"], "gate.output_path", kind="output"),
            "config": _declared_path(root, gate["config_path"], "gate.config_path", kind="file"),
            "deployment_plan": _declared_path(
                root, gate["deployment_plan_path"], "gate.deployment_plan_path", kind="file"
            ),
            "site_profile": _declared_path(
                root, gate["site_profile_path"], "gate.site_profile_path", kind="file"
            ),
        }
    )
    for field, path in (
        ("config_sha256", paths["config"]),
        ("deployment_plan_sha256", paths["deployment_plan"]),
        ("site_profile_sha256", paths["site_profile"]),
    ):
        if lifecycle._sha256_file(path) != gate[field]:
            raise RuntimeError(f"declared input bytes changed for {field}")
    return document, gate, paths


def _process_sample(
    pid: int, start_ticks: int, gateway_port: int, *, proc_root: Path = Path("/proc")
) -> dict:
    process = proc_root / str(pid)
    stat_text = (process / "stat").read_text(encoding="utf-8")
    tail = stat_text.rsplit(") ", 1)[1].split()
    observed_start = int(tail[19])
    if observed_start != start_ticks:
        raise RuntimeError(f"gateway PID {pid} was reused during qualification")
    status = (process / "status").read_text(encoding="utf-8")
    uid_line = next(line for line in status.splitlines() if line.startswith("Uid:"))
    if int(uid_line.split()[1]) != os.getuid():
        raise RuntimeError("gateway process ownership changed during qualification")
    return {
        "observed_at": time.time(),
        "observed_monotonic": time.monotonic(),
        "pid": pid,
        "process_start_ticks": observed_start,
        "process_state": tail[0],
        "cpu_ticks": int(tail[11]) + int(tail[12]),
        "threads": int(tail[17]),
        "rss_bytes": int(tail[21]) * os.sysconf("SC_PAGE_SIZE"),
        "open_fds": len(list((process / "fd").iterdir())),
        "tcp": _tcp_counters(proc_root),
        "connections": _connection_states(gateway_port, proc_root),
    }


def _tcp_counters(proc_root: Path = Path("/proc")) -> dict[str, int]:
    lines = (proc_root / "net/snmp").read_text(encoding="utf-8").splitlines()
    pairs = [line.split() for line in lines if line.startswith("Tcp:")]
    if len(pairs) != 2 or len(pairs[0]) != len(pairs[1]):
        raise RuntimeError("/proc/net/snmp does not contain one typed TCP counter pair")
    values = dict(zip(pairs[0][1:], pairs[1][1:], strict=True))
    required = ("ActiveOpens", "PassiveOpens", "CurrEstab", "RetransSegs")
    if any(name not in values for name in required):
        raise RuntimeError("/proc/net/snmp lacks required TCP counters")
    return {name: int(values[name]) for name in required}


def _connection_states(gateway_port: int, proc_root: Path = Path("/proc")) -> dict[str, int]:
    counts = {state: 0 for state in _TCP_STATES.values()}
    for table in (proc_root / "net/tcp", proc_root / "net/tcp6"):
        for line in table.read_text(encoding="utf-8").splitlines()[1:]:
            fields = line.split()
            if len(fields) < 4:
                continue
            local_port = int(fields[1].rsplit(":", 1)[1], 16)
            remote_port = int(fields[2].rsplit(":", 1)[1], 16)
            if gateway_port not in {local_port, remote_port}:
                continue
            state = _TCP_STATES.get(fields[3].upper())
            if state is not None:
                counts[state] += 1
    counts["TOTAL"] = sum(counts.values())
    return counts


class _GatewaySampler:
    def __init__(
        self,
        *,
        pid: int,
        gateway_port: int,
        interval_s: float,
        proc_root: Path = Path("/proc"),
    ) -> None:
        self.pid = pid
        self.gateway_port = gateway_port
        self.interval_s = interval_s
        self.proc_root = proc_root
        self.start_ticks = lifecycle._process_start_ticks(pid)
        self.samples: list[dict] = []
        self.stop_event = threading.Event()
        self.failure: BaseException | None = None
        self.thread = threading.Thread(target=self._run, name="gateway-sampler", daemon=False)

    def start(self) -> None:
        self.thread.start()

    def _run(self) -> None:
        try:
            while not self.stop_event.is_set():
                self.samples.append(
                    _process_sample(
                        self.pid,
                        self.start_ticks,
                        self.gateway_port,
                        proc_root=self.proc_root,
                    )
                )
                self.stop_event.wait(self.interval_s)
        except BaseException as exc:
            self.failure = exc
            self.stop_event.set()

    def stop(self) -> dict:
        self.stop_event.set()
        self.thread.join(self.interval_s + 5.0)
        if self.thread.is_alive():
            raise RuntimeError("gateway sampler did not stop by its deadline")
        if self.failure is not None:
            raise RuntimeError(f"gateway sampler failed: {self.failure}") from self.failure
        self.samples.append(
            _process_sample(
                self.pid,
                self.start_ticks,
                self.gateway_port,
                proc_root=self.proc_root,
            )
        )
        if len(self.samples) < 2:
            raise RuntimeError("gateway sampler captured fewer than two samples")
        first, last = self.samples[0], self.samples[-1]
        duration = last["observed_monotonic"] - first["observed_monotonic"]
        cpu_delta = last["cpu_ticks"] - first["cpu_ticks"]
        if duration <= 0 or cpu_delta < 0:
            raise RuntimeError("gateway samples have non-monotonic time or CPU counters")
        retransmits = last["tcp"]["RetransSegs"] - first["tcp"]["RetransSegs"]
        if retransmits < 0:
            raise RuntimeError("host TCP retransmission counter moved backwards")
        return {
            "schema_version": 1,
            "gateway_process": {
                "pid": self.pid,
                "process_start_ticks": self.start_ticks,
            },
            "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
            "sample_count": len(self.samples),
            "duration_s": duration,
            "cpu_percent": 100.0 * cpu_delta / float(os.sysconf("SC_CLK_TCK")) / duration,
            "cpu_ticks_delta": cpu_delta,
            "retransmits_delta": retransmits,
            "active_opens_delta": last["tcp"]["ActiveOpens"] - first["tcp"]["ActiveOpens"],
            "passive_opens_delta": last["tcp"]["PassiveOpens"] - first["tcp"]["PassiveOpens"],
            "peak_connections": max(sample["connections"]["TOTAL"] for sample in self.samples),
            "peak_established": max(
                sample["connections"]["ESTABLISHED"] for sample in self.samples
            ),
            "peak_threads": max(sample["threads"] for sample in self.samples),
            "peak_open_fds": max(sample["open_fds"] for sample in self.samples),
            "peak_rss_bytes": max(sample["rss_bytes"] for sample in self.samples),
            "process_states": sorted({sample["process_state"] for sample in self.samples}),
            "samples": self.samples,
        }


def _stream_request(
    endpoint: str, model_id: str, request_id: str, max_tokens: int, timeout: float
) -> dict:
    body = json.dumps(
        {
            "model": model_id,
            "prompt": "bounded proxy qualification request",
            "max_tokens": max_tokens,
            "stream": True,
        }
    ).encode()
    request = urllib.request.Request(
        endpoint.rstrip("/") + "/v1/completions",
        data=body,
        headers={"Content-Type": "application/json", "X-Request-ID": request_id},
        method="POST",
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    started = time.monotonic()
    try:
        with opener.open(request, timeout=timeout) as response:
            raw = response.read(8 << 20)
            status = response.status
    except (TimeoutError, urllib.error.URLError) as exc:
        raise RuntimeError(f"stream request {request_id} failed: {exc}") from exc
    if status != 200 or b"data:" not in raw or b"[DONE]" not in raw:
        raise RuntimeError(
            f"stream request {request_id} returned status={status}, bytes={len(raw)}, "
            f"done={b'[DONE]' in raw}"
        )
    return {
        "request_id": request_id,
        "status_code": status,
        "response_bytes": len(raw),
        "duration_s": time.monotonic() - started,
    }


def _bounded_clients(endpoint: str, model_id: str, gate: dict) -> dict:
    duration_s = float(gate["client_duration_s"])
    workers = int(gate["client_workers"])
    max_requests = int(gate["client_max_requests"])
    deadline = time.monotonic() + duration_s
    lock = threading.Lock()
    next_index = 0
    successes: list[dict] = []
    failures: list[dict] = []

    def worker() -> None:
        nonlocal next_index
        while time.monotonic() < deadline:
            with lock:
                if next_index >= max_requests:
                    return
                index = next_index
                next_index += 1
            request_id = f"{gate['gate_id'].lower()}-request-{index:06d}"
            try:
                result = _stream_request(
                    endpoint,
                    model_id,
                    request_id,
                    int(gate["max_tokens"]),
                    float(gate["request_timeout_s"]),
                )
            except BaseException as exc:
                with lock:
                    failures.append({"request_id": request_id, "error": str(exc)})
            else:
                with lock:
                    successes.append(result)

    started = time.time()
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="proxy-client") as pool:
        futures = [pool.submit(worker) for _ in range(workers)]
        for future in futures:
            future.result(timeout=duration_s + float(gate["request_timeout_s"]) + 10.0)
    request_ids = [item["request_id"] for item in successes + failures]
    if len(request_ids) != len(set(request_ids)):
        raise RuntimeError("bounded client reused a request identity")
    if len(successes) < workers or failures:
        raise RuntimeError(
            f"bounded proxy workload is incomplete: successes={len(successes)}, "
            f"failures={failures[:8]}"
        )
    return {
        "schema_version": 1,
        "run_id": gate["gate_id"].lower(),
        "started_at": started,
        "completed_at": time.time(),
        "duration_limit_s": duration_s,
        "worker_limit": workers,
        "request_limit": max_requests,
        "request_timeout_s": gate["request_timeout_s"],
        "max_tokens": gate["max_tokens"],
        "issued": len(request_ids),
        "completed": len(successes),
        "failed": len(failures),
        "unique_request_ids": len(set(request_ids)),
        "requests": sorted(successes, key=lambda item: item["request_id"]),
        "failures": failures,
    }


def _launch(
    *,
    output: Path,
    plan_path: Path,
    site_path: Path,
    gate: dict,
    nodes: tuple[str, ...],
) -> dict:
    from exaserve.plan.io import load_deployment_plan
    from exaserve.status_api import require_ready_endpoint

    plan = load_deployment_plan(str(plan_path))
    run_dir = output / "deployment"
    run_dir.mkdir()
    argv = [sys.executable, "-u", "-m", "exaserve.launcher", str(plan_path)]
    lifecycle._atomic_text(output / "command.txt", shlex.join(argv) + "\n")
    generation = time.time_ns()
    environment = os.environ.copy()
    environment.update(
        {
            "EXASERVE_GENERATION": str(generation),
            "EXASERVE_DEPLOYMENT_ID": plan.deployment_id,
            "EXASERVE_RUN_LOG_DIR": str(run_dir),
            "EXASERVE_SITE_PROFILE_PATH": str(site_path),
            "EXASERVE_NODEFILE": os.environ["PBS_NODEFILE"],
            "EXASERVE_SCHEDULER": "pbs",
            "EXASERVE_VENDOR": "xpu",
        }
    )
    process = subprocess.Popen(
        argv,
        cwd=output,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    assert process.stdout is not None and process.stderr is not None
    stdout = lifecycle._Tee(process.stdout, output / "stdout.log", "proxy:stdout")
    stderr = lifecycle._Tee(process.stderr, output / "stderr.log", "proxy:stderr")
    stdout.start()
    stderr.start()
    started = time.time()
    try:
        status = lifecycle._wait_status(
            run_dir,
            generation,
            plan.deployment_plan_hash,
            float(gate["ready_timeout_s"]),
            process=process,
        )
        if not status.ready:
            cleanup = lifecycle._await_premature_terminal_cleanup(process, run_dir, status, plan)
            raise RuntimeError(f"proxy qualification became terminal before READY: {cleanup}")
        endpoint = require_ready_endpoint(
            str(run_dir),
            expected_generation=generation,
            expected_plan_hash=plan.deployment_plan_hash,
        )
        canary = [lifecycle._canary(endpoint, model.model_id) for model in plan.models]
        ready_evidence = lifecycle._validate_ready_evidence(status, plan, run_dir)
        receipt_manifest = lifecycle._read_json(Path(status.receipt_manifest_path))
        gateway_pid = lifecycle._owned_gateway_pid(receipt_manifest, plan)
        sampler = _GatewaySampler(
            pid=gateway_pid,
            gateway_port=plan.gateway.port,
            interval_s=float(gate["sample_interval_s"]),
        )
        sampler.start()
        workload_error: BaseException | None = None
        workload: dict | None = None
        try:
            workload = _bounded_clients(endpoint, plan.models[0].model_id, gate)
        except BaseException as exc:
            workload_error = exc
        diagnostics = sampler.stop()
        if workload_error is not None:
            lifecycle._add_note(
                workload_error,
                f"gateway diagnostics captured before workload failure: {diagnostics}",
            )
            raise workload_error
        assert workload is not None
        if diagnostics["duration_s"] < float(gate["client_duration_s"]) * 0.8:
            raise RuntimeError("gateway diagnostics did not cover the bounded client interval")
        lifecycle._atomic_json(output / "canary.json", canary)
        lifecycle._atomic_json(output / "client_workload.json", workload)
        lifecycle._atomic_json(output / "gateway_diagnostics.json", diagnostics)

        process.send_signal(signal.SIGTERM)
        try:
            returncode = process.wait(timeout=lifecycle._owner_exit_timeout_s(plan))
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                "proxy qualification launcher did not drain by its deadline"
            ) from exc
        if returncode != 143:
            raise RuntimeError(f"proxy qualification drain returned {returncode}, expected 143")
        terminal, shutdown_report = lifecycle._terminal_record(
            run_dir, "STOPPED", require_graceful_deployment=True
        )
        return {
            "schema_version": 1,
            "passed": True,
            "generation": generation,
            "deployment_plan_hash": plan.deployment_plan_hash,
            "advertised_endpoint": endpoint,
            "ready_revision": status.revision,
            "ready_evidence": ready_evidence,
            "canary": canary,
            "client_workload": workload,
            "gateway_diagnostics": diagnostics,
            "terminal_revision": terminal.revision,
            "terminal_state": terminal.state,
            "terminal_reason_code": terminal.reason_code,
            "returncode": returncode,
            "shutdown_report": shutdown_report,
            "duration_s": round(time.time() - started, 3),
        }
    finally:
        active_error = sys.exc_info()[1]
        cleanup_errors: list[BaseException] = []
        cleanup_deadline = time.monotonic() + float(plan.control.watchdog_cleanup_deadline_s) + 60.0
        if process.poll() is None:
            try:
                process.send_signal(signal.SIGTERM)
                process.wait(timeout=max(0.0, cleanup_deadline - time.monotonic()))
            except BaseException as exc:
                cleanup_errors.append(exc)
        if lifecycle._process_group_exists(process.pid):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            reports = lifecycle._cleanup_generation_on_nodes(
                nodes,
                deployment_id=plan.deployment_id,
                generation=generation,
                plan_hash=plan.deployment_plan_hash,
                run_dir=run_dir,
            )
            lifecycle._atomic_json(
                output / "exact_generation_cleanup.json",
                {"schema_version": 1, "reports": reports},
            )
            leftovers = [item for report in reports for item in report["matched"]]
            if leftovers:
                cleanup_errors.append(
                    RuntimeError(f"owner left exact generation processes for fallback: {leftovers}")
                )
        except BaseException as exc:
            cleanup_errors.append(exc)
        for tee in (stdout, stderr):
            try:
                tee.join(deadline=cleanup_deadline)
            except BaseException as exc:
                cleanup_errors.append(exc)
        if cleanup_errors:
            if active_error is not None:
                for error in cleanup_errors:
                    lifecycle._add_note(
                        active_error, f"proxy qualification cleanup also failed: {error}"
                    )
            else:
                raise cleanup_errors[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-plan", required=True)
    parser.add_argument("--gate-id", required=True)
    args = parser.parse_args()
    gate_id = args.gate_id.strip()
    if not gate_id or any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in gate_id):
        parser.error("--gate-id must contain uppercase ASCII letters, digits, '-' and '_'")
    experiment_path = Path(args.experiment_plan).resolve()
    try:
        document, gate, paths = _load_gate(experiment_path, gate_id)
    except RuntimeError as exc:
        parser.error(str(exc))
    output = paths["output"]
    try:
        output.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        parser.error(f"output must be new and immutable: {output}")

    bootstrap = paths["bootstrap"]
    if str(bootstrap) not in [entry for entry in sys.path if entry]:
        raise SystemExit(f"PYTHONPATH must include immutable bootstrap {bootstrap}")
    lifecycle._pin_bootstrap_environment(bootstrap)
    started = time.time()
    result: dict | None = None
    try:
        from exaserve.plan.compiler import compile_deployment_plan
        from exaserve.plan.io import load_deployment_plan, load_site_profile
        from exaserve.yaml_support import load_yaml_mapping

        plan = load_deployment_plan(str(paths["deployment_plan"]))
        profile = load_site_profile(str(paths["site_profile"]))
        candidate = document["candidate"]
        rebuilt_plan = compile_deployment_plan(
            load_yaml_mapping(paths["config"]),
            site=profile,
            deployment_id=gate_id.lower(),
            compatibility_profile_hash=candidate["compatibility_profile_hash"],
            manifest_hash=candidate["compatibility_manifest_hash"],
        )
        if rebuilt_plan.deployment_plan_hash != plan.deployment_plan_hash:
            raise RuntimeError(
                "compiled deployment plan is not the exact result of the declared config, "
                "site profile, compatibility identity, and gate ID"
            )
        gateway_options = dict(plan.gateway.options) if plan.gateway is not None else {}
        if (
            plan.deployment_id != gate_id.lower()
            or plan.num_nodes != 1
            or plan.runtime.null_compute
            or plan.gateway is None
            or plan.gateway.kind != "haproxy"
            or gateway_options.get("http_no_delay") is not gate["http_no_delay"]
            or plan.site_profile_hash != profile.site_profile_hash
            or profile.site_profile_hash != candidate["site_profile_hash"]
            or plan.compatibility_profile_hash != candidate["compatibility_profile_hash"]
            or plan.manifest_hash != candidate["compatibility_manifest_hash"]
        ):
            raise RuntimeError("compiled plan does not match the declared proxy arm/candidate")
        nodes = lifecycle._validated_nodes(1, acquisition_source=gate["acquisition_source"])
        environment = lifecycle._environment_receipt(
            nodes, paths["bootstrap"], paths["wheel"], gate_id=gate_id
        )
        if environment["pbs_queue"] != gate["queue"]:
            raise RuntimeError(
                f"PBS queue {environment['pbs_queue']!r} differs from {gate['queue']!r}"
            )
        lifecycle._atomic_json(output / "environment.json", environment)
        manifest = {
            "schema_version": 1,
            "gate_id": gate_id,
            "lane": gate["lane"],
            "experiment_plan_path": str(experiment_path),
            "experiment_plan_sha256": lifecycle._sha256_file(experiment_path),
            "declared_gate": gate,
            "candidate": candidate,
            "harness": str(Path(__file__).resolve()),
            "harness_sha256": lifecycle._sha256_file(Path(__file__).resolve()),
            "support": str(Path(lifecycle.__file__).resolve()),
            "support_sha256": lifecycle._sha256_file(Path(lifecycle.__file__).resolve()),
            "pbs_job_id": environment["pbs_job_id"],
            "queue": environment["pbs_queue"],
            "nodes": list(nodes),
            "started_at": started,
            "attempt": gate["attempt"],
            "attempt_limit": gate["attempt_limit"],
            "http_no_delay": gate["http_no_delay"],
            "deployment_plan_path": str(paths["deployment_plan"]),
            "deployment_plan_hash": plan.deployment_plan_hash,
            "site_profile_path": str(paths["site_profile"]),
            "site_profile_hash": profile.site_profile_hash,
            "wheel": str(paths["wheel"]),
            "wheel_sha256": environment["wheel_sha256"],
        }
        lifecycle._atomic_json(output / "manifest.json", manifest)
        execution = _launch(
            output=output,
            plan_path=paths["deployment_plan"],
            site_path=paths["site_profile"],
            gate=gate,
            nodes=nodes,
        )
        result = {
            **manifest,
            "passed": True,
            "completed_at": time.time(),
            "duration_s": round(time.time() - started, 3),
            "execution": execution,
        }
        lifecycle._atomic_json(output / "result.json", result)
        lifecycle._atomic_text(
            output / "verdict.md",
            f"# {gate_id} verdict\n\nVerdict: **PASS**\n\n"
            f"- http-no-delay: `{gate['http_no_delay']}`\n"
            f"- completed bounded requests: `{execution['client_workload']['completed']}`\n"
            f"- TCP retransmits: `{execution['gateway_diagnostics']['retransmits_delta']}`\n"
            f"- peak connections: `{execution['gateway_diagnostics']['peak_connections']}`\n"
            f"- HAProxy CPU: `{execution['gateway_diagnostics']['cpu_percent']:.3f}%`\n",
        )
        print(f"QUALIFICATION_VERDICT|gate={gate_id}|PASS", flush=True)
        return 0
    except BaseException as exc:
        error = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        result = {
            "schema_version": 1,
            "gate_id": gate_id,
            "lane": gate["lane"],
            "experiment_plan_path": str(experiment_path),
            "experiment_plan_sha256": lifecycle._sha256_file(experiment_path),
            "declared_gate": gate,
            "attempt": gate["attempt"],
            "attempt_limit": gate["attempt_limit"],
            "passed": False,
            "completed_at": time.time(),
            "duration_s": round(time.time() - started, 3),
            "error": error,
        }
        lifecycle._atomic_json(output / "result.json", result)
        lifecycle._atomic_text(
            output / "verdict.md",
            f"# {gate_id} verdict\n\nVerdict: **FAIL**\n\n```text\n{error.rstrip()}\n```\n",
        )
        print(f"QUALIFICATION_VERDICT|gate={gate_id}|FAIL", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
