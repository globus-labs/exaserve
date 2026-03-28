import argparse
import json
import math
import random
import socket
import struct
import threading
import time
from collections import deque
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn
from typing import Any


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def load_config(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    idx = min(int((len(ordered) - 1) * fraction), len(ordered) - 1)
    return ordered[idx]


class SyntheticState:
    def __init__(self, config):
        self.config = config
        self.lock = threading.Lock()
        self.total_requests = 0
        self.accepted = 0
        self.completed = 0
        self.rejections = 0
        self.errors = 0
        self.active = 0
        self.waiting = 0
        self.max_active = 0
        self.max_queue_depth = 0
        self.request_timestamps = deque(maxlen=50_000)
        self.latency_samples = deque(maxlen=50_000)
        self.queue_wait_samples = deque(maxlen=50_000)
        self.error_status = int(config["faults"].get("error_status", 500))
        self.reject_status = int(config["faults"].get("reject_status", 429))
        max_inflight = int(config["faults"].get("max_inflight", 0))
        max_queue = int(config["faults"].get("max_queue", 0))
        self.service_slots = threading.Semaphore(max_inflight) if max_inflight > 0 else None
        total_capacity = max_inflight + max_queue if max_inflight > 0 else 0
        self.capacity_slots = threading.Semaphore(total_capacity) if total_capacity > 0 else None
        self.rng = random.Random(42)
        self.chat_response = json.dumps(self._make_chat_completion()).encode("utf-8")
        self.completion_response = json.dumps(self._make_completion()).encode("utf-8")

    def _build_response_text(self, token_count: int) -> str:
        return " ".join(["token"] * max(1, token_count))

    def _make_chat_completion(self):
        response_tokens = int(self.config["target"].get("response_tokens", 16))
        model = str(self.config["client"].get("model", "stub-model"))
        return {
            "id": "chatcmpl-clientlab",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": self._build_response_text(response_tokens)},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": int(self.config["client"].get("prompt_words", 32)),
                "completion_tokens": response_tokens,
                "total_tokens": int(self.config["client"].get("prompt_words", 32)) + response_tokens,
            },
        }

    def _make_completion(self):
        response_tokens = int(self.config["target"].get("response_tokens", 16))
        model = str(self.config["client"].get("model", "stub-model"))
        return {
            "id": "cmpl-clientlab",
            "object": "text_completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "text": self._build_response_text(response_tokens),
                    "index": 0,
                    "finish_reason": "stop",
                    "logprobs": None,
                }
            ],
            "usage": {
                "prompt_tokens": int(self.config["client"].get("prompt_words", 32)),
                "completion_tokens": response_tokens,
                "total_tokens": int(self.config["client"].get("prompt_words", 32)) + response_tokens,
            },
        }

    def service_delay_s(self) -> float:
        service_cfg = self.config["faults"].get("service_time", {})
        dist = str(service_cfg.get("distribution", "fixed"))
        value_ms = float(service_cfg.get("value_ms", 0.0))
        stddev_ms = float(service_cfg.get("stddev_ms", 0.0))
        if dist == "fixed":
            return max(value_ms, 0.0) / 1000.0
        if dist == "normal":
            return max(self.rng.gauss(value_ms, stddev_ms), 0.0) / 1000.0
        if dist == "lognormal":
            sigma = max(stddev_ms / 1000.0, 0.01)
            mean = max(value_ms / 1000.0, 1e-6)
            mu = math.log(mean) - 0.5 * sigma * sigma
            return max(self.rng.lognormvariate(mu, sigma), 0.0)
        return max(value_ms, 0.0) / 1000.0

    def should_inject_error(self) -> bool:
        error_rate = float(self.config["faults"].get("error_rate", 0.0))
        if error_rate > 0 and self.rng.random() < error_rate:
            return True
        burst_every = int(self.config["faults"].get("burst_every", 0))
        burst_duration = int(self.config["faults"].get("burst_duration", 0))
        if burst_every > 0 and burst_duration > 0:
            with self.lock:
                req_index = self.total_requests
            phase = req_index % burst_every
            return phase < burst_duration
        return False

    def record_request_start(self) -> bool:
        with self.lock:
            self.total_requests += 1
        if self.capacity_slots is not None and not self.capacity_slots.acquire(blocking=False):
            with self.lock:
                self.rejections += 1
            return False
        with self.lock:
            self.accepted += 1
            self.waiting += 1
            self.max_queue_depth = max(self.max_queue_depth, self.waiting)
        return True

    def acquire_service(self) -> float:
        queue_started = time.perf_counter()
        if self.service_slots is not None:
            self.service_slots.acquire()
        queue_wait = time.perf_counter() - queue_started
        with self.lock:
            self.waiting = max(self.waiting - 1, 0)
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.queue_wait_samples.append(queue_wait)
        return queue_wait

    def release_service(self, latency_s: float, *, error: bool) -> None:
        with self.lock:
            self.active = max(self.active - 1, 0)
            self.completed += 1
            self.request_timestamps.append(time.time())
            self.latency_samples.append(latency_s)
            if error:
                self.errors += 1
        if self.service_slots is not None:
            self.service_slots.release()
        if self.capacity_slots is not None:
            self.capacity_slots.release()

    def metrics(self):
        with self.lock:
            latencies = list(self.latency_samples)
            queue_waits = list(self.queue_wait_samples)
            timestamps = list(self.request_timestamps)
            total_requests = self.total_requests
            accepted = self.accepted
            completed = self.completed
            rejections = self.rejections
            errors = self.errors
            max_active = self.max_active
            max_queue_depth = self.max_queue_depth

        now = time.time()
        recent_rps = sum(1 for ts in timestamps if now - ts <= 1.0)
        return {
            "total_requests": total_requests,
            "accepted": accepted,
            "completed": completed,
            "rejections": rejections,
            "errors": errors,
            "error_fraction": (errors / completed) if completed else 0.0,
            "rps_last_1s": recent_rps,
            "max_active": max_active,
            "max_queue_depth": max_queue_depth,
            "latency_p50_s": percentile(latencies, 0.50),
            "latency_p99_s": percentile(latencies, 0.99),
            "queue_wait_p50_s": percentile(queue_waits, 0.50),
            "queue_wait_p99_s": percentile(queue_waits, 0.99),
        }


class SyntheticHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    state: SyntheticState

    def do_GET(self) -> None:  # noqa: N802
        if self.path in {"/health", "/health/liveliness"}:
            self._write_json({"status": "ok"})
            return
        if self.path == "/v1/models":
            self._write_json({"object": "list", "data": [{"id": self.state.config["client"]["model"], "object": "model"}]})
            return
        if self.path == "/metrics":
            self._write_json(self.state.metrics())
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        if self.path not in {"/v1/chat/completions", "/v1/completions"}:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length > 0:
            _ = self.rfile.read(content_length)
        if not self.state.record_request_start():
            self._write_json({"error": "capacity rejected"}, status=self.state.reject_status)
            return

        start = time.perf_counter()
        self.state.acquire_service()
        injected_error = self.state.should_inject_error()
        queue_delay_ms = float(self.state.config["faults"].get("queue_delay_ms", 0.0))
        if queue_delay_ms > 0:
            time.sleep(queue_delay_ms / 1000.0)
        time.sleep(self.state.service_delay_s())

        if injected_error:
            payload = {"error": "injected"}
            status = self.state.error_status
        elif self.path == "/v1/chat/completions":
            payload = json.loads(self.state.chat_response.decode("utf-8"))
            status = HTTPStatus.OK
        else:
            payload = json.loads(self.state.completion_response.decode("utf-8"))
            status = HTTPStatus.OK

        self._write_json(payload, status=status)
        latency_s = time.perf_counter() - start
        self.state.release_service(latency_s, error=injected_error or status >= 400)
        if self.state.config["faults"].get("reset_after_response"):
            self._force_reset()

    def log_message(self, format, *args):  # noqa: A003
        return

    def _write_json(self, payload, status=200):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        if self.state.config["faults"].get("close_after_response"):
            self.send_header("Connection", "close")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        self.wfile.flush()
        if self.state.config["faults"].get("close_after_response"):
            self.close_connection = True

    def _force_reset(self) -> None:
        try:
            linger = struct.pack("ii", 1, 0)
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger)
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
        except OSError:
            pass


def build_server(config):
    state = SyntheticState(config)

    class BoundSyntheticHandler(SyntheticHandler):
        pass

    BoundSyntheticHandler.state = state
    server = ThreadingHTTPServer((config["target"]["host"], int(config["target"]["port"])), BoundSyntheticHandler)
    if float(config["faults"].get("idle_timeout_s", 0.0)) > 0:
        server.timeout = float(config["faults"]["idle_timeout_s"])
    return server


def main(argv=None):
    parser = argparse.ArgumentParser(description="ClientLab synthetic target server")
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)

    config = load_config(args.config)
    server = build_server(config)
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
