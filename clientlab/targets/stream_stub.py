#!/usr/bin/env python3
"""Minimal streaming stub server for TTFT validation.

Returns SSE-formatted responses with configurable TTFT delay.
Supports both streaming and non-streaming modes based on request body.
"""
import argparse
import json
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn


class StreamHandler(BaseHTTPRequestHandler):
    ttft_delay = 0.05   # seconds to first token
    token_delay = 0.01  # seconds between tokens
    num_tokens = 16

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
            return
        self.send_error(404)

    def do_POST(self):
        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len)
        try:
            req = json.loads(body) if body else {}
        except json.JSONDecodeError:
            req = {}

        model = req.get("model", "stub-model")
        max_tokens = req.get("max_tokens", self.num_tokens)
        stream = req.get("stream", False)
        prompt_tokens = 32  # approximate

        if stream:
            self._handle_stream(model, prompt_tokens, max_tokens)
        else:
            self._handle_non_stream(model, prompt_tokens, max_tokens)

    def _handle_stream(self, model, prompt_tokens, max_tokens):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()

        # TTFT delay: simulate time to first token
        time.sleep(self.ttft_delay)

        for i in range(max_tokens):
            chunk = {
                "choices": [{"delta": {"content": "tok "}, "index": 0}],
            }
            # Include usage in the last chunk
            if i == max_tokens - 1:
                chunk["usage"] = {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": max_tokens,
                }
            self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
            self.wfile.flush()
            if i < max_tokens - 1:
                time.sleep(self.token_delay)

        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

    def _handle_non_stream(self, model, prompt_tokens, max_tokens):
        # Simulate full generation time
        time.sleep(self.ttft_delay + self.token_delay * max_tokens)

        resp = {
            "choices": [{"message": {"content": "tok " * max_tokens}}],
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": max_tokens,
            },
        }
        body = json.dumps(resp).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # suppress request logging


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=18800)
    parser.add_argument("--ttft-delay", type=float, default=0.05, help="Seconds to first token")
    parser.add_argument("--token-delay", type=float, default=0.01, help="Seconds between tokens")
    parser.add_argument("--num-tokens", type=int, default=16, help="Default output tokens")
    args = parser.parse_args()

    StreamHandler.ttft_delay = args.ttft_delay
    StreamHandler.token_delay = args.token_delay
    StreamHandler.num_tokens = args.num_tokens

    class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
        daemon_threads = True

    server = ThreadedHTTPServer(("0.0.0.0", args.port), StreamHandler)
    print(f"Streaming stub server on port {args.port} (ttft={args.ttft_delay}s, token_delay={args.token_delay}s)", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
