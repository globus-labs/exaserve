"""
Client for Aurora Ray LLM Server.

Usage:
  # Normal chat
  python src/client.py --prompt "Hello! How are you?"

  # Benchmark: fixed 128 output tokens
  python src/client.py --prompt "Hello!" --output-len 128

  # Benchmark: fixed input + output, with timing
  python src/client.py --prompt "Explain quantum computing" --output-len 256 --benchmark
"""

import argparse
import time
from openai import OpenAI

MODEL = "meta-llama/Meta-Llama-3-8B-Instruct"
BASE_URL = "http://localhost:8000/v1"


def chat(client: OpenAI, prompt: str, output_len: int | None = None) -> dict:
    """Send a chat completion request.

    Args:
        client: OpenAI client instance.
        prompt: User message.
        output_len: If set, force the model to generate exactly this many tokens
                    by setting min_tokens = max_tokens = output_len.

    Returns:
        dict with response text and usage stats.
    """
    extra_body = {}
    extra_kwargs = {}

    if output_len is not None:
        # max_tokens caps generation; min_tokens (via extra_body) prevents
        # early stopping on EOS so the model produces exactly output_len tokens.
        extra_kwargs["max_tokens"] = output_len
        extra_body["min_tokens"] = output_len
        # Ignore EOS so the model doesn't stop before reaching min_tokens.
        extra_body["ignore_eos"] = True

    t0 = time.perf_counter()
    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        extra_body=extra_body if extra_body else None,
        **extra_kwargs,
    )
    elapsed = time.perf_counter() - t0

    usage = response.usage
    return {
        "text": response.choices[0].message.content,
        "elapsed_s": elapsed,
        "prompt_tokens": usage.prompt_tokens if usage else None,
        "completion_tokens": usage.completion_tokens if usage else None,
        "tokens_per_sec": (
            usage.completion_tokens / elapsed if usage and elapsed > 0 else None
        ),
    }


def main():
    parser = argparse.ArgumentParser(description="Chat / benchmark client")
    parser.add_argument("--prompt", default="Hello! How are you?", help="User prompt")
    parser.add_argument(
        "--output-len",
        type=int,
        default=None,
        help="Force fixed output length (tokens). Omit for normal chat.",
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Print timing and throughput stats.",
    )
    parser.add_argument("--base-url", default=BASE_URL, help="Server base URL")
    args = parser.parse_args()

    client = OpenAI(base_url=args.base_url, api_key="fake-key")
    result = chat(client, args.prompt, args.output_len)

    # Always print the response
    print(result["text"])

    if args.benchmark or args.output_len is not None:
        print(f"\n--- stats ---")
        print(f"Prompt tokens:     {result['prompt_tokens']}")
        print(f"Completion tokens: {result['completion_tokens']}")
        print(f"Wall time:         {result['elapsed_s']:.3f} s")
        if result["tokens_per_sec"] is not None:
            print(f"Throughput:        {result['tokens_per_sec']:.1f} tok/s")


if __name__ == "__main__":
    main()
