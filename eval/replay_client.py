import argparse
import asyncio
import glob
import json
import os
import re
import resource
import time
import uuid
import gc
import signal
from dataclasses import dataclass

import aiohttp
import numpy as np
import yaml

@dataclass
class TraceRequest:
    timestamp: float
    model: str
    prompt: str
    input_len: int
    output_len: int
    tensor_parallel_size: int
    req_id: str
    mode: str = "chat"  # from trace file modes distribution, or config default

TIMEOUT_S = 3600 # 1 hour
REQ_BATCH_SIZE = 5
EXTRA_RATE = 0.2

_RESULT_PATTERN = re.compile(r"result(\d+)\.json$")


def _next_result_path(result_dir: str) -> str:
    """Return path for next result file: result0.json, result1.json, ...
    Scans result_dir for existing resultN.json and uses next available index.
    """
    os.makedirs(result_dir, exist_ok=True)
    candidates = glob.glob(os.path.join(result_dir, "result*.json"))
    indices = []
    for p in candidates:
        m = _RESULT_PATTERN.search(os.path.basename(p))
        if m:
            indices.append(int(m.group(1)))
    next_idx = max(indices) + 1 if indices else 0
    return os.path.join(result_dir, f"result{next_idx}.json")


async def send_request(session, base_url, req, mode_map, include_tp: bool, generation_mode: str):
    # Per-request mode from trace file
    mode = getattr(req, 'mode', None) 
    url = f"{base_url}/v1/chat/completions" if mode == "chat" else f"{base_url}/v1/completions"
    
    # Construct Payload based on generation_mode
    if generation_mode == "deterministic":
        # Deterministic mode: force exact output length
        if mode == "chat":
            payload = {
                "model": req.model,
                "messages": [{"role": "user", "content": req.prompt}],
                "max_tokens": req.output_len,
                "min_tokens": req.output_len,
                "temperature": 0.7,
                "ignore_eos": True
            }
        else:
            payload = {
                "model": req.model,
                "prompt": req.prompt,
                "max_tokens": req.output_len,
                "min_tokens": req.output_len,
                "temperature": 0.7,
                "ignore_eos": True
            }
    else:
        # Natural mode: allow natural generation with EOS
        if mode == "chat":
            payload = {
                "model": req.model,
                "messages": [{"role": "user", "content": req.prompt}],
                "max_tokens": req.output_len,
                "temperature": 0.7,
            }
        else:
            payload = {
                "model": req.model,
                "prompt": req.prompt,
                "max_tokens": req.output_len,
                "temperature": 0.7,
            }

    if include_tp:
        payload["tensor_parallel_size"] = req.tensor_parallel_size

    start = time.time()
    success = False
    error_msg = ""
    response_data = None
    
    try:
        # High timeout because queued requests in Ray/MPI might take time
        async with session.post(url, json=payload, timeout=TIMEOUT_S) as resp:
            success = (resp.status == 200)
            if success:
                # Parse response body to extract usage information
                response_data = await resp.json()
            else:
                error_msg = f"HTTP {resp.status}"
                await resp.read()  # Consume body even on error
    except Exception as e:
        error_msg = str(e)
    
    end_time = time.time()
    latency = end_time - start
    
    # Extract token counts from response usage field (if available)
    actual_prompt_tokens = None
    actual_completion_tokens = None
    if response_data and "usage" in response_data:
        usage = response_data["usage"]
        actual_prompt_tokens = usage.get("prompt_tokens")
        actual_completion_tokens = usage.get("completion_tokens")
    
    return req, latency, success, error_msg, end_time, actual_prompt_tokens, actual_completion_tokens

def _config_trace_path(cfg: dict) -> str:
    """Trace path from ExpConfig-shaped config: job_trace_config.output_trace_path."""
    jtc = cfg.get('job_trace_config') or {}
    return jtc.get('output_trace_path', 'experiment_trace.jsonl')

def _config_result_dir(cfg: dict):
    """Result directory from ExpConfig-shaped config: pbs_result_dir."""
    return cfg.get('pbs_result_dir')

def _config_mode_map(cfg: dict) -> dict:
    """Build model_id -> mode from ExpConfig-shaped config (model_deployment_config.model_configs)."""
    dep = cfg.get('model_deployment_config') or {}
    model_configs = dep.get('model_configs') or []
    return {m.get('model_id', ''): m.get('mode', 'chat') for m in model_configs if m.get('model_id')}

def _config_gpu_topology(cfg: dict) -> tuple:
    """(num_nodes, num_gpus_per_node) from ExpConfig-shaped config."""
    dep = cfg.get('model_deployment_config') or {}
    return (dep.get('num_nodes', 1), dep.get('num_gpus_per_node', 4))

async def replay(config_path, include_tp: bool, early_stop: float, no_warmup: bool, num_runs: int):
    # 1. Load Config (ExpConfig-shaped YAML)
    with open(config_path, 'r') as f:
        cfg = yaml.safe_load(f) or {}
    trace_path = _config_trace_path(cfg)
    port = cfg.get('port', 8000)
    base_url = f"http://localhost:{port}"
    
    # Get generation mode from config
    replay_cfg = cfg.get('job_replay_client_config', {})
    generation_mode = replay_cfg.get('generation_mode', 'deterministic')
    
    # Validate generation mode
    if generation_mode not in ['deterministic', 'natural']:
        print(f"!!! WARNING: Invalid generation_mode '{generation_mode}', defaulting to 'deterministic'")
        generation_mode = 'deterministic'

    # 2. Pre-Load Requests (Blocking Phase)
    print(f">>> [REPLAY] Loading trace from {trace_path}...")
    requests = []
    try:
        with open(trace_path, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                
                # [NEW] Skip metadata header
                if data.get("__type__") == "metadata":
                    print(f">>> [REPLAY] Found trace metadata (generated {data.get('timestamp')})")
                    continue
                    
                requests.append(TraceRequest(
                    timestamp=data['timestamp'],
                    model=data['model'],
                    prompt=data['prompt'],
                    input_len=data.get('input_len', 0),
                    output_len=data['output_len'],
                    tensor_parallel_size=data.get('tensor_parallel_size', 1),
                    req_id=uuid.uuid4().hex,
                    mode=data.get('mode', 'chat'),
                ))
    except FileNotFoundError:
        print(f"!!! ERROR: Trace file {trace_path} not found. Run trace_generator.py first.")
        return

    # 3. Mode mapping (model_id -> mode; per-request mode from trace overrides when set)
    mode_map = _config_mode_map(cfg)

    # 4. Early stop calculation
    target_responses = None
    if early_stop > 0:
        target_responses = int(len(requests) * early_stop)
        print(f">>> [EARLY STOP] Will stop after {target_responses}/{len(requests)} responses received ({early_stop*100:.1f}%)")

    print(f">>> [REPLAY] Target: {base_url}")
    print(f"    Requests: {len(requests)}")
    print(f"    Duration: {requests[-1].timestamp:.2f}s")
    print(f">>> [REPLAY] Generation Mode: {generation_mode.upper()}")
    if generation_mode == "deterministic":
        print(f"    Using min_tokens=max_tokens={requests[0].output_len if requests else 'N/A'} with ignore_eos=True")
        print(f"    This ensures deterministic compute load matching trace output_len.")
    else:
        print(f"    Using natural generation with EOS termination.")
        print(f"    Output lengths may vary from trace-specified output_len.")
    print(">>> [REPLAY] Disabling Garbage Collection for precision...")
    
    # 5. Setup interrupt handler
    interrupted = False
    def signal_handler(sig, frame):
        nonlocal interrupted
        interrupted = True
    
    # Install signal handler for Ctrl-C
    old_handler = signal.signal(signal.SIGINT, signal_handler)
    
    # 6. The Loop
    tasks = []
    gc.disable() # Critical for reducing jitter
    t0 = time.time()
    
    results = []
    all_runs_results = []
    run_durations = []
    warmup_duration_s = 0.0
    session = None
    
    try:
        # Dynamic Resource Management:
        # 1. Get current limits
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        print(f">>> [SYSTEM] Current open file limit: soft={soft}, hard={hard}")
        
        # 2. Try to raise soft limit to hard limit
        if soft < hard:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
                soft = hard
                print(f">>> [SYSTEM] Increased soft limit to {soft}")
            except Exception as e:
                print(f">>> [SYSTEM] Failed to increase file limit: {e}")

        # 3. Calculate safe max connections (reserve buffer for system/other files)
        # Reserve ~512 FDs for overhead (libraries, stdout, etc.)
        safe_conn_limit = max(100, soft - 512)
        print(f">>> [CONFIG] Setting dynamic max concurrent connections to {safe_conn_limit}")

        # Use TCPConnector with the calculated safe limit.
        # This saturates the OS resources without crashing (queues excess requests).
        connector = aiohttp.TCPConnector(limit=safe_conn_limit)
        session = aiohttp.ClientSession(connector=connector)
        
        # Track inflight requests to detect client-side queuing
        inflight_stats = {'count': 0}
        def task_done_callback(future):
            inflight_stats['count'] -= 1

        # ==============================================================================
        # WARMUP PHASE
        # ==============================================================================
        if not no_warmup:
            num_nodes, num_gpus_per_node = _config_gpu_topology(cfg)
            
            warmup_count = int(num_nodes * num_gpus_per_node * REQ_BATCH_SIZE * (1 + EXTRA_RATE))
            print(f"\n>>> [WARMUP] Starting warmup phase...")
            print(f"    Nodes: {num_nodes}, GPUs/Node: {num_gpus_per_node}")
            print(f"    Batch Size: {REQ_BATCH_SIZE}, Extra Rate: {EXTRA_RATE}")
            print(f"    Total Warmup Requests: {warmup_count}")
            
            if requests:
                # Reuse the first request template for warmup
                base_req = requests[0]
                warmup_tasks = []
                warmup_start = time.time()
                
                print(f"    Firing {warmup_count} requests concurrently...")
                for _ in range(warmup_count):
                    # Create a copy with new ID
                    w_req = TraceRequest(
                        timestamp=0, # Immediate
                        model=base_req.model,
                        prompt=base_req.prompt,
                        input_len=base_req.input_len,
                        output_len=base_req.output_len,
                        tensor_parallel_size=base_req.tensor_parallel_size,
                        req_id=uuid.uuid4().hex
                    )
                    task = asyncio.create_task(send_request(session, base_url, w_req, mode_map, include_tp, generation_mode))
                    warmup_tasks.append(task)
                
                # Wait for all warmup requests to finish
                done, _ = await asyncio.wait(warmup_tasks, return_when=asyncio.ALL_COMPLETED)
                
                # Check success rate
                success_count = sum(1 for t in done if t.result()[2])
                warmup_dur = time.time() - warmup_start
                warmup_duration_s = warmup_dur
                print(f">>> [WARMUP] Completed in {warmup_dur:.2f}s. Success: {success_count}/{warmup_count}")
                print(f">>> [WARMUP] Resting for 5s before main trace...")
                await asyncio.sleep(5)
            else:
                print(">>> [WARMUP] No requests loaded to use for warmup. Skipping.")

        # ==============================================================================
        # MAIN EXPERIMENT LOOP (Multiple Runs)
        # ==============================================================================
        all_runs_results = []
        run_durations = []

        for run_idx in range(num_runs):
            if interrupted:
                break

            print("\n" + "=" * 70)
            print(f">>> [RUN {run_idx + 1}/{num_runs}] Starting main experiment replay...")
            print("=" * 70)

            run_t0 = time.time()
            tasks = []
            inflight_stats["count"] = 0

            # Fire all requests according to schedule
            for i, req in enumerate(requests):
                # Check for interrupt
                if interrupted:
                    print(f"\n>>> [INTERRUPTED] Stopping at request {i}/{len(requests)}")
                    break

                # Precision Sleep
                target_time = run_t0 + req.timestamp
                now = time.time()
                if target_time > now:
                    await asyncio.sleep(target_time - now)

                # Fire
                inflight_stats["count"] += 1
                task = asyncio.create_task(
                    send_request(session, base_url, req, mode_map, include_tp, generation_mode)
                )
                task.add_done_callback(task_done_callback)
                tasks.append(task)

                if i % 50 == 0:
                    print(f"\r[RUNNING] Fired {i+1}/{len(requests)}", end="", flush=True)

                    # Check for client-side bottleneck
                    if inflight_stats["count"] >= safe_conn_limit:
                        queued = inflight_stats["count"] - safe_conn_limit
                        print(
                            f"\n>>> [WARNING] Client connection limit hit! Active: "
                            f"{safe_conn_limit}, Queued: {queued}"
                        )

            # Print final count if not already printed
            if not interrupted:
                print(f"\r[RUNNING] Fired {len(requests)}/{len(requests)}")
            print(">>> [FINISH] All requests fired. Waiting for responses...")

            # Wait for responses with early stop support
            if target_responses is not None:
                # Early stop mode: poll until we have enough responses
                completed_results = []
                pending = set(tasks)

                while pending and not interrupted:
                    done, pending = await asyncio.wait(
                        pending,
                        timeout=0.5,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    for task in done:
                        try:
                            res = task.result()
                            completed_results.append(res)
                        except Exception:
                            pass

                    # Check if we've reached the target
                    if len(completed_results) >= target_responses:
                        print(
                            f"\n>>> [EARLY STOP] Target reached "
                            f"({len(completed_results)}/{target_responses}). "
                            "Cancelling remaining tasks..."
                        )
                        for task in pending:
                            task.cancel()
                        break

                    if len(completed_results) % 50 == 0 and len(completed_results) > 0:
                        print(
                            f"\r[WAITING] Received {len(completed_results)}/"
                            f"{target_responses} responses...",
                            end="",
                            flush=True,
                        )

                # Handle interrupt
                if interrupted:
                    print(
                        "\n>>> [INTERRUPTED] Received Ctrl-C. "
                        "Cancelling remaining tasks..."
                    )
                    for task in pending:
                        task.cancel()

                # Wait for any cancellations to complete
                if pending:
                    await asyncio.wait(
                        pending, return_when=asyncio.ALL_COMPLETED
                    )

                run_results = completed_results
            else:
                # Normal mode: wait for all with progress updates
                completed_results = []
                pending = set(tasks)
                total_tasks = len(tasks)

                while pending and not interrupted:
                    done, pending = await asyncio.wait(
                        pending,
                        timeout=0.5,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    for task in done:
                        try:
                            res = task.result()
                            completed_results.append(res)
                        except Exception:
                            pass

                    # Show progress
                    if len(completed_results) % 50 == 0 and len(completed_results) > 0:
                        print(
                            f"\r[WAITING] Received {len(completed_results)}/"
                            f"{total_tasks} responses...",
                            end="",
                            flush=True,
                        )

                # Handle interrupt
                if interrupted:
                    print(
                        "\n>>> [INTERRUPTED] Received Ctrl-C. "
                        "Cancelling remaining tasks..."
                    )
                    for task in pending:
                        task.cancel()
                    # Wait briefly for cancellations
                    if pending:
                        await asyncio.wait(
                            pending,
                            timeout=1.0,
                            return_when=asyncio.ALL_COMPLETED,
                        )
                else:
                    print(
                        f"\r[WAITING] Received {len(completed_results)}/"
                        f"{total_tasks} responses..."
                    )

                run_results = completed_results

            all_runs_results.append(run_results)

            # Track run duration using response end timestamps when possible
            if run_results:
                run_end_time = max((r[4] for r in run_results), default=None)
                if run_end_time is not None:
                    run_durations.append(run_end_time - run_t0)
                else:
                    run_durations.append(max(time.time() - run_t0, 0.0))
            else:
                run_durations.append(max(time.time() - run_t0, 0.0))

            if interrupted:
                break

            if run_idx < num_runs - 1:
                print(
                    f">>> [RUN {run_idx + 1}] Completed. "
                    "Resting for 5s before next run..."
                )
                await asyncio.sleep(5)

        results = all_runs_results[-1] if all_runs_results else []
    
    except Exception as e:
        print(f"\n\n!!! [ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        
        # Try to collect whatever results we have
        results_store = []
        for task in tasks:
            if task.done() and not task.cancelled():
                try:
                    res = task.result()
                    results_store.append(res)
                except Exception:
                    continue
        results = results_store
        if not all_runs_results and results_store:
            all_runs_results = [results_store]
    
    finally:
        # Restore signal handler
        signal.signal(signal.SIGINT, old_handler)
        
        # Cleanup
        gc.enable()
        if session and not session.closed:
            await session.close()
    
    # 6. Analysis & Save
    if interrupted:
        print(
            f"\n>>> [INTERRUPTED] Collected {len(results)} completed responses "
            f"out of {len(requests)} scheduled requests in last run."
        )

    print("\n" + "=" * 70)
    print(f"{'MODEL':<45} | {'CNT':<5} | {'P50 (s)':<8} | {'P99 (s)':<8} | {'ERR'}")
    print("-" * 70)

    model_stats = {}
    raw_results = []

    # Save all runs with run_index for later analysis
    for run_idx, run_results in enumerate(all_runs_results):
        for r in run_results:
            # r = (TraceRequest, latency, success, error_msg, end_time, actual_prompt_tokens, actual_completion_tokens)
            req_obj, latency, success, error_msg, end_time, actual_prompt_tokens, actual_completion_tokens = r
            raw_results.append({
                "run_index": run_idx,
                "model": req_obj.model,
                "latency": latency,
                "success": success,
                "error": error_msg,
                "input_len": req_obj.input_len,
                "output_len": req_obj.output_len,
                "actual_prompt_tokens": actual_prompt_tokens,
                "actual_completion_tokens": actual_completion_tokens,
                "tensor_parallel_size": req_obj.tensor_parallel_size,
                "req_id": req_obj.req_id
            })

    # Print summary stats for the last run only
    for r in results:
        req_obj, latency, success, error_msg, end_time, actual_prompt_tokens, actual_completion_tokens = r
        m_name = req_obj.model
        if m_name not in model_stats:
            model_stats[m_name] = []
        model_stats[m_name].append((req_obj, latency, success, error_msg, end_time, actual_prompt_tokens, actual_completion_tokens))

    for m_name, stats in model_stats.items():
        succ_lats = [x[1] for x in stats if x[2]]
        fails = len(stats) - len(succ_lats)

        if succ_lats:
            p50 = np.percentile(succ_lats, 50)
            p99 = np.percentile(succ_lats, 99)
            print(f"{m_name:<45} | {len(stats):<5} | {p50:.4f}   | {p99:.4f}   | {fails}")
        else:
            print(f"{m_name:<45} | {len(stats):<5} | N/A        | N/A        | {fails}")

    print("=" * 70)

    total_duration = run_durations[-1] if run_durations else max(time.time() - t0, 0.0)

    duration_for_rate = max(total_duration, 1e-6)
    
    # Calculate token statistics - prioritize actual usage data over trace specs
    # Count successful requests that have actual token counts vs those using trace specs
    actual_count = 0
    trace_count = 0
    
    total_input_tokens = 0
    total_output_tokens = 0
    
    for r in results:
        if r[2]:  # if success
            req_obj = r[0]
            actual_prompt_tokens = r[5]
            actual_completion_tokens = r[6]
            
            # Prioritize actual usage data from API response
            if actual_prompt_tokens is not None and actual_completion_tokens is not None:
                total_input_tokens += actual_prompt_tokens
                total_output_tokens += actual_completion_tokens
                actual_count += 1
            else:
                # Fallback to trace specifications
                total_input_tokens += req_obj.input_len
                total_output_tokens += req_obj.output_len
                trace_count += 1
    
    total_tokens = total_input_tokens + total_output_tokens
    
    completed_requests = len(results)
    scheduled_requests = len(requests)
    
    # Calculate rates
    rps = completed_requests / duration_for_rate
    tps = total_tokens / duration_for_rate
    processed_tps = total_input_tokens / duration_for_rate
    generated_tps = total_output_tokens / duration_for_rate
    
    token_source = f"(Usage API: {actual_count}, Trace Spec: {trace_count})" if completed_requests > 0 else ""

    print(f"System duration: {total_duration:.2f}s | RPS: {rps:.2f} | TPS: {tps:.2f} (Processed: {processed_tps:.2f}, Generated: {generated_tps:.2f}) | Completed: {completed_requests}/{scheduled_requests}")
    if token_source:
        print(f"Token counts from {token_source}")
    
    # Save Results
    config_result_dir = _config_result_dir(cfg)
    
    final_save_path = None
    if config_result_dir:
        final_save_path = _next_result_path(config_result_dir)
        
    if final_save_path:
        print(f">>> [REPLAY] Saving detailed results to {final_save_path}")
        try:
            with open(final_save_path, 'w') as f:
                json.dump({
                    "config": cfg,
                    "meta": {
                        "num_runs": num_runs,
                        "completed_runs": len(all_runs_results),
                        "warmup_duration_s": warmup_duration_s,
                        "generation_mode": generation_mode,
                        "token_counts_from_usage_api": actual_count,
                        "token_counts_from_trace_spec": trace_count
                    },
                    "summary": {m: len(s) for m, s in model_stats.items()},
                    "overall": {
                        "duration_s": total_duration,
                        "rps": rps,
                        "tps": tps,
                        "processed_tps": processed_tps,
                        "generated_tps": generated_tps,
                        "total_tokens": total_tokens,
                        "total_input_tokens": total_input_tokens,
                        "total_output_tokens": total_output_tokens,
                        "requests_completed": completed_requests,
                        "requests_scheduled": scheduled_requests
                    },
                    "requests": raw_results
                }, f, indent=2)
        except Exception as e:
            print(f"!!! ERROR Saving results: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--include-tp",
        action="store_true",
        help="Include the tensor_parallel_size metadata in every request payload."
    )
    parser.add_argument(
        "--early-stop",
        type=float,
        default=0.0,
        help="Fraction (0-1) of responses to wait for before stopping. 0 = wait for all (default), 0.5 = stop after 50%% of responses."
    )
    parser.add_argument(
        "--no-warmup",
        action="store_true",
        help="Disable the warmup phase."
    )
    parser.add_argument(
        "--num-runs",
        type=int,
        default=1,
        help="Number of times to run the main experiment replay (after warmup)."
    )
    args = parser.parse_args()
    
    # Validate early_stop range
    if not (0.0 <= args.early_stop <= 1.0):
        parser.error("--early-stop must be between 0.0 and 1.0")
    
    asyncio.run(replay(args.config, args.include_tp, args.early_stop, args.no_warmup, args.num_runs))
