#!/usr/bin/env python3
"""
Submit PBS jobs from all subdirectories containing job.pbs.
Discovers job dirs automatically. Run from the parent of 1_nodes, 2_nodes, etc.
"""
import argparse
import os
import re
import subprocess
import sys
import time

DEFAULT_QUEUE_LIMITS = {
    "debug": 1,
    "debug-scaling": 4,
    "prod": 100,
}
DEFAULT_SLEEP_SEC = 60


def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))


def discover_job_dirs(parent_dir):
    """Find subdirs of parent_dir that contain job.pbs."""
    job_dirs = []
    for name in sorted(os.listdir(parent_dir)):
        path = os.path.join(parent_dir, name)
        if os.path.isdir(path) and os.path.exists(os.path.join(path, "job.pbs")):
            job_dirs.append(name)
    return job_dirs


def parse_args():
    parser = argparse.ArgumentParser(
        description="Submit PBS jobs with queue-limit backoff."
    )
    parser.add_argument(
        "job_dirs",
        nargs="*",
        help="Optional subset of job directories to submit (e.g. 1_nodes 2_nodes).",
    )
    parser.add_argument(
        "--node-nums",
        default=None,
        help="Comma-separated list of node counts to submit (e.g. 1,2,4). "
             "Filters job dirs whose name starts with <n>_nodes.",
    )
    parser.add_argument(
        "--parent-dir",
        default=None,
        help="Parent directory containing job folders (default: script directory).",
    )
    parser.add_argument(
        "--sleep-sec",
        type=int,
        default=DEFAULT_SLEEP_SEC,
        help="Seconds to sleep between queue checks or retries.",
    )
    parser.add_argument(
        "--queue-limit",
        action="append",
        default=[],
        help="Override queue limit, e.g. --queue-limit debug=2",
    )
    return parser.parse_args()


def read_queue_from_pbs(pbs_path):
    if not os.path.exists(pbs_path):
        return None
    queue_re = re.compile(r"^#PBS\s+-q\s+(\S+)")
    with open(pbs_path, "r") as f:
        for line in f:
            match = queue_re.match(line.strip())
            if match:
                return match.group(1)
    return None


def get_user_queue_counts():
    user = os.environ.get("USER")
    if not user:
        return {}
    try:
        result = subprocess.run(
            ["qstat", "-u", user],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError:
        return {}

    if result.returncode != 0:
        return {}

    counts = {}
    lines = result.stdout.splitlines()
    for line in lines:
        line = line.strip()
        if not line or line.startswith("Job id") or line.startswith("-"):
            continue
        parts = line.split()
        if len(parts) < 6:
            continue
        queue = parts[-1]
        counts[queue] = counts.get(queue, 0) + 1
    return counts


def can_submit(queue, queue_limits):
    if not queue:
        return True
    limit = queue_limits.get(queue)
    if limit is None:
        return True
    counts = get_user_queue_counts()
    return counts.get(queue, 0) < limit


def submit_job(pbs_path):
    try:
        result = subprocess.run(
            ["qsub", pbs_path],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as exc:
        return False, f"qsub not found: {exc}"

    if result.returncode == 0:
        return True, result.stdout.strip()
    return False, result.stderr.strip() or result.stdout.strip()


def submit_with_queue_limit(job_dir, pbs_path, queue, sleep_sec, queue_limits):
    while True:
        if not can_submit(queue, queue_limits):
            print(
                f"[WAIT] Queue {queue} at limit. "
                f"Sleeping {sleep_sec}s before retry..."
            )
            time.sleep(sleep_sec)
            continue

        ok, msg = submit_job(pbs_path)
        if ok:
            print(f"[OK] Submitted {job_dir}: {msg}")
            return True
        print(
            f"[RETRY] Failed to submit {job_dir}: {msg} "
            f"(sleep {sleep_sec}s)"
        )
        time.sleep(sleep_sec)


def main():
    args = parse_args()
    parent_dir = os.path.abspath(
        args.parent_dir if args.parent_dir is not None else get_script_dir()
    )
    job_dirs = args.job_dirs if args.job_dirs else discover_job_dirs(parent_dir)

    if args.node_nums is not None:
        node_set = {n.strip() for n in args.node_nums.split(",")}
        job_dirs = [d for d in job_dirs if d.split("_")[0] in node_set]

    queue_limits = dict(DEFAULT_QUEUE_LIMITS)
    for item in args.queue_limit:
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        try:
            queue_limits[key] = int(value)
        except ValueError:
            continue

    print(f"Parent dir: {parent_dir}")
    print(f"Jobs to submit: {', '.join(job_dirs)}")
    print(f"Queue limits: {queue_limits}")

    jobs_by_queue = {}
    for job_dir in job_dirs:
        pbs_path = os.path.join(parent_dir, job_dir, "job.pbs")
        queue = read_queue_from_pbs(pbs_path)
        if not os.path.exists(pbs_path):
            print(f"[SKIP] Missing job.pbs: {pbs_path}")
            continue

        jobs_by_queue.setdefault(queue, []).append((job_dir, pbs_path))

    for queue, jobs in jobs_by_queue.items():
        limit = queue_limits.get(queue)
        requires_serial = limit is not None and limit <= 1

        if requires_serial:
            for job_dir, pbs_path in jobs:
                submit_with_queue_limit(
                    job_dir,
                    pbs_path,
                    queue,
                    args.sleep_sec,
                    queue_limits,
                )
            continue

        failed = []
        for job_dir, pbs_path in jobs:
            ok, msg = submit_job(pbs_path)
            if ok:
                print(f"[OK] Submitted {job_dir}: {msg}")
            else:
                print(f"[WARN] Immediate submit failed {job_dir}: {msg}")
                failed.append((job_dir, pbs_path))

        for job_dir, pbs_path in failed:
            submit_with_queue_limit(
                job_dir,
                pbs_path,
                queue,
                args.sleep_sec,
                queue_limits,
            )

    print("All submissions attempted.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
