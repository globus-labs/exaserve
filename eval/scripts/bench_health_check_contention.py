#!/usr/bin/env python3
"""
Benchmark: measure health check response latency under replica-count scaling.

Creates N null-compute replicas via Ray actors (not Serve) and measures how
long it takes to get a health check response from each actor as N grows.
This simulates what the ServeController does during startup.

Usage:
    python3 bench_health_check_contention.py [max_actors]
"""
import os
import sys
import time
import ray

# Suppress excessive Ray logging
os.environ.setdefault("RAY_DEDUP_LOGS", "0")


@ray.remote(num_cpus=0.01)
class FakeReplica:
    """Minimal actor that simulates a Ray Serve replica."""

    def __init__(self, actor_id: int, init_delay: float = 0.0):
        self.actor_id = actor_id
        self.created_at = time.time()
        if init_delay > 0:
            time.sleep(init_delay)

    def health_check(self) -> dict:
        return {
            "actor_id": self.actor_id,
            "pid": os.getpid(),
            "uptime": time.time() - self.created_at,
        }


def measure_health_check_latency(actors: list) -> dict:
    """Send a health check to every actor and measure response time."""
    start = time.time()
    refs = [a.health_check.remote() for a in actors]
    # Wait for all responses
    results = ray.get(refs, timeout=60)
    elapsed = time.time() - start

    # Measure per-actor response time by submitting one at a time
    per_actor_times = []
    for a in actors[:min(10, len(actors))]:  # sample 10
        t0 = time.time()
        ray.get(a.health_check.remote(), timeout=30)
        per_actor_times.append(time.time() - t0)

    return {
        "total_actors": len(actors),
        "batch_latency_s": elapsed,
        "per_actor_mean_s": sum(per_actor_times) / len(per_actor_times) if per_actor_times else 0,
        "per_actor_max_s": max(per_actor_times) if per_actor_times else 0,
    }


def main():
    max_actors = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    init_delay = float(sys.argv[2]) if len(sys.argv) > 2 else 0.0

    ray.init(ignore_reinit_error=True)

    # Test at increasing actor counts
    actor_counts = [1, 4, 12, 24, 48, 96]
    actor_counts = [n for n in actor_counts if n <= max_actors]
    if max_actors not in actor_counts:
        actor_counts.append(max_actors)

    print(f"{'actors':>8} {'batch_lat(s)':>12} {'per_actor_mean(s)':>18} {'per_actor_max(s)':>16}")
    print("-" * 60)

    actors = []
    for target_count in actor_counts:
        # Create actors to reach target count
        while len(actors) < target_count:
            a = FakeReplica.remote(len(actors), init_delay=init_delay)
            actors.append(a)

        # Wait for all actors to be ready
        ray.get([a.health_check.remote() for a in actors], timeout=60)
        time.sleep(1)  # Let things settle

        # Measure
        result = measure_health_check_latency(actors)
        print(
            f"{result['total_actors']:>8} "
            f"{result['batch_latency_s']:>12.4f} "
            f"{result['per_actor_mean_s']:>18.4f} "
            f"{result['per_actor_max_s']:>16.4f}"
        )

    # Cleanup
    for a in actors:
        ray.kill(a)
    print("\nDone.")


if __name__ == "__main__":
    main()
