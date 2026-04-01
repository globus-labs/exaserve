#pragma once
#include "config.hpp"
#include <chrono>
#include <deque>
#include <mutex>
#include <random>

// Non-blocking capacity gate for the reactor pattern.
// Replaces the old blocking CapacityGate that used condition_variable::wait().
class AsyncCapacityGate {
public:
    enum AdmitResult { ADMITTED, QUEUED, REJECTED };

    explicit AsyncCapacityGate(int max_inflight, int max_queue);

    // Non-blocking. Returns ADMITTED (service slot acquired), QUEUED (admitted
    // but no service slot — caller should park), or REJECTED (429).
    AdmitResult try_admit();

    // Called when a connection enters WAITING_GATE.
    void enqueue_waiter(int worker_id);

    // Release one service + capacity slot. Returns the worker_id of a
    // waiting connection that should now proceed, or -1 if none.
    int release();

    bool enabled() const { return enabled_; }

private:
    bool enabled_;
    std::mutex mu_;
    int service_slots_;
    int capacity_slots_;
    std::deque<int> waiter_queue_;  // worker_ids waiting for service slots
};

class FaultInjector {
public:
    explicit FaultInjector(const FaultConfig& cfg);

    // Returns service delay duration.
    std::chrono::nanoseconds service_delay();

    // Returns queue delay duration.
    std::chrono::nanoseconds queue_delay() const;

    // Should this request be an injected error? Uses request_index for burst logic.
    bool should_inject_error(uint64_t request_index);

    // Seed the RNG with a specific value (used per-worker in reactor).
    void seed_rng(unsigned seed);

private:
    FaultConfig cfg_;
    std::mt19937 rng_;
    bool rng_seeded_ = false;
    std::mt19937& get_rng();
};
