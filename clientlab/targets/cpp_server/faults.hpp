#pragma once
#include "config.hpp"
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <random>

class CapacityGate {
public:
    explicit CapacityGate(int max_inflight, int max_queue);

    // Non-blocking admission check. Returns false if total capacity exceeded (→ 429).
    bool try_admit();

    // Blocks until a service slot is available. Returns queue wait in seconds.
    double acquire_service();

    // Release both service and capacity slots.
    void release();

    bool enabled() const { return enabled_; }

private:
    bool enabled_;
    std::mutex mu_;
    std::condition_variable cv_;
    int service_slots_;
    int capacity_slots_;
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

private:
    FaultConfig cfg_;
    // Thread-local RNG is used; this seed is for reference.
    static thread_local std::mt19937 rng_;
    static thread_local bool rng_seeded_;
    std::mt19937& get_rng();
};
