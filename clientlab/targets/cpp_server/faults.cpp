#include "faults.hpp"
#include <algorithm>
#include <cmath>
#include <thread>

// ---- CapacityGate ----

CapacityGate::CapacityGate(int max_inflight, int max_queue)
    : enabled_(max_inflight > 0),
      service_slots_(max_inflight),
      capacity_slots_(max_inflight > 0 ? max_inflight + max_queue : 0) {}

bool CapacityGate::try_admit() {
    if (!enabled_) return true;
    std::lock_guard<std::mutex> lock(mu_);
    if (capacity_slots_ <= 0) return false;
    capacity_slots_--;
    return true;
}

double CapacityGate::acquire_service() {
    if (!enabled_) return 0.0;
    auto start = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this] { return service_slots_ > 0; });
    service_slots_--;
    lock.unlock();
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(end - start).count();
}

void CapacityGate::release() {
    if (!enabled_) return;
    std::lock_guard<std::mutex> lock(mu_);
    service_slots_++;
    capacity_slots_++;
    cv_.notify_one();
}

// ---- FaultInjector ----

thread_local std::mt19937 FaultInjector::rng_;
thread_local bool FaultInjector::rng_seeded_ = false;

FaultInjector::FaultInjector(const FaultConfig& cfg) : cfg_(cfg) {}

std::mt19937& FaultInjector::get_rng() {
    if (!rng_seeded_) {
        rng_.seed(42 + static_cast<unsigned>(std::hash<std::thread::id>{}(std::this_thread::get_id())));
        rng_seeded_ = true;
    }
    return rng_;
}

std::chrono::nanoseconds FaultInjector::service_delay() {
    const auto& st = cfg_.service_time;
    double delay_s = 0.0;

    if (st.distribution == "fixed") {
        delay_s = std::max(st.value_ms, 0.0) / 1000.0;
    } else if (st.distribution == "normal") {
        std::normal_distribution<double> dist(st.value_ms, st.stddev_ms);
        delay_s = std::max(dist(get_rng()), 0.0) / 1000.0;
    } else if (st.distribution == "lognormal") {
        double sigma = std::max(st.stddev_ms / 1000.0, 0.01);
        double mean = std::max(st.value_ms / 1000.0, 1e-6);
        double mu = std::log(mean) - 0.5 * sigma * sigma;
        std::lognormal_distribution<double> dist(mu, sigma);
        delay_s = std::max(dist(get_rng()), 0.0);
    } else {
        delay_s = std::max(st.value_ms, 0.0) / 1000.0;
    }

    return std::chrono::nanoseconds(static_cast<int64_t>(delay_s * 1e9));
}

std::chrono::nanoseconds FaultInjector::queue_delay() const {
    if (cfg_.queue_delay_ms <= 0.0) return std::chrono::nanoseconds(0);
    return std::chrono::nanoseconds(static_cast<int64_t>(cfg_.queue_delay_ms * 1e6));
}

bool FaultInjector::should_inject_error(uint64_t request_index) {
    if (cfg_.error_rate > 0.0) {
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        if (dist(get_rng()) < cfg_.error_rate) return true;
    }
    if (cfg_.burst_every > 0 && cfg_.burst_duration > 0) {
        uint64_t phase = request_index % static_cast<uint64_t>(cfg_.burst_every);
        if (phase < static_cast<uint64_t>(cfg_.burst_duration)) return true;
    }
    return false;
}
