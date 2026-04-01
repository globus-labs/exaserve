#include "faults.hpp"
#include <algorithm>
#include <cmath>

// ---- AsyncCapacityGate ----

AsyncCapacityGate::AsyncCapacityGate(int max_inflight, int max_queue)
    : enabled_(max_inflight > 0),
      service_slots_(max_inflight),
      capacity_slots_(max_inflight > 0 ? max_inflight + max_queue : 0) {}

AsyncCapacityGate::AdmitResult AsyncCapacityGate::try_admit() {
    if (!enabled_) return ADMITTED;
    std::lock_guard<std::mutex> lock(mu_);
    if (capacity_slots_ <= 0) return REJECTED;
    capacity_slots_--;
    if (service_slots_ > 0) {
        service_slots_--;
        return ADMITTED;
    }
    return QUEUED;
}

void AsyncCapacityGate::enqueue_waiter(int worker_id) {
    std::lock_guard<std::mutex> lock(mu_);
    waiter_queue_.push_back(worker_id);
}

int AsyncCapacityGate::release() {
    if (!enabled_) return -1;
    std::lock_guard<std::mutex> lock(mu_);
    service_slots_++;
    capacity_slots_++;
    if (!waiter_queue_.empty()) {
        // Immediately grant the service slot to the next waiter.
        service_slots_--;
        int wid = waiter_queue_.front();
        waiter_queue_.pop_front();
        return wid;
    }
    return -1;
}

// ---- FaultInjector ----

FaultInjector::FaultInjector(const FaultConfig& cfg) : cfg_(cfg) {}

void FaultInjector::seed_rng(unsigned seed) {
    rng_.seed(seed);
    rng_seeded_ = true;
}

std::mt19937& FaultInjector::get_rng() {
    if (!rng_seeded_) {
        rng_.seed(42);
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
