#pragma once
#include <algorithm>
#include <atomic>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

template <size_t Cap>
class RingBuffer {
public:
    void push(double v) {
        buf_[pos_ % Cap] = v;
        pos_++;
        if (size_ < Cap) size_++;
    }

    std::vector<double> snapshot() const {
        std::vector<double> out(size_);
        if (size_ < Cap) {
            std::memcpy(out.data(), buf_, size_ * sizeof(double));
        } else {
            size_t start = pos_ % Cap;
            size_t tail = Cap - start;
            std::memcpy(out.data(), buf_ + start, tail * sizeof(double));
            std::memcpy(out.data() + tail, buf_, start * sizeof(double));
        }
        return out;
    }

private:
    double buf_[Cap]{};
    size_t pos_ = 0;
    size_t size_ = 0;
};

inline double percentile(std::vector<double>& sorted_vals, double fraction) {
    if (sorted_vals.empty()) return 0.0;
    std::sort(sorted_vals.begin(), sorted_vals.end());
    size_t idx = static_cast<size_t>((static_cast<double>(sorted_vals.size()) - 1.0) * fraction);
    if (idx >= sorted_vals.size()) idx = sorted_vals.size() - 1;
    return sorted_vals[idx];
}

// Atomically update a max value.
template <typename T>
void atomic_max(std::atomic<T>& target, T value) {
    T prev = target.load(std::memory_order_relaxed);
    while (prev < value && !target.compare_exchange_weak(prev, value, std::memory_order_relaxed)) {}
}

struct Metrics {
    std::atomic<uint64_t> total_requests{0};
    std::atomic<uint64_t> accepted{0};
    std::atomic<uint64_t> completed{0};
    std::atomic<uint64_t> rejections{0};
    std::atomic<uint64_t> errors{0};
    std::atomic<int> active{0};
    std::atomic<int> waiting{0};
    std::atomic<int> max_active{0};
    std::atomic<int> max_queue_depth{0};

    std::mutex samples_mu;
    RingBuffer<50000> latency_samples;
    RingBuffer<50000> queue_wait_samples;
    RingBuffer<50000> request_timestamps;

    std::string to_json();
};
