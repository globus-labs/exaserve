#include "metrics.hpp"
#include <cstdio>
#include <ctime>
#include <string>
#include <vector>

std::string Metrics::to_json() {
    std::vector<double> lats, qwaits, timestamps;
    {
        std::lock_guard<std::mutex> lock(samples_mu);
        lats = latency_samples.snapshot();
        qwaits = queue_wait_samples.snapshot();
        timestamps = request_timestamps.snapshot();
    }

    uint64_t total = total_requests.load();
    uint64_t acc = accepted.load();
    uint64_t comp = completed.load();
    uint64_t rej = rejections.load();
    uint64_t err = errors.load();
    int ma = max_active.load();
    int mqd = max_queue_depth.load();

    double error_frac = (comp > 0) ? static_cast<double>(err) / static_cast<double>(comp) : 0.0;

    double now = static_cast<double>(std::time(nullptr));
    int rps = 0;
    for (double ts : timestamps) {
        if (now - ts <= 1.0) rps++;
    }

    double lat_p50 = percentile(lats, 0.50);
    double lat_p99 = percentile(lats, 0.99);
    double qw_p50 = percentile(qwaits, 0.50);
    double qw_p99 = percentile(qwaits, 0.99);

    char buf[1024];
    std::snprintf(buf, sizeof(buf),
        R"({"total_requests":%llu,"accepted":%llu,"completed":%llu,)"
        R"("rejections":%llu,"errors":%llu,"error_fraction":%.6f,)"
        R"("rps_last_1s":%d,"max_active":%d,"max_queue_depth":%d,)"
        R"("latency_p50_s":%.9f,"latency_p99_s":%.9f,)"
        R"("queue_wait_p50_s":%.9f,"queue_wait_p99_s":%.9f})",
        (unsigned long long)total, (unsigned long long)acc, (unsigned long long)comp,
        (unsigned long long)rej, (unsigned long long)err, error_frac,
        rps, ma, mqd,
        lat_p50, lat_p99, qw_p50, qw_p99);
    return std::string(buf);
}
