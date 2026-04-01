#pragma once
#include <string>

struct ServiceTimeConfig {
    std::string distribution = "fixed";
    double value_ms = 0.0;
    double stddev_ms = 0.0;
};

struct FaultConfig {
    ServiceTimeConfig service_time;
    int max_inflight = 0;
    int max_queue = 0;
    double queue_delay_ms = 0.0;
    double error_rate = 0.0;
    int error_status = 500;
    int reject_status = 429;
    bool close_after_response = false;
    bool reset_after_response = false;
    double idle_timeout_s = 0.0;
    int burst_every = 0;
    int burst_duration = 0;
};

struct ServerConfig {
    std::string host = "127.0.0.1";
    int port = 18100;
    int response_tokens = 32;
    std::string model = "stub-model";
    int prompt_words = 32;
    int client_max_active = 0;  // from client.max_active_requests; used to size thread pool
    FaultConfig faults;
};

ServerConfig load_config(const std::string& path);
