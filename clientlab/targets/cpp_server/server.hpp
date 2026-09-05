#pragma once
#include "config.hpp"
#include "faults.hpp"
#include "handler.hpp"
#include "metrics.hpp"
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

// Forward declaration.
class Server;

// ---- Per-connection state machine ----

enum class ConnPhase : uint8_t {
    READING_HEADERS,
    READING_BODY,
    WAITING_GATE,
    DELAYING,
    WRITING,
    IDLE,
    CLOSED
};

struct ConnectionState {
    int fd = -1;
    ConnPhase phase = ConnPhase::READING_HEADERS;

    // Read buffer (accumulates until \r\n\r\n found).
    static constexpr size_t kReadBufSize = 8192;
    char read_buf[kReadBufSize];
    size_t read_len = 0;

    // Parsed request (filled after parse_headers succeeds).
    ParsedRequest req;
    size_t body_remaining = 0;

    // Write state. response points into ResponseSet (zero alloc for most requests).
    // owned_response holds dynamically built responses (/metrics).
    const std::string* response = nullptr;
    std::string owned_response;
    size_t write_offset = 0;

    // Timing / fault state.
    uint64_t req_index = 0;
    bool injected_error = false;
    bool is_post = false;
    std::chrono::steady_clock::time_point deadline;
    std::chrono::steady_clock::time_point service_start;
    double queue_wait_s = 0.0;
    std::chrono::steady_clock::time_point idle_deadline;

    void reset_for_next_request() {
        phase = ConnPhase::READING_HEADERS;
        read_len = 0;
        req = ParsedRequest{};
        body_remaining = 0;
        response = nullptr;
        owned_response.clear();
        write_offset = 0;
        req_index = 0;
        injected_error = false;
        is_post = false;
        queue_wait_s = 0.0;
    }
};

// ---- Deadline min-heap entry ----

struct DeadlineEntry {
    std::chrono::steady_clock::time_point when;
    int fd;
    bool operator>(const DeadlineEntry& o) const { return when > o.when; }
};

// ---- Per-worker reactor ----

class WorkerReactor {
    friend class Server;  // Server needs access to event_fd_ for shutdown wakeup.
public:
    WorkerReactor(int worker_id, Server& server);
    ~WorkerReactor();

    // Called by main thread to hand off a newly accepted fd.
    void add_connection(int fd);

    // Called by any thread to notify that a WAITING_GATE connection can proceed.
    void notify_gate_release();

    // Worker thread entry point.
    void run();

    int worker_id() const { return worker_id_; }

private:
    void process_new_connections();
    void process_gate_releases();
    void fire_expired_deadlines();
    void rearm_timerfd();

    void handle_readable(ConnectionState& conn);
    void handle_writable(ConnectionState& conn);
    void route_request(ConnectionState& conn);
    void route_post(ConnectionState& conn);
    void enter_delay_phase(ConnectionState& conn);
    void prepare_post_response(ConnectionState& conn);
    void finish_response(ConnectionState& conn);
    void set_epoll_interest(int fd, uint32_t events);
    void close_connection(ConnectionState& conn);

    int worker_id_;
    Server& server_;

    int epoll_fd_ = -1;
    int event_fd_ = -1;
    int timer_fd_ = -1;

    // New connections arrive via this queue (only lock shared with main thread).
    std::mutex incoming_mu_;
    std::vector<int> incoming_fds_;

    // Gate release count (atomic, written by any releasing worker).
    std::atomic<int> gate_wakeups_{0};

    // Connection storage.
    std::unordered_map<int, ConnectionState> connections_;

    // Deadline min-heap for DELAYING and IDLE connections.
    std::priority_queue<DeadlineEntry, std::vector<DeadlineEntry>,
                        std::greater<DeadlineEntry>> deadline_heap_;

    // Fds in WAITING_GATE (FIFO order).
    std::vector<int> gate_waiters_;

    // Per-worker fault injector (owns its own RNG).
    FaultInjector fault_injector_;
};

// ---- Server (main thread) ----

class Server {
public:
    explicit Server(const ServerConfig& cfg);
    ~Server();

    // Blocks until SIGTERM; returns nonzero on startup/runtime failure.
    int run();

    // Accessors for workers.
    const ServerConfig& config() const { return cfg_; }
    const ResponseSet& responses() const { return responses_; }
    Metrics& metrics() { return metrics_; }
    AsyncCapacityGate& gate() { return gate_; }
    std::atomic<bool>& running() { return running_; }

    // Notify a specific worker that a gate slot opened for its waiting connection.
    void notify_worker(int worker_id);

private:
    ServerConfig cfg_;
    ResponseSet responses_;
    Metrics metrics_;
    AsyncCapacityGate gate_;
    std::atomic<bool> running_{true};

    std::vector<std::unique_ptr<WorkerReactor>> workers_;
    std::vector<std::thread> worker_threads_;
    unsigned next_worker_ = 0;
};
