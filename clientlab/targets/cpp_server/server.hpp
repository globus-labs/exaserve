#pragma once
#include "config.hpp"
#include "faults.hpp"
#include "handler.hpp"
#include "metrics.hpp"
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class Server {
public:
    explicit Server(const ServerConfig& cfg);
    ~Server();

    // Blocks until SIGTERM or error.
    void run();

private:
    void accept_loop(int listen_fd);
    void worker_loop();
    void handle_connection(int fd);
    void handle_post(int fd, const ParsedRequest& req);

    const std::string& pick_response(const ParsedRequest& req, bool is_chat, bool injected_error);

    ServerConfig cfg_;
    ResponseSet responses_;
    Metrics metrics_;
    CapacityGate gate_;
    FaultInjector fault_injector_;

    // Thread pool work queue.
    std::mutex queue_mu_;
    std::condition_variable queue_cv_;
    std::queue<int> fd_queue_;
    std::atomic<bool> running_{true};
    std::vector<std::thread> workers_;
};
