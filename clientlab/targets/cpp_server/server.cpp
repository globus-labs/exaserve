#include "server.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

Server::Server(const ServerConfig& cfg)
    : cfg_(cfg),
      responses_(build_responses(cfg)),
      gate_(cfg.faults.max_inflight, cfg.faults.max_queue),
      fault_injector_(cfg.faults) {}

Server::~Server() {
    running_.store(false);
    queue_cv_.notify_all();
    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
}

void Server::run() {
    // Block SIGTERM/SIGINT in all threads; we'll read them via signalfd.
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGTERM);
    sigaddset(&mask, SIGINT);
    pthread_sigmask(SIG_BLOCK, &mask, nullptr);

    int sig_fd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (sig_fd < 0) {
        std::perror("signalfd");
        return;
    }

    // Create listen socket.
    int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd < 0) {
        std::perror("socket");
        close(sig_fd);
        return;
    }
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(cfg_.port));
    inet_pton(AF_INET, cfg_.host.c_str(), &addr.sin_addr);

    if (bind(listen_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::perror("bind");
        close(listen_fd);
        close(sig_fd);
        return;
    }
    if (listen(listen_fd, 512) < 0) {
        std::perror("listen");
        close(listen_fd);
        close(sig_fd);
        return;
    }

    std::fprintf(stderr, "Listening on %s:%d\n", cfg_.host.c_str(), cfg_.port);

    // Start worker threads.
    unsigned num_workers = std::max(std::thread::hardware_concurrency(), 4u);
    for (unsigned i = 0; i < num_workers; i++) {
        workers_.emplace_back(&Server::worker_loop, this);
    }

    // epoll: listen socket + signal fd.
    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    struct epoll_event ev{};

    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev);

    ev.events = EPOLLIN;
    ev.data.fd = sig_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sig_fd, &ev);

    struct epoll_event events[64];
    while (running_.load(std::memory_order_relaxed)) {
        int n = epoll_wait(epoll_fd, events, 64, 500);
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == sig_fd) {
                // Signal received — shutdown.
                running_.store(false);
                break;
            }
            if (events[i].data.fd == listen_fd) {
                // Accept all pending connections.
                while (true) {
                    int client_fd = accept4(listen_fd, nullptr, nullptr, SOCK_CLOEXEC);
                    if (client_fd < 0) break;
                    int tcp_nodelay = 1;
                    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &tcp_nodelay, sizeof(tcp_nodelay));
                    {
                        std::lock_guard<std::mutex> lock(queue_mu_);
                        fd_queue_.push(client_fd);
                    }
                    queue_cv_.notify_one();
                }
            }
        }
    }

    close(epoll_fd);
    close(listen_fd);
    close(sig_fd);

    // Wake all workers to exit.
    running_.store(false);
    queue_cv_.notify_all();
}

void Server::worker_loop() {
    while (true) {
        int fd;
        {
            std::unique_lock<std::mutex> lock(queue_mu_);
            queue_cv_.wait(lock, [this] { return !fd_queue_.empty() || !running_.load(); });
            if (!running_.load() && fd_queue_.empty()) return;
            fd = fd_queue_.front();
            fd_queue_.pop();
        }
        handle_connection(fd);
    }
}

void Server::handle_connection(int fd) {
    double recv_timeout = cfg_.faults.idle_timeout_s;

    while (running_.load(std::memory_order_relaxed)) {
        ParsedRequest req;
        if (!read_request(fd, req, recv_timeout)) break;

        if (req.method == ParsedRequest::GET) {
            if (req.path == "/health" || req.path == "/health/liveliness") {
                write_response(fd, responses_.health);
            } else if (req.path == "/v1/models") {
                write_response(fd, responses_.models);
            } else if (req.path == "/metrics") {
                std::string json = metrics_.to_json();
                std::string resp = build_metrics_response(json);
                write_response(fd, resp);
            } else {
                std::string body = R"({"error":"not found"})";
                std::string resp = "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n"
                                   "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n" + body;
                write_response(fd, resp);
            }
            if (!req.keep_alive) break;
            continue;
        }

        if (req.method == ParsedRequest::POST &&
            (req.path == "/v1/chat/completions" || req.path == "/v1/completions")) {
            handle_post(fd, req);
            if (cfg_.faults.reset_after_response) {
                struct linger lg{1, 0};
                setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
                shutdown(fd, SHUT_RDWR);
                close(fd);
                return;
            }
            if (cfg_.faults.close_after_response || !req.keep_alive) {
                shutdown(fd, SHUT_WR);
                break;
            }
            continue;
        }

        // Unknown method/path.
        break;
    }

    close(fd);
}

void Server::handle_post(int fd, const ParsedRequest& req) {
    bool is_chat = (req.path == "/v1/chat/completions");
    uint64_t req_idx = metrics_.total_requests.fetch_add(1, std::memory_order_relaxed);

    // Capacity check.
    if (!gate_.try_admit()) {
        metrics_.rejections.fetch_add(1, std::memory_order_relaxed);
        const std::string& resp = cfg_.faults.close_after_response ? responses_.reject_close : responses_.reject;
        write_response(fd, resp);
        return;
    }

    metrics_.accepted.fetch_add(1, std::memory_order_relaxed);
    int w = metrics_.waiting.fetch_add(1, std::memory_order_relaxed) + 1;
    atomic_max(metrics_.max_queue_depth, w);

    // Acquire service slot (may block).
    double queue_wait = gate_.acquire_service();

    metrics_.waiting.fetch_sub(1, std::memory_order_relaxed);
    int a = metrics_.active.fetch_add(1, std::memory_order_relaxed) + 1;
    atomic_max(metrics_.max_active, a);

    auto start = std::chrono::steady_clock::now();

    bool injected_error = fault_injector_.should_inject_error(req_idx);

    // Queue delay.
    auto qd = fault_injector_.queue_delay();
    if (qd.count() > 0) std::this_thread::sleep_for(qd);

    // Service delay.
    auto sd = fault_injector_.service_delay();
    if (sd.count() > 0) std::this_thread::sleep_for(sd);

    // Write response.
    const std::string& resp = pick_response(req, is_chat, injected_error);
    write_response(fd, resp);

    auto end = std::chrono::steady_clock::now();
    double latency_s = std::chrono::duration<double>(end - start).count();

    // Record metrics.
    metrics_.active.fetch_sub(1, std::memory_order_relaxed);
    metrics_.completed.fetch_add(1, std::memory_order_relaxed);
    if (injected_error) {
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
    }
    {
        std::lock_guard<std::mutex> lock(metrics_.samples_mu);
        metrics_.request_timestamps.push(static_cast<double>(std::time(nullptr)));
        metrics_.latency_samples.push(latency_s);
        metrics_.queue_wait_samples.push(queue_wait);
    }

    gate_.release();
}

const std::string& Server::pick_response(const ParsedRequest& /*req*/, bool is_chat, bool injected_error) {
    bool do_close = cfg_.faults.close_after_response;
    if (injected_error) {
        return do_close ? responses_.error_close : responses_.error;
    }
    if (is_chat) {
        return do_close ? responses_.chat_ok_close : responses_.chat_ok;
    }
    return do_close ? responses_.completion_ok_close : responses_.completion_ok;
}
