#include "server.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <climits>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

// ---- Server ----

Server::Server(const ServerConfig& cfg)
    : cfg_(cfg),
      responses_(build_responses(cfg)),
      gate_(cfg.faults.max_inflight, cfg.faults.max_queue) {}

Server::~Server() {
    running_.store(false);
    for (auto& w : workers_) {
        // Wake each worker so it exits its epoll loop.
        uint64_t val = 1;
        write(w->event_fd_, &val, sizeof(val));
    }
    for (auto& t : worker_threads_) {
        if (t.joinable()) t.join();
    }
}

void Server::notify_worker(int worker_id) {
    if (worker_id >= 0 && worker_id < static_cast<int>(workers_.size())) {
        workers_[static_cast<size_t>(worker_id)]->notify_gate_release();
    }
}

int Server::run() {
    // Block SIGTERM/SIGINT in all threads; read via signalfd.
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGTERM);
    sigaddset(&mask, SIGINT);
    int mask_result = pthread_sigmask(SIG_BLOCK, &mask, nullptr);
    if (mask_result != 0) {
        errno = mask_result;
        std::perror("pthread_sigmask");
        return 1;
    }

    int sig_fd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (sig_fd < 0) { std::perror("signalfd"); return 1; }

    // Prefer a socket bound/listening by the owning launcher. This removes the
    // probe/close/rebind race and makes a bind collision a startup failure
    // before the synthetic target can claim health.
    int listen_fd = -1;
    const char* inherited = std::getenv("CLIENTLAB_LISTEN_FD");
    if (inherited != nullptr && *inherited != '\0') {
        char* end = nullptr;
        long parsed = std::strtol(inherited, &end, 10);
        if (end == inherited || *end != '\0' || parsed < 0 || parsed > INT_MAX) {
            std::fprintf(stderr, "Invalid CLIENTLAB_LISTEN_FD\n");
            close(sig_fd);
            return 1;
        }
        listen_fd = static_cast<int>(parsed);
        if (fcntl(listen_fd, F_GETFD) < 0) {
            std::perror("inherited listen fd"); close(sig_fd); return 1;
        }
        int flags = fcntl(listen_fd, F_GETFL, 0);
        if (flags < 0 || fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
            std::perror("fcntl inherited listen fd"); close(listen_fd); close(sig_fd); return 1;
        }
        if (fcntl(listen_fd, F_SETFD, FD_CLOEXEC) < 0) {
            std::perror("fcntl inherited listen fd cloexec");
            close(listen_fd);
            close(sig_fd);
            return 1;
        }
    } else {
        listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (listen_fd < 0) { std::perror("socket"); close(sig_fd); return 1; }
        int opt = 1;
        if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            std::perror("setsockopt SO_REUSEADDR");
            close(listen_fd);
            close(sig_fd);
            return 1;
        }

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(cfg_.port));
        if (inet_pton(AF_INET, cfg_.host.c_str(), &addr.sin_addr) != 1) {
            std::fprintf(stderr, "Invalid IPv4 bind address: %s\n", cfg_.host.c_str());
            close(listen_fd);
            close(sig_fd);
            return 1;
        }

        if (bind(listen_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            std::perror("bind"); close(listen_fd); close(sig_fd); return 1;
        }
        if (listen(listen_fd, 4096) < 0) {
            std::perror("listen"); close(listen_fd); close(sig_fd); return 1;
        }
    }

    std::fprintf(stderr, "Listening on %s:%d\n", cfg_.host.c_str(), cfg_.port);

    // Create worker reactors.
    unsigned num_workers = std::max(std::thread::hardware_concurrency(), 4u);
    std::fprintf(stderr, "Worker reactors: %u\n", num_workers);

    for (unsigned i = 0; i < num_workers; i++) {
        workers_.push_back(std::make_unique<WorkerReactor>(static_cast<int>(i), *this));
    }
    for (unsigned i = 0; i < num_workers; i++) {
        worker_threads_.emplace_back([this, i] { workers_[i]->run(); });
    }

    // Main thread: epoll on listen_fd + sig_fd.
    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) {
        std::perror("epoll_create1");
        running_.store(false);
        close(listen_fd);
        close(sig_fd);
        return 1;
    }
    struct epoll_event ev{};

    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) {
        std::perror("epoll_ctl listen");
        running_.store(false);
        close(epoll_fd);
        close(listen_fd);
        close(sig_fd);
        return 1;
    }

    ev.events = EPOLLIN;
    ev.data.fd = sig_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sig_fd, &ev) < 0) {
        std::perror("epoll_ctl signal");
        running_.store(false);
        close(epoll_fd);
        close(listen_fd);
        close(sig_fd);
        return 1;
    }

    struct epoll_event events[64];
    int exit_code = 0;
    while (running_.load(std::memory_order_relaxed)) {
        int n = epoll_wait(epoll_fd, events, 64, 500);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::perror("epoll_wait");
            running_.store(false);
            exit_code = 1;
            break;
        }
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == sig_fd) {
                running_.store(false);
                break;
            }
            if (events[i].data.fd == listen_fd) {
                // Accept all pending connections, round-robin to workers.
                while (true) {
                    int client_fd = accept4(listen_fd, nullptr, nullptr,
                                            SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (client_fd < 0) break;
                    int tcp_nodelay = 1;
                    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY,
                               &tcp_nodelay, sizeof(tcp_nodelay));
                    unsigned idx = next_worker_++ % num_workers;
                    workers_[idx]->add_connection(client_fd);
                }
            }
        }
    }

    close(epoll_fd);
    close(listen_fd);
    close(sig_fd);
    return exit_code;
}

// ---- WorkerReactor ----

WorkerReactor::WorkerReactor(int worker_id, Server& server)
    : worker_id_(worker_id),
      server_(server),
      fault_injector_(server.config().faults) {
    fault_injector_.seed_rng(42 + static_cast<unsigned>(worker_id));

    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);

    // Register eventfd and timerfd with epoll (level-triggered).
    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = event_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &ev);

    ev.events = EPOLLIN;
    ev.data.fd = timer_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timer_fd_, &ev);
}

WorkerReactor::~WorkerReactor() {
    for (auto& [fd, conn] : connections_) {
        close(fd);
    }
    if (timer_fd_ >= 0) close(timer_fd_);
    if (event_fd_ >= 0) close(event_fd_);
    if (epoll_fd_ >= 0) close(epoll_fd_);
}

void WorkerReactor::add_connection(int fd) {
    {
        std::lock_guard<std::mutex> lock(incoming_mu_);
        incoming_fds_.push_back(fd);
    }
    uint64_t val = 1;
    write(event_fd_, &val, sizeof(val));
}

void WorkerReactor::notify_gate_release() {
    gate_wakeups_.fetch_add(1, std::memory_order_release);
    uint64_t val = 1;
    write(event_fd_, &val, sizeof(val));
}

void WorkerReactor::run() {
    struct epoll_event events[256];

    while (server_.running().load(std::memory_order_relaxed)) {
        // Compute timeout from nearest deadline.
        int timeout_ms = 500;
        if (!deadline_heap_.empty()) {
            auto now = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline_heap_.top().when - now);
            timeout_ms = std::max(static_cast<int>(delta.count()), 0);
            if (timeout_ms > 500) timeout_ms = 500;
        }

        int n = epoll_wait(epoll_fd_, events, 256, timeout_ms);

        // Fire expired deadlines first.
        fire_expired_deadlines();

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;

            if (fd == event_fd_) {
                uint64_t val;
                read(event_fd_, &val, sizeof(val));
                process_new_connections();
                process_gate_releases();
                continue;
            }
            if (fd == timer_fd_) {
                uint64_t val;
                read(timer_fd_, &val, sizeof(val));
                // Deadlines already handled above.
                continue;
            }

            auto it = connections_.find(fd);
            if (it == connections_.end()) continue;
            ConnectionState& conn = it->second;

            if (events[i].events & (EPOLLHUP | EPOLLERR)) {
                close_connection(conn);
                continue;
            }
            if (events[i].events & EPOLLIN) {
                handle_readable(conn);
                // Connection may have been closed.
                if (connections_.find(fd) == connections_.end()) continue;
            }
            if (events[i].events & EPOLLOUT) {
                handle_writable(conn);
            }
        }

        rearm_timerfd();
    }

    // Cleanup: close all remaining connections.
    for (auto& [fd, conn] : connections_) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
    }
    connections_.clear();
}

void WorkerReactor::process_new_connections() {
    std::vector<int> fds;
    {
        std::lock_guard<std::mutex> lock(incoming_mu_);
        fds.swap(incoming_fds_);
    }
    for (int fd : fds) {
        auto [it, ok] = connections_.emplace(fd, ConnectionState{});
        if (!ok) {
            close(fd);
            continue;
        }
        it->second.fd = fd;
        it->second.phase = ConnPhase::READING_HEADERS;
        it->second.read_len = 0;
        set_epoll_interest(fd, EPOLLIN | EPOLLET);
    }
}

void WorkerReactor::process_gate_releases() {
    int wakeups = gate_wakeups_.exchange(0, std::memory_order_acquire);
    while (wakeups > 0 && !gate_waiters_.empty()) {
        int fd = gate_waiters_.front();
        gate_waiters_.erase(gate_waiters_.begin());
        wakeups--;

        auto it = connections_.find(fd);
        if (it == connections_.end()) continue;
        ConnectionState& conn = it->second;
        if (conn.phase != ConnPhase::WAITING_GATE) continue;

        server_.metrics().waiting.fetch_sub(1, std::memory_order_relaxed);
        conn.queue_wait_s = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - conn.service_start).count();
        enter_delay_phase(conn);
    }
}

void WorkerReactor::fire_expired_deadlines() {
    auto now = std::chrono::steady_clock::now();
    while (!deadline_heap_.empty() && deadline_heap_.top().when <= now) {
        auto entry = deadline_heap_.top();
        deadline_heap_.pop();

        auto it = connections_.find(entry.fd);
        if (it == connections_.end()) continue;
        ConnectionState& conn = it->second;

        if (conn.phase == ConnPhase::DELAYING && conn.deadline == entry.when) {
            prepare_post_response(conn);
            conn.phase = ConnPhase::WRITING;
            conn.write_offset = 0;
            set_epoll_interest(conn.fd, EPOLLOUT | EPOLLET);
            // Try writing immediately.
            handle_writable(conn);
        } else if (conn.phase == ConnPhase::IDLE && conn.idle_deadline == entry.when) {
            close_connection(conn);
        }
        // else: stale entry, ignore.
    }
}

void WorkerReactor::rearm_timerfd() {
    struct itimerspec its{};
    if (!deadline_heap_.empty()) {
        auto now = std::chrono::steady_clock::now();
        auto delta = deadline_heap_.top().when - now;
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(delta);
        if (ns.count() <= 0) ns = std::chrono::nanoseconds(1);
        its.it_value.tv_sec = ns.count() / 1'000'000'000;
        its.it_value.tv_nsec = ns.count() % 1'000'000'000;
    }
    // its = {0,0} disarms the timer if heap is empty.
    timerfd_settime(timer_fd_, 0, &its, nullptr);
}

void WorkerReactor::handle_readable(ConnectionState& conn) {
    if (conn.phase == ConnPhase::READING_HEADERS) {
        // Non-blocking recv into read_buf.
        while (conn.read_len < ConnectionState::kReadBufSize) {
            ssize_t n = recv(conn.fd,
                             conn.read_buf + conn.read_len,
                             ConnectionState::kReadBufSize - conn.read_len, 0);
            if (n > 0) {
                conn.read_len += static_cast<size_t>(n);
            } else if (n == 0) {
                close_connection(conn);
                return;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                close_connection(conn);
                return;
            }
        }

        // Try to parse headers.
        size_t header_end_offset = 0;
        if (parse_headers(conn.read_buf, conn.read_len, conn.req, header_end_offset)) {
            // Headers complete. Check for body.
            if (conn.req.content_length > 0) {
                size_t body_already = conn.read_len - header_end_offset;
                if (body_already >= static_cast<size_t>(conn.req.content_length)) {
                    // Body fully received in the header read.
                    route_request(conn);
                } else {
                    conn.body_remaining = static_cast<size_t>(conn.req.content_length) - body_already;
                    conn.phase = ConnPhase::READING_BODY;
                    // Try to drain more body immediately.
                    handle_readable(conn);
                }
            } else {
                route_request(conn);
            }
        } else if (conn.read_len >= ConnectionState::kReadBufSize) {
            // Headers too large.
            close_connection(conn);
        }
        // else: incomplete headers, wait for more data.
        return;
    }

    if (conn.phase == ConnPhase::READING_BODY) {
        // Drain body bytes (we discard them).
        char drain[4096];
        while (conn.body_remaining > 0) {
            size_t want = std::min(conn.body_remaining, sizeof(drain));
            ssize_t n = recv(conn.fd, drain, want, 0);
            if (n > 0) {
                conn.body_remaining -= static_cast<size_t>(n);
            } else if (n == 0) {
                close_connection(conn);
                return;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                close_connection(conn);
                return;
            }
        }
        if (conn.body_remaining == 0) {
            route_request(conn);
        }
        return;
    }

    if (conn.phase == ConnPhase::IDLE) {
        // Data on a keep-alive connection — new request incoming.
        conn.reset_for_next_request();
        handle_readable(conn);
        return;
    }
}

void WorkerReactor::handle_writable(ConnectionState& conn) {
    if (conn.phase != ConnPhase::WRITING || !conn.response) return;

    const char* data = conn.response->data() + conn.write_offset;
    size_t remaining = conn.response->size() - conn.write_offset;

    while (remaining > 0) {
        ssize_t n = send(conn.fd, data, remaining, MSG_NOSIGNAL);
        if (n > 0) {
            conn.write_offset += static_cast<size_t>(n);
            data += n;
            remaining -= static_cast<size_t>(n);
        } else if (n == 0) {
            break;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            close_connection(conn);
            return;
        }
    }

    if (remaining == 0) {
        finish_response(conn);
    }
}

void WorkerReactor::route_request(ConnectionState& conn) {
    const auto& responses = server_.responses();

    if (conn.req.method == ParsedRequest::GET) {
        if (conn.req.path == "/health" || conn.req.path == "/health/liveliness") {
            conn.response = &responses.health;
        } else if (conn.req.path == "/v1/models") {
            conn.response = &responses.models;
        } else if (conn.req.path == "/metrics") {
            conn.owned_response = build_metrics_response(server_.metrics().to_json());
            conn.response = &conn.owned_response;
        } else {
            conn.response = &responses.not_found;
        }
        conn.is_post = false;
        conn.phase = ConnPhase::WRITING;
        conn.write_offset = 0;
        set_epoll_interest(conn.fd, EPOLLOUT | EPOLLET);
        handle_writable(conn);
        return;
    }

    if (conn.req.method == ParsedRequest::POST &&
        (conn.req.path == "/v1/chat/completions" || conn.req.path == "/v1/completions")) {
        conn.is_post = true;
        route_post(conn);
        return;
    }

    // Unknown method/path.
    close_connection(conn);
}

void WorkerReactor::route_post(ConnectionState& conn) {
    const auto& cfg = server_.config();
    const auto& responses = server_.responses();
    auto& metrics = server_.metrics();

    conn.req_index = metrics.total_requests.fetch_add(1, std::memory_order_relaxed);

    auto result = server_.gate().try_admit();
    if (result == AsyncCapacityGate::REJECTED) {
        metrics.rejections.fetch_add(1, std::memory_order_relaxed);
        bool do_close = cfg.faults.close_after_response;
        conn.response = do_close ? &responses.reject_close : &responses.reject;
        conn.phase = ConnPhase::WRITING;
        conn.write_offset = 0;
        set_epoll_interest(conn.fd, EPOLLOUT | EPOLLET);
        handle_writable(conn);
        return;
    }

    metrics.accepted.fetch_add(1, std::memory_order_relaxed);

    if (result == AsyncCapacityGate::QUEUED) {
        int w = metrics.waiting.fetch_add(1, std::memory_order_relaxed) + 1;
        atomic_max(metrics.max_queue_depth, w);
        conn.service_start = std::chrono::steady_clock::now();
        conn.phase = ConnPhase::WAITING_GATE;
        // Remove from epoll interest while waiting.
        set_epoll_interest(conn.fd, 0);
        server_.gate().enqueue_waiter(worker_id_);
        gate_waiters_.push_back(conn.fd);
        return;
    }

    // ADMITTED — proceed directly to delay phase.
    enter_delay_phase(conn);
}

void WorkerReactor::enter_delay_phase(ConnectionState& conn) {
    auto& metrics = server_.metrics();
    int a = metrics.active.fetch_add(1, std::memory_order_relaxed) + 1;
    atomic_max(metrics.max_active, a);

    conn.service_start = std::chrono::steady_clock::now();
    conn.injected_error = fault_injector_.should_inject_error(conn.req_index);

    auto qd = fault_injector_.queue_delay();
    auto sd = fault_injector_.service_delay();
    auto total_delay = qd + sd;

    if (total_delay.count() > 0) {
        conn.deadline = std::chrono::steady_clock::now() + total_delay;
        deadline_heap_.push({conn.deadline, conn.fd});
        conn.phase = ConnPhase::DELAYING;
        // No epoll interest while delaying.
        set_epoll_interest(conn.fd, 0);
        rearm_timerfd();
    } else {
        // Zero delay — respond immediately.
        prepare_post_response(conn);
        conn.phase = ConnPhase::WRITING;
        conn.write_offset = 0;
        set_epoll_interest(conn.fd, EPOLLOUT | EPOLLET);
        handle_writable(conn);
    }
}

void WorkerReactor::prepare_post_response(ConnectionState& conn) {
    const auto& cfg = server_.config();
    const auto& responses = server_.responses();
    bool is_chat = (conn.req.path == "/v1/chat/completions");
    bool do_close = cfg.faults.close_after_response;

    if (conn.injected_error) {
        conn.response = do_close ? &responses.error_close : &responses.error;
    } else if (is_chat) {
        conn.response = do_close ? &responses.chat_ok_close : &responses.chat_ok;
    } else {
        conn.response = do_close ? &responses.completion_ok_close : &responses.completion_ok;
    }
}

void WorkerReactor::finish_response(ConnectionState& conn) {
    const auto& cfg = server_.config();
    auto& metrics = server_.metrics();

    if (conn.is_post) {
        auto end = std::chrono::steady_clock::now();
        double latency_s = std::chrono::duration<double>(end - conn.service_start).count();

        metrics.active.fetch_sub(1, std::memory_order_relaxed);
        metrics.completed.fetch_add(1, std::memory_order_relaxed);
        if (conn.injected_error) {
            metrics.errors.fetch_add(1, std::memory_order_relaxed);
        }
        {
            std::lock_guard<std::mutex> lock(metrics.samples_mu);
            metrics.request_timestamps.push(static_cast<double>(std::time(nullptr)));
            metrics.latency_samples.push(latency_s);
            metrics.queue_wait_samples.push(conn.queue_wait_s);
        }

        // Release gate slot. If a waiter was freed, notify its worker.
        int wid = server_.gate().release();
        if (wid >= 0) {
            server_.notify_worker(wid);
        }
    }

    // Connection control after response.
    if (cfg.faults.reset_after_response && conn.is_post) {
        struct linger lg{1, 0};
        setsockopt(conn.fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
        shutdown(conn.fd, SHUT_RDWR);
        close_connection(conn);
        return;
    }
    if ((cfg.faults.close_after_response && conn.is_post) || !conn.req.keep_alive) {
        shutdown(conn.fd, SHUT_WR);
        close_connection(conn);
        return;
    }

    // Keep-alive: wait for next request.
    conn.reset_for_next_request();
    conn.phase = ConnPhase::IDLE;
    set_epoll_interest(conn.fd, EPOLLIN | EPOLLET);

    // Set idle timeout if configured.
    double idle_s = cfg.faults.idle_timeout_s;
    if (idle_s > 0.0) {
        conn.idle_deadline = std::chrono::steady_clock::now() +
            std::chrono::nanoseconds(static_cast<int64_t>(idle_s * 1e9));
        deadline_heap_.push({conn.idle_deadline, conn.fd});
        rearm_timerfd();
    }
}

void WorkerReactor::set_epoll_interest(int fd, uint32_t events) {
    struct epoll_event ev{};
    ev.data.fd = fd;
    ev.events = events;
    if (events == 0) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    } else {
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
            if (errno == ENOENT) {
                epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
            }
        }
    }
}

void WorkerReactor::close_connection(ConnectionState& conn) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, conn.fd, nullptr);
    close(conn.fd);
    int fd = conn.fd;
    connections_.erase(fd);
}
