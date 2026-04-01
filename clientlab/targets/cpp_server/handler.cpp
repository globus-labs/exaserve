#include "handler.hpp"
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

static constexpr size_t MAX_HEADER_SIZE = 8192;

bool read_request(int fd, ParsedRequest& req, double recv_timeout_s) {
    req = ParsedRequest{};
    char buf[MAX_HEADER_SIZE];
    size_t total = 0;
    const char* header_end = nullptr;

    // Set recv timeout if configured.
    if (recv_timeout_s > 0.0) {
        struct timeval tv;
        tv.tv_sec = static_cast<long>(recv_timeout_s);
        tv.tv_usec = static_cast<long>((recv_timeout_s - tv.tv_sec) * 1e6);
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    while (total < MAX_HEADER_SIZE) {
        ssize_t n = recv(fd, buf + total, MAX_HEADER_SIZE - total, 0);
        if (n <= 0) return false;
        total += static_cast<size_t>(n);
        buf[total] = '\0';
        header_end = strstr(buf, "\r\n\r\n");
        if (header_end) break;
    }
    if (!header_end) return false;

    // Parse request line: "METHOD /path HTTP/1.1\r\n"
    if (strncmp(buf, "GET ", 4) == 0) {
        req.method = ParsedRequest::GET;
        const char* path_start = buf + 4;
        const char* path_end = strchr(path_start, ' ');
        if (path_end) req.path.assign(path_start, path_end);
    } else if (strncmp(buf, "POST ", 5) == 0) {
        req.method = ParsedRequest::POST;
        const char* path_start = buf + 5;
        const char* path_end = strchr(path_start, ' ');
        if (path_end) req.path.assign(path_start, path_end);
    } else {
        req.method = ParsedRequest::UNKNOWN;
        return true;
    }

    // Parse Content-Length.
    const char* cl = strcasestr(buf, "Content-Length:");
    if (cl) {
        cl += 15;
        while (*cl == ' ') cl++;
        req.content_length = atoi(cl);
    }

    // Parse Connection header for keep-alive.
    req.keep_alive = true;
    const char* conn = strcasestr(buf, "Connection:");
    if (conn) {
        conn += 11;
        while (*conn == ' ') conn++;
        if (strncasecmp(conn, "close", 5) == 0) {
            req.keep_alive = false;
        }
    }

    // Drain request body if present.
    if (req.content_length > 0) {
        size_t headers_len = static_cast<size_t>(header_end - buf) + 4;
        size_t body_already = total - headers_len;
        size_t remaining = static_cast<size_t>(req.content_length) - body_already;
        char drain[4096];
        while (remaining > 0) {
            ssize_t n = recv(fd, drain, std::min(remaining, sizeof(drain)), 0);
            if (n <= 0) return false;
            remaining -= static_cast<size_t>(n);
        }
    }

    return true;
}

static std::string format_response(int status_code, const char* status_text,
                                   const std::string& body, bool conn_close) {
    std::string r;
    r.reserve(256 + body.size());
    r += "HTTP/1.1 ";
    r += std::to_string(status_code);
    r += ' ';
    r += status_text;
    r += "\r\n";
    if (conn_close) {
        r += "Connection: close\r\n";
    }
    r += "Content-Type: application/json\r\n";
    r += "Content-Length: ";
    r += std::to_string(body.size());
    r += "\r\n\r\n";
    r += body;
    return r;
}

static std::string make_token_text(int count) {
    std::string s;
    for (int i = 0; i < std::max(count, 1); i++) {
        if (i > 0) s += ' ';
        s += "token";
    }
    return s;
}

static std::string make_chat_json(const ServerConfig& cfg) {
    std::string text = make_token_text(cfg.response_tokens);
    int total = cfg.prompt_words + cfg.response_tokens;
    std::string s;
    s += R"({"id":"chatcmpl-clientlab","object":"chat.completion","created":)";
    s += std::to_string(std::time(nullptr));
    s += R"(,"model":")";
    s += cfg.model;
    s += R"(","choices":[{"index":0,"message":{"role":"assistant","content":")";
    s += text;
    s += R"("},"finish_reason":"stop"}],"usage":{"prompt_tokens":)";
    s += std::to_string(cfg.prompt_words);
    s += R"(,"completion_tokens":)";
    s += std::to_string(cfg.response_tokens);
    s += R"(,"total_tokens":)";
    s += std::to_string(total);
    s += "}}";
    return s;
}

static std::string make_completion_json(const ServerConfig& cfg) {
    std::string text = make_token_text(cfg.response_tokens);
    int total = cfg.prompt_words + cfg.response_tokens;
    std::string s;
    s += R"({"id":"cmpl-clientlab","object":"text_completion","created":)";
    s += std::to_string(std::time(nullptr));
    s += R"(,"model":")";
    s += cfg.model;
    s += R"(","choices":[{"text":")";
    s += text;
    s += R"(","index":0,"finish_reason":"stop","logprobs":null}],"usage":{"prompt_tokens":)";
    s += std::to_string(cfg.prompt_words);
    s += R"(,"completion_tokens":)";
    s += std::to_string(cfg.response_tokens);
    s += R"(,"total_tokens":)";
    s += std::to_string(total);
    s += "}}";
    return s;
}

ResponseSet build_responses(const ServerConfig& cfg) {
    ResponseSet rs;

    std::string health_body = R"({"status":"ok"})";
    rs.health = format_response(200, "OK", health_body, false);

    std::string models_body = R"({"object":"list","data":[{"id":")";
    models_body += cfg.model;
    models_body += R"(","object":"model"}]})";
    rs.models = format_response(200, "OK", models_body, false);

    std::string chat_body = make_chat_json(cfg);
    rs.chat_ok = format_response(200, "OK", chat_body, false);
    rs.chat_ok_close = format_response(200, "OK", chat_body, true);

    std::string comp_body = make_completion_json(cfg);
    rs.completion_ok = format_response(200, "OK", comp_body, false);
    rs.completion_ok_close = format_response(200, "OK", comp_body, true);

    std::string error_body = R"({"error":"injected"})";
    rs.error = format_response(cfg.faults.error_status, "Internal Server Error", error_body, false);
    rs.error_close = format_response(cfg.faults.error_status, "Internal Server Error", error_body, true);

    std::string reject_body = R"({"error":"capacity rejected"})";
    rs.reject = format_response(cfg.faults.reject_status, "Too Many Requests", reject_body, false);
    rs.reject_close = format_response(cfg.faults.reject_status, "Too Many Requests", reject_body, true);

    return rs;
}

std::string build_metrics_response(const std::string& metrics_json) {
    return format_response(200, "OK", metrics_json, false);
}

std::string build_metrics_response_close(const std::string& metrics_json) {
    return format_response(200, "OK", metrics_json, true);
}

bool write_response(int fd, const std::string& response) {
    const char* data = response.data();
    size_t remaining = response.size();
    while (remaining > 0) {
        ssize_t n = send(fd, data, remaining, MSG_NOSIGNAL);
        if (n <= 0) return false;
        data += n;
        remaining -= static_cast<size_t>(n);
    }
    return true;
}
