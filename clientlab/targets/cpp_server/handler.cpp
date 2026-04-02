#include "handler.hpp"
#include <cctype>
#include <cstring>
#include <ctime>
#include <string>

static char ascii_lower(char ch) {
    return static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
}

static const char* find_case_insensitive(const char* begin, const char* end, const char* needle) {
    size_t needle_len = std::strlen(needle);
    if (needle_len == 0 || static_cast<size_t>(end - begin) < needle_len) return nullptr;
    for (const char* cur = begin; cur + needle_len <= end; ++cur) {
        bool match = true;
        for (size_t i = 0; i < needle_len; ++i) {
            if (ascii_lower(cur[i]) != ascii_lower(needle[i])) {
                match = false;
                break;
            }
        }
        if (match) return cur;
    }
    return nullptr;
}

static bool has_case_insensitive_prefix(const char* begin, const char* end, const char* prefix) {
    size_t prefix_len = std::strlen(prefix);
    if (static_cast<size_t>(end - begin) < prefix_len) return false;
    for (size_t i = 0; i < prefix_len; ++i) {
        if (ascii_lower(begin[i]) != ascii_lower(prefix[i])) return false;
    }
    return true;
}

bool parse_headers(const char* buf, size_t len, ParsedRequest& req, size_t& header_end_offset) {
    req = ParsedRequest{};
    header_end_offset = 0;

    // Need at least "G / H\r\n\r\n" worth of data.
    if (len < 4) return false;

    // Search for end of headers.
    const char* end = static_cast<const char*>(memmem(buf, len, "\r\n\r\n", 4));
    if (!end) return false;
    header_end_offset = static_cast<size_t>(end - buf) + 4;

    // Parse request line.
    if (len >= 4 && strncmp(buf, "GET ", 4) == 0) {
        req.method = ParsedRequest::GET;
        const char* path_start = buf + 4;
        const char* path_end = static_cast<const char*>(memchr(path_start, ' ', len - 4));
        if (path_end) req.path.assign(path_start, path_end);
    } else if (len >= 5 && strncmp(buf, "POST ", 5) == 0) {
        req.method = ParsedRequest::POST;
        const char* path_start = buf + 5;
        const char* path_end = static_cast<const char*>(memchr(path_start, ' ', len - 5));
        if (path_end) req.path.assign(path_start, path_end);
    } else {
        req.method = ParsedRequest::UNKNOWN;
    }

    // Parse Content-Length (case-insensitive).
    const char* cl = find_case_insensitive(buf, end, "Content-Length:");
    if (cl && cl < end) {
        cl += 15;
        while (cl < end && (*cl == ' ' || *cl == '\t')) cl++;
        int content_length = 0;
        while (cl < end && *cl >= '0' && *cl <= '9') {
            content_length = (content_length * 10) + (*cl - '0');
            cl++;
        }
        req.content_length = content_length;
    }

    // Parse Connection header for keep-alive.
    req.keep_alive = true;
    const char* conn = find_case_insensitive(buf, end, "Connection:");
    if (conn && conn < end) {
        conn += 11;
        while (conn < end && (*conn == ' ' || *conn == '\t')) conn++;
        if (has_case_insensitive_prefix(conn, end, "close")) {
            req.keep_alive = false;
        }
    }

    return true;
}

// ---- Response building (unchanged) ----

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

    std::string not_found_body = R"({"error":"not found"})";
    rs.not_found = format_response(404, "Not Found", not_found_body, false);

    return rs;
}

std::string build_metrics_response(const std::string& metrics_json) {
    return format_response(200, "OK", metrics_json, false);
}
