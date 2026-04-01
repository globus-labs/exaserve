#pragma once
#include "config.hpp"
#include <string>

struct ParsedRequest {
    enum Method { GET, POST, UNKNOWN } method = UNKNOWN;
    std::string path;
    int content_length = 0;
    bool keep_alive = true;
};

// Parse HTTP headers from a buffer. Returns true if headers are complete
// (\r\n\r\n found). Fills req with method, path, content_length, keep_alive.
// header_end_offset is set to the byte offset past the \r\n\r\n.
bool parse_headers(const char* buf, size_t len, ParsedRequest& req, size_t& header_end_offset);

// Pre-built complete HTTP response buffers (status line + headers + body).
struct ResponseSet {
    // GET responses
    std::string health;
    std::string models;

    // POST success (two variants: keep-alive vs close)
    std::string chat_ok;
    std::string chat_ok_close;
    std::string completion_ok;
    std::string completion_ok_close;

    // POST error/reject
    std::string error;
    std::string error_close;
    std::string reject;
    std::string reject_close;

    // 404
    std::string not_found;
};

ResponseSet build_responses(const ServerConfig& cfg);

// Build /metrics JSON body at query time (not pre-computed).
std::string build_metrics_response(const std::string& metrics_json);
