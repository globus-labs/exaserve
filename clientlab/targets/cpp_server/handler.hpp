#pragma once
#include "config.hpp"
#include <string>

struct ParsedRequest {
    enum Method { GET, POST, UNKNOWN } method = UNKNOWN;
    std::string path;
    int content_length = 0;
    bool keep_alive = true;
};

// Read and parse one HTTP/1.1 request from fd.
// Returns false on connection close / read error.
bool read_request(int fd, ParsedRequest& req, double recv_timeout_s);

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
};

ResponseSet build_responses(const ServerConfig& cfg);

// Build /metrics JSON body at query time (not pre-computed).
std::string build_metrics_response(const std::string& metrics_json);
std::string build_metrics_response_close(const std::string& metrics_json);

// Write a complete response buffer to fd. Returns false on write error.
bool write_response(int fd, const std::string& response);
