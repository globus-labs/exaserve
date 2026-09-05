#include "config.hpp"
#include "vendor/yyjson.h"
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

static std::string read_file(const std::string& path) {
    std::ifstream f(path);
    if (!f.is_open()) {
        std::fprintf(stderr, "Cannot open config: %s\n", path.c_str());
        std::exit(1);
    }
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

static const char* yy_str(yyjson_val* obj, const char* key, const char* def) {
    yyjson_val* v = yyjson_obj_get(obj, key);
    return (v && yyjson_is_str(v)) ? yyjson_get_str(v) : def;
}

static double yy_num(yyjson_val* obj, const char* key, double def) {
    yyjson_val* v = yyjson_obj_get(obj, key);
    if (!v) return def;
    if (yyjson_is_real(v)) return yyjson_get_real(v);
    if (yyjson_is_int(v)) return static_cast<double>(yyjson_get_int(v));
    if (yyjson_is_sint(v)) return static_cast<double>(yyjson_get_sint(v));
    return def;
}

static int yy_int(yyjson_val* obj, const char* key, int def) {
    yyjson_val* v = yyjson_obj_get(obj, key);
    if (!v) return def;
    if (yyjson_is_int(v)) return static_cast<int>(yyjson_get_int(v));
    if (yyjson_is_sint(v)) return static_cast<int>(yyjson_get_sint(v));
    if (yyjson_is_real(v)) return static_cast<int>(yyjson_get_real(v));
    return def;
}

static bool yy_bool(yyjson_val* obj, const char* key, bool def) {
    yyjson_val* v = yyjson_obj_get(obj, key);
    return (v && yyjson_is_bool(v)) ? yyjson_get_bool(v) : def;
}

ServerConfig load_config_json(const std::string& data) {
    yyjson_doc* doc = yyjson_read(data.c_str(), data.size(), 0);
    if (!doc) {
        std::fprintf(stderr, "Failed to parse JSON config\n");
        std::exit(1);
    }
    yyjson_val* root = yyjson_doc_get_root(doc);
    if (!root || !yyjson_is_obj(root)) {
        std::fprintf(stderr, "Synthetic target config root must be an object\n");
        yyjson_doc_free(doc);
        std::exit(1);
    }

    ServerConfig cfg;

    yyjson_val* target = yyjson_obj_get(root, "target");
    if (target) {
        cfg.host = yy_str(target, "host", "127.0.0.1");
        cfg.port = yy_int(target, "port", 18100);
        cfg.response_tokens = yy_int(target, "response_tokens", 32);
    }

    yyjson_val* client = yyjson_obj_get(root, "client");
    if (client) {
        cfg.model = yy_str(client, "model", "stub-model");
        cfg.prompt_words = yy_int(client, "prompt_words", 32);
        cfg.client_max_active = yy_int(client, "max_active_requests", 0);
    }

    yyjson_val* faults = yyjson_obj_get(root, "faults");
    if (faults) {
        yyjson_val* st = yyjson_obj_get(faults, "service_time");
        if (st) {
            cfg.faults.service_time.distribution = yy_str(st, "distribution", "fixed");
            cfg.faults.service_time.value_ms = yy_num(st, "value_ms", 0.0);
            cfg.faults.service_time.stddev_ms = yy_num(st, "stddev_ms", 0.0);
        }
        cfg.faults.max_inflight = yy_int(faults, "max_inflight", 0);
        cfg.faults.max_queue = yy_int(faults, "max_queue", 0);
        cfg.faults.queue_delay_ms = yy_num(faults, "queue_delay_ms", 0.0);
        cfg.faults.error_rate = yy_num(faults, "error_rate", 0.0);
        cfg.faults.error_status = yy_int(faults, "error_status", 500);
        cfg.faults.reject_status = yy_int(faults, "reject_status", 429);
        cfg.faults.close_after_response = yy_bool(faults, "close_after_response", false);
        cfg.faults.reset_after_response = yy_bool(faults, "reset_after_response", false);
        cfg.faults.idle_timeout_s = yy_num(faults, "idle_timeout_s", 0.0);
        cfg.faults.burst_every = yy_int(faults, "burst_every", 0);
        cfg.faults.burst_duration = yy_int(faults, "burst_duration", 0);
    }

    if (cfg.host.empty() || cfg.port < 1 || cfg.port > 65535 ||
        cfg.response_tokens < 0 || cfg.client_max_active < 0) {
        std::fprintf(stderr, "Synthetic target config contains invalid bounds\n");
        yyjson_doc_free(doc);
        std::exit(1);
    }

    yyjson_doc_free(doc);
    return cfg;
}

ServerConfig load_config(const std::string& path) {
    return load_config_json(read_file(path));
}
