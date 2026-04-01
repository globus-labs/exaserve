#include "config.hpp"
#include "server.hpp"
#include <cstdio>
#include <cstring>
#include <string>

static void usage(const char* prog) {
    std::fprintf(stderr, "Usage: %s --config <json_path>\n", prog);
}

int main(int argc, char* argv[]) {
    std::string config_path;

    for (int i = 1; i < argc; i++) {
        if ((std::strcmp(argv[i], "--config") == 0) && i + 1 < argc) {
            config_path = argv[++i];
        } else if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            usage(argv[0]);
            return 0;
        }
    }

    if (config_path.empty()) {
        usage(argv[0]);
        return 1;
    }

    ServerConfig cfg = load_config(config_path);
    Server server(cfg);
    server.run();
    return 0;
}
