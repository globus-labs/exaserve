#include "config.hpp"
#include "server.hpp"
#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <unistd.h>

static constexpr long kTmpfsMagic = 0x01021994;

static void usage(const char* prog) {
    std::fprintf(stderr,
                 "Usage: %s (--config <json_path> | --config-json <json>) "
                 "[--rank-port-offset] [--require-aurora-local-runtime]\n",
                 prog);
}

static bool root_owned_site_path(const char* path, bool require_directory) {
    char resolved[PATH_MAX];
    if (realpath(path, resolved) == nullptr) {
        std::fprintf(stderr, "Aurora runtime preflight cannot resolve %s: %s\n",
                     path, std::strerror(errno));
        return false;
    }
    const bool approved_root = std::strncmp(resolved, "/etc/", 5) == 0 ||
                               std::strncmp(resolved, "/usr/", 5) == 0;
    struct stat metadata {};
    if (!approved_root || lstat(resolved, &metadata) != 0 ||
        S_ISLNK(metadata.st_mode) || metadata.st_uid != 0 ||
        (metadata.st_mode & 0022) != 0 ||
        (require_directory ? !S_ISDIR(metadata.st_mode) : !S_ISREG(metadata.st_mode))) {
        std::fprintf(stderr, "Aurora runtime preflight rejected site path %s\n", path);
        return false;
    }
    return true;
}

static bool aurora_local_runtime_preflight() {
    const char* home = std::getenv("HOME");
    const char* tmpdir = std::getenv("TMPDIR");
    char cwd[PATH_MAX];
    struct statfs filesystem {};
    if (home == nullptr || std::strcmp(home, "/tmp") != 0 ||
        tmpdir == nullptr || std::strcmp(tmpdir, "/tmp") != 0 ||
        getcwd(cwd, sizeof(cwd)) == nullptr || std::strcmp(cwd, "/tmp") != 0 ||
        statfs("/tmp", &filesystem) != 0 || filesystem.f_type != kTmpfsMagic) {
        std::fprintf(stderr,
                     "Aurora runtime preflight requires HOME/TMPDIR/cwd on tmpfs /tmp\n");
        return false;
    }
    if (!root_owned_site_path("/etc/pmix-mca-params.conf", false) ||
        !root_owned_site_path("/usr/lib64/pmix", true)) {
        return false;
    }
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        std::fprintf(stderr, "Aurora runtime preflight cannot identify host\n");
        return false;
    }
    hostname[sizeof(hostname) - 1] = '\0';
    std::printf("CLIENTLAB_LOCAL_PREFLIGHT host=%s tmpfs=ok pmix=ok\n", hostname);
    std::fflush(stdout);
    return true;
}

static int process_rank() {
    const char* names[] = {
        "PALS_RANKID", "PMI_RANK", "SLURM_PROCID", "OMPI_COMM_WORLD_RANK"
    };
    for (const char* name : names) {
        const char* value = std::getenv(name);
        if (value == nullptr || *value == '\0') continue;
        errno = 0;
        char* end = nullptr;
        long parsed = std::strtol(value, &end, 10);
        if (errno != 0 || end == value || *end != '\0' || parsed < 0 || parsed > INT_MAX) {
            std::fprintf(stderr, "Invalid %s rank: %s\n", name, value);
            std::exit(1);
        }
        return static_cast<int>(parsed);
    }
    std::fprintf(stderr, "Rank port offset requested without an MPI rank identity\n");
    std::exit(1);
}

int main(int argc, char* argv[]) {
    std::string config_path;
    std::string config_json;
    bool rank_port_offset = false;
    bool require_aurora_local_runtime = false;

    for (int i = 1; i < argc; i++) {
        if ((std::strcmp(argv[i], "--config") == 0) && i + 1 < argc) {
            config_path = argv[++i];
        } else if ((std::strcmp(argv[i], "--config-json") == 0) && i + 1 < argc) {
            config_json = argv[++i];
        } else if (std::strcmp(argv[i], "--rank-port-offset") == 0) {
            rank_port_offset = true;
        } else if (std::strcmp(argv[i], "--require-aurora-local-runtime") == 0) {
            require_aurora_local_runtime = true;
        } else if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    if (config_path.empty() == config_json.empty()) {
        usage(argv[0]);
        return 1;
    }

    if (require_aurora_local_runtime && !aurora_local_runtime_preflight()) return 1;

    ServerConfig cfg = config_path.empty() ? load_config_json(config_json) : load_config(config_path);
    if (rank_port_offset) {
        const int rank = process_rank();
        if (cfg.port < 1 || rank > 65535 - cfg.port) {
            std::fprintf(stderr, "Rank-adjusted target port is outside 1..65535\n");
            return 1;
        }
        cfg.port += rank;
    }
    Server server(cfg);
    return server.run();
}
