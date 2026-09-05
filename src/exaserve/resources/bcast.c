#define _POSIX_C_SOURCE 200809L

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <mpi.h>
#include <regex.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/* A bounded buffer avoids aggregate memory growth at scale. */
#define BUFFER_SIZE (64L << 20)
#define MAX_WALK_DEPTH 1024

enum entry_kind {
    ENTRY_END = 0,
    ENTRY_DIRECTORY_BEGIN = 1,
    ENTRY_DIRECTORY_END = 2,
    ENTRY_FILE = 3
};

struct walk_context {
    MPI_Comm communicator;
    void *buffer;
    const char *destination;
    int write_root;
    unsigned long long total_bytes;
    dev_t ancestor_devices[MAX_WALK_DEPTH];
    ino_t ancestor_inodes[MAX_WALK_DEPTH];
};

static double get_elapsed(struct timespec t1, struct timespec t2) {
    time_t sec = t2.tv_sec - t1.tv_sec;
    long nsec = t2.tv_nsec - t1.tv_nsec;
    if (nsec < 0) {
        sec--;
        nsec += 1000000000L;
    }
    return sec + nsec * 1e-9;
}

static void fatal(const char *message) {
    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    fprintf(stderr, "bcast rank %d: %s: %s\n", rank, message, strerror(errno));
    MPI_Abort(MPI_COMM_WORLD, 1);
}

static void protocol_fatal(const char *message) {
    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    fprintf(stderr, "bcast rank %d: %s\n", rank, message);
    MPI_Abort(MPI_COMM_WORLD, 1);
}

static int same_node(const char *left, const char *right) {
    size_t left_len = strcspn(left, ".");
    size_t right_len = strcspn(right, ".");
    return left_len > 0 && left_len == right_len &&
           strncasecmp(left, right, left_len) == 0;
}

static int parse_positive_int(const char *value, int *result) {
    char *end = NULL;
    errno = 0;
    long parsed = strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < 1 ||
        parsed > 2147483647L) {
        return -1;
    }
    *result = (int)parsed;
    return 0;
}

static int parse_recipients(const char *value, int world_size,
                            unsigned char *bitmap) {
    if (value == NULL) {
        memset(bitmap, 1, (size_t)world_size);
        return world_size;
    }
    if (*value == '\0') return -1;
    char *copy = strdup(value);
    if (copy == NULL) return -1;
    int count = 0;
    char *cursor = copy;
    while (*cursor != '\0') {
        char *comma = strchr(cursor, ',');
        if (comma != NULL) *comma = '\0';
        char *end = NULL;
        errno = 0;
        long parsed = strtol(cursor, &end, 10);
        if (errno != 0 || end == cursor || *end != '\0' || parsed < 0 ||
            parsed >= world_size || bitmap[parsed]) {
            free(copy);
            return -1;
        }
        bitmap[parsed] = 1;
        count++;
        if (comma == NULL) break;
        cursor = comma + 1;
        if (*cursor == '\0') {
            free(copy);
            return -1;
        }
    }
    free(copy);
    return count;
}

static int mkdir_p(const char *path, mode_t mode) {
    char *copy = strdup(path);
    if (copy == NULL || copy[0] != '/') {
        free(copy);
        errno = EINVAL;
        return -1;
    }
    size_t length = strlen(copy);
    while (length > 1 && copy[length - 1] == '/') copy[--length] = '\0';
    for (char *cursor = copy + 1; *cursor != '\0'; cursor++) {
        if (*cursor != '/') continue;
        *cursor = '\0';
        if (mkdir(copy, mode) != 0 && errno != EEXIST) {
            free(copy);
            return -1;
        }
        struct stat metadata;
        if (lstat(copy, &metadata) != 0 || !S_ISDIR(metadata.st_mode) ||
            S_ISLNK(metadata.st_mode)) {
            free(copy);
            errno = ENOTDIR;
            return -1;
        }
        *cursor = '/';
    }
    if (mkdir(copy, mode) != 0 && errno != EEXIST) {
        free(copy);
        return -1;
    }
    struct stat metadata;
    int valid = lstat(copy, &metadata) == 0 && S_ISDIR(metadata.st_mode) &&
                !S_ISLNK(metadata.st_mode);
    free(copy);
    if (!valid) {
        errno = ENOTDIR;
        return -1;
    }
    return 0;
}

static int safe_relative_path(const char *path) {
    if (path == NULL || path[0] == '\0' || path[0] == '/' ||
        strstr(path, "//") != NULL) {
        return 0;
    }
    const char *cursor = path;
    while (*cursor != '\0') {
        const char *slash = strchr(cursor, '/');
        size_t length = slash == NULL ? strlen(cursor) : (size_t)(slash - cursor);
        if (length == 0 || (length == 1 && cursor[0] == '.') ||
            (length == 2 && cursor[0] == '.' && cursor[1] == '.')) {
            return 0;
        }
        if (slash == NULL) break;
        cursor = slash + 1;
    }
    return 1;
}

static char *join_path(const char *left, const char *right) {
    size_t left_length = strlen(left);
    size_t right_length = strlen(right);
    if (left_length > SIZE_MAX - right_length - 2) return NULL;
    char *result = malloc(left_length + right_length + 2);
    if (result == NULL) return NULL;
    memcpy(result, left, left_length);
    result[left_length] = '/';
    memcpy(result + left_length + 1, right, right_length + 1);
    return result;
}

static int safe_absolute_path(const char *path) {
    if (path == NULL || path[0] != '/' || path[1] == '\0' ||
        strstr(path, "//") != NULL) {
        return 0;
    }
    const char *cursor = path + 1;
    while (*cursor != '\0') {
        const char *slash = strchr(cursor, '/');
        size_t length = slash == NULL ? strlen(cursor) : (size_t)(slash - cursor);
        if (length == 0 || (length == 1 && cursor[0] == '.') ||
            (length == 2 && cursor[0] == '.' && cursor[1] == '.')) {
            return 0;
        }
        if (slash == NULL) break;
        cursor = slash + 1;
    }
    return 1;
}

static int remove_tree(const char *path) {
    struct stat metadata;
    if (lstat(path, &metadata) != 0) return errno == ENOENT ? 0 : -1;
    if (metadata.st_uid != getuid()) {
        errno = EPERM;
        return -1;
    }
    if (!S_ISDIR(metadata.st_mode) || S_ISLNK(metadata.st_mode)) return unlink(path);
    if (chmod(path, 0700) != 0) return -1;
    struct dirent **entries = NULL;
    int count = scandir(path, &entries, NULL, alphasort);
    if (count < 0) return -1;
    int failed = 0;
    for (int index = 0; index < count; index++) {
        const char *name = entries[index]->d_name;
        if (!failed && strcmp(name, ".") != 0 && strcmp(name, "..") != 0) {
            char *child = join_path(path, name);
            if (child == NULL || remove_tree(child) != 0) failed = 1;
            free(child);
        }
        free(entries[index]);
    }
    free(entries);
    if (failed) return -1;
    return rmdir(path);
}

static int matches_pattern(const char *value, const char *pattern) {
    regex_t expression;
    if (regcomp(&expression, pattern, REG_EXTENDED | REG_NOSUB) != 0) return 0;
    int matches = regexec(&expression, value, 0, NULL, 0) == 0;
    regfree(&expression);
    return matches;
}

static int valid_cleanup_candidate(const char *root, const char *candidate) {
    if (!safe_absolute_path(root) || !safe_absolute_path(candidate) ||
        strcmp(root, candidate) == 0 || strcmp(root, "/home") == 0 ||
        strncmp(root, "/home/", 6) == 0 || strcmp(root, "/lus/flare") == 0 ||
        strncmp(root, "/lus/flare/", 11) == 0 || strcmp(candidate, "/home") == 0 ||
        strncmp(candidate, "/home/", 6) == 0 ||
        strcmp(candidate, "/lus/flare") == 0 ||
        strncmp(candidate, "/lus/flare/", 11) == 0) {
        return 0;
    }
    size_t root_length = strlen(root);
    if (strncmp(root, candidate, root_length) != 0 || candidate[root_length] != '/') return 0;
    const char *relative = candidate + root_length + 1;
    const char *patterns[] = {
        "^candidates/g[0-9]+/source\\.[0-9a-f]{32}$",
        "^\\.exaserve_stage\\.[A-Za-z0-9_.-]+\\.[0-9]+\\.[0-9a-f]{32}$",
        "^\\.exaserve_pp_candidate\\.[A-Za-z0-9_.-]+\\.[0-9]+\\.[0-9a-f]{32}\\.stage[0-9]+$",
    };
    for (size_t index = 0; index < sizeof(patterns) / sizeof(patterns[0]); index++) {
        if (matches_pattern(relative, patterns[index])) return 1;
    }
    return 0;
}

static int validate_cleanup_chain(const char *root, const char *candidate,
                                  const struct stat *root_metadata) {
    size_t root_length = strlen(root);
    char *copy = strdup(candidate);
    if (copy == NULL) return 0;
    char *cursor = copy + root_length + 1;
    while (*cursor != '\0') {
        char *slash = strchr(cursor, '/');
        if (slash != NULL) *slash = '\0';
        struct stat metadata;
        if (lstat(copy, &metadata) != 0) {
            int missing = errno == ENOENT;
            free(copy);
            return missing;
        }
        if (S_ISLNK(metadata.st_mode) || metadata.st_dev != root_metadata->st_dev ||
            metadata.st_uid != getuid() ||
            (slash != NULL && !S_ISDIR(metadata.st_mode))) {
            free(copy);
            errno = EPERM;
            return 0;
        }
        if (slash == NULL) break;
        *slash = '/';
        cursor = slash + 1;
    }
    free(copy);
    return 1;
}

static void broadcast_header(MPI_Comm communicator, int kind, const char *path,
                             unsigned long long size, unsigned int mode) {
    size_t raw_path_length = path == NULL ? 0 : strlen(path);
    if (raw_path_length > INT_MAX) protocol_fatal("streamed path exceeds MPI count limit");
    int path_length = (int)raw_path_length;
    MPI_Bcast(&kind, 1, MPI_INT, 0, communicator);
    MPI_Bcast(&path_length, 1, MPI_INT, 0, communicator);
    MPI_Bcast(&size, 1, MPI_UNSIGNED_LONG_LONG, 0, communicator);
    MPI_Bcast(&mode, 1, MPI_UNSIGNED, 0, communicator);
    if (path_length > 0) {
        MPI_Bcast((void *)path, path_length, MPI_BYTE, 0, communicator);
    }
}

static int open_destination_file(const char *destination, const char *relative) {
    if (!safe_relative_path(relative)) protocol_fatal("unsafe relative path in stream");
    char *path = join_path(destination, relative);
    if (path == NULL) fatal("destination path allocation failed");
    char *parent = strdup(path);
    if (parent == NULL) fatal("destination parent allocation failed");
    char *slash = strrchr(parent, '/');
    if (slash == NULL || slash == parent) protocol_fatal("invalid destination parent");
    *slash = '\0';
    if (mkdir_p(parent, 0700) != 0) fatal("creating destination parent failed");
    free(parent);
    int descriptor = open(path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
    free(path);
    if (descriptor < 0) fatal("creating destination file failed");
    return descriptor;
}

static void write_all(int descriptor, const void *buffer, size_t size) {
    const char *cursor = buffer;
    size_t remaining = size;
    while (remaining > 0) {
        ssize_t written = write(descriptor, cursor, remaining);
        if (written < 0) {
            if (errno == EINTR) continue;
            fatal("writing destination file failed");
        }
        if (written == 0) protocol_fatal("zero-byte destination write");
        cursor += written;
        remaining -= (size_t)written;
    }
}

static void emit_file(struct walk_context *context, const char *source,
                      const char *relative, const struct stat *metadata) {
    int input = open(source, O_RDONLY | O_CLOEXEC);
    if (input < 0) fatal("opening source file failed");
    struct stat opened;
    if (fstat(input, &opened) != 0 || !S_ISREG(opened.st_mode) ||
        opened.st_dev != metadata->st_dev || opened.st_ino != metadata->st_ino ||
        opened.st_size != metadata->st_size || opened.st_size < 0) {
        close(input);
        protocol_fatal("source file changed during staging");
    }
    unsigned long long size = (unsigned long long)opened.st_size;
    broadcast_header(context->communicator, ENTRY_FILE, relative, size,
                     (unsigned int)(opened.st_mode & 07777));
    int output = -1;
    if (context->write_root) output = open_destination_file(context->destination, relative);
    unsigned long long remaining = size;
    while (remaining > 0) {
        size_t wanted = remaining < BUFFER_SIZE ? (size_t)remaining : (size_t)BUFFER_SIZE;
        size_t filled = 0;
        while (filled < wanted) {
            ssize_t count = read(input, (char *)context->buffer + filled, wanted - filled);
            if (count < 0) {
                if (errno == EINTR) continue;
                fatal("reading source file failed");
            }
            if (count == 0) protocol_fatal("source file truncated during staging");
            filled += (size_t)count;
        }
        MPI_Bcast(context->buffer, (int)wanted, MPI_BYTE, 0, context->communicator);
        if (output >= 0) write_all(output, context->buffer, wanted);
        remaining -= wanted;
        context->total_bytes += wanted;
    }
    struct stat final_source;
    if (fstat(input, &final_source) != 0 || final_source.st_size != opened.st_size ||
        final_source.st_mtim.tv_sec != opened.st_mtim.tv_sec ||
        final_source.st_mtim.tv_nsec != opened.st_mtim.tv_nsec ||
        final_source.st_ctim.tv_sec != opened.st_ctim.tv_sec ||
        final_source.st_ctim.tv_nsec != opened.st_ctim.tv_nsec) {
        close(input);
        protocol_fatal("source file changed while staging");
    }
    if (close(input) != 0) fatal("closing source file failed");
    if (output >= 0) {
        if (fchmod(output, opened.st_mode & 07777) != 0 || fsync(output) != 0 ||
            close(output) != 0) {
            fatal("publishing destination file failed");
        }
    }
}

static void emit_entry(struct walk_context *context, const char *source,
                       const char *relative, int depth) {
    if (depth >= MAX_WALK_DEPTH) protocol_fatal("source tree exceeds maximum depth");
    struct stat metadata;
    if (stat(source, &metadata) != 0) fatal("stating source entry failed");
    if (S_ISREG(metadata.st_mode)) {
        emit_file(context, source, relative, &metadata);
        return;
    }
    if (!S_ISDIR(metadata.st_mode)) protocol_fatal("source contains unsupported entry");
    for (int index = 0; index < depth; index++) {
        if (context->ancestor_devices[index] == metadata.st_dev &&
            context->ancestor_inodes[index] == metadata.st_ino) {
            protocol_fatal("source directory contains a symlink cycle");
        }
    }
    context->ancestor_devices[depth] = metadata.st_dev;
    context->ancestor_inodes[depth] = metadata.st_ino;
    broadcast_header(context->communicator, ENTRY_DIRECTORY_BEGIN, relative, 0,
                     (unsigned int)(metadata.st_mode & 07777));
    if (context->write_root) {
        char *destination = join_path(context->destination, relative);
        if (destination == NULL || mkdir_p(destination, 0700) != 0) {
            free(destination);
            fatal("creating destination directory failed");
        }
        free(destination);
    }
    struct dirent **entries = NULL;
    int count = scandir(source, &entries, NULL, alphasort);
    if (count < 0) fatal("listing source directory failed");
    for (int index = 0; index < count; index++) {
        const char *name = entries[index]->d_name;
        if (strcmp(name, ".") != 0 && strcmp(name, "..") != 0) {
            char *child_source = join_path(source, name);
            char *child_relative = join_path(relative, name);
            if (child_source == NULL || child_relative == NULL) {
                protocol_fatal("source path allocation failed");
            }
            emit_entry(context, child_source, child_relative, depth + 1);
            free(child_source);
            free(child_relative);
        }
        free(entries[index]);
    }
    free(entries);
    broadcast_header(context->communicator, ENTRY_DIRECTORY_END, relative, 0,
                     (unsigned int)(metadata.st_mode & 07777));
    if (context->write_root) {
        char *destination = join_path(context->destination, relative);
        if (destination == NULL || chmod(destination, metadata.st_mode & 07777) != 0) {
            free(destination);
            fatal("finalizing destination directory failed");
        }
        free(destination);
    }
}

static void receive_stream(MPI_Comm communicator, void *buffer,
                           const char *destination) {
    while (1) {
        int kind = -1;
        int path_length = 0;
        unsigned long long size = 0;
        unsigned int mode = 0;
        MPI_Bcast(&kind, 1, MPI_INT, 0, communicator);
        MPI_Bcast(&path_length, 1, MPI_INT, 0, communicator);
        MPI_Bcast(&size, 1, MPI_UNSIGNED_LONG_LONG, 0, communicator);
        MPI_Bcast(&mode, 1, MPI_UNSIGNED, 0, communicator);
        if (kind == ENTRY_END) {
            if (path_length != 0 || size != 0) protocol_fatal("invalid end record");
            break;
        }
        if (path_length < 1 || path_length > INT_MAX - 1) {
            protocol_fatal("invalid streamed path length");
        }
        char *relative = malloc((size_t)path_length + 1);
        if (relative == NULL) fatal("streamed path allocation failed");
        MPI_Bcast(relative, path_length, MPI_BYTE, 0, communicator);
        relative[path_length] = '\0';
        if (!safe_relative_path(relative)) protocol_fatal("unsafe streamed relative path");
        char *path = join_path(destination, relative);
        if (path == NULL) fatal("destination path allocation failed");
        if (kind == ENTRY_DIRECTORY_BEGIN) {
            if (size != 0 || mkdir_p(path, 0700) != 0) {
                free(path);
                free(relative);
                fatal("creating streamed directory failed");
            }
        } else if (kind == ENTRY_DIRECTORY_END) {
            if (size != 0 || chmod(path, (mode_t)mode) != 0) {
                free(path);
                free(relative);
                fatal("finalizing streamed directory failed");
            }
        } else if (kind == ENTRY_FILE) {
            int output = open_destination_file(destination, relative);
            unsigned long long remaining = size;
            while (remaining > 0) {
                size_t wanted = remaining < BUFFER_SIZE ? (size_t)remaining : (size_t)BUFFER_SIZE;
                MPI_Bcast(buffer, (int)wanted, MPI_BYTE, 0, communicator);
                write_all(output, buffer, wanted);
                remaining -= wanted;
            }
            if (fchmod(output, (mode_t)mode) != 0 || fsync(output) != 0 ||
                close(output) != 0) {
                free(path);
                free(relative);
                fatal("publishing streamed file failed");
            }
        } else {
            free(path);
            free(relative);
            protocol_fatal("unknown stream entry kind");
        }
        free(path);
        free(relative);
    }
}

int main(int argc, char **argv) {
    struct timespec start, end;
    int rank = -1;
    int world_size = 0;
    int no_root_write = 0;
    int expected_world_size = -1;
    const char *expected_root_host = NULL;
    const char *recipient_text = NULL;
    const char *cleanup_path = NULL;
    const char *cleanup_root = NULL;
    int positional[2];
    int positional_count = 0;

    for (int index = 1; index < argc; index++) {
        if (strcmp(argv[index], "--no-root-write") == 0) {
            no_root_write = 1;
        } else if (strcmp(argv[index], "--expected-root-host") == 0 && index + 1 < argc) {
            expected_root_host = argv[++index];
        } else if (strcmp(argv[index], "--expected-world-size") == 0 && index + 1 < argc) {
            if (parse_positive_int(argv[++index], &expected_world_size) != 0) return 1;
        } else if (strcmp(argv[index], "--recipients") == 0 && index + 1 < argc) {
            recipient_text = argv[++index];
        } else if (strcmp(argv[index], "--cleanup") == 0 && index + 1 < argc) {
            cleanup_path = argv[++index];
        } else if (strcmp(argv[index], "--cleanup-root") == 0 && index + 1 < argc) {
            cleanup_root = argv[++index];
        } else {
            if (positional_count < 2) positional[positional_count] = index;
            positional_count++;
        }
    }

    MPI_Init(NULL, NULL);
    clock_gettime(CLOCK_MONOTONIC, &start);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    if ((positional_count < 1 && cleanup_path == NULL) || expected_root_host == NULL ||
        expected_world_size < 1) {
        if (rank == 0) {
            fprintf(stderr,
                    "Usage: bcast --expected-root-host HOST --expected-world-size N "
                    "[--recipients RANK,...] [--no-root-write] <src> [dest] "
                    "| --cleanup-root /ABS/LOCAL --cleanup /ABS/CANDIDATE\n");
        }
        MPI_Finalize();
        return 1;
    }
    if (world_size != expected_world_size) protocol_fatal("MPI world size mismatch");
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) fatal("gethostname failed");
    hostname[sizeof(hostname) - 1] = '\0';
    if (rank == 0 && !same_node(hostname, expected_root_host)) {
        protocol_fatal("global rank 0 is not the allocation head");
    }

    unsigned char *recipient_bitmap = calloc((size_t)world_size, 1);
    if (recipient_bitmap == NULL) fatal("recipient bitmap allocation failed");
    int recipient_count = parse_recipients(recipient_text, world_size, recipient_bitmap);
    if (recipient_count < 1) protocol_fatal("recipient set is invalid or empty");
    if (cleanup_path != NULL) {
        if (cleanup_root == NULL || !valid_cleanup_candidate(cleanup_root, cleanup_path)) {
            protocol_fatal("cleanup root/candidate is not an owned transaction path");
        }
        int local_cleanup_failure = 0;
        if (recipient_bitmap[rank]) {
            struct stat root_metadata;
            if (lstat(cleanup_root, &root_metadata) != 0 ||
                !S_ISDIR(root_metadata.st_mode) || S_ISLNK(root_metadata.st_mode) ||
                root_metadata.st_uid != getuid() ||
                !validate_cleanup_chain(cleanup_root, cleanup_path, &root_metadata) ||
                remove_tree(cleanup_path) != 0) {
                local_cleanup_failure = 1;
            }
        }
        int global_cleanup_failure = 0;
        MPI_Allreduce(&local_cleanup_failure, &global_cleanup_failure, 1, MPI_INT,
                      MPI_MAX, MPI_COMM_WORLD);
        if (rank == 0) {
            printf("bcast: cleanup %s on %d recipient rank(s): %s\n", cleanup_path,
                   recipient_count, global_cleanup_failure ? "FAILED" : "ok");
        }
        free(recipient_bitmap);
        MPI_Finalize();
        return global_cleanup_failure ? 1 : 0;
    }
    int participates = rank == 0 || recipient_bitmap[rank];
    MPI_Comm transfer_communicator = MPI_COMM_NULL;
    MPI_Comm_split(MPI_COMM_WORLD, participates ? 1 : MPI_UNDEFINED, rank,
                   &transfer_communicator);
    int transfer_rank = -1;
    if (participates) MPI_Comm_rank(transfer_communicator, &transfer_rank);
    if (rank == 0 && transfer_rank != 0) protocol_fatal("global rank 0 is not transfer root");

    char *source = strdup(argv[positional[0]]);
    char *destination = positional_count >= 2 ? strdup(argv[positional[1]]) : strdup("/tmp");
    if (source == NULL || destination == NULL || destination[0] != '/') {
        protocol_fatal("source/destination values are invalid");
    }
    size_t source_length = strlen(source);
    while (source_length > 1 && source[source_length - 1] == '/') {
        source[--source_length] = '\0';
    }
    const char *basename = strrchr(source, '/');
    basename = basename == NULL ? source : basename + 1;
    if (!safe_relative_path(basename)) protocol_fatal("source basename is unsafe");

    int writes = recipient_bitmap[rank] && !(rank == 0 && no_root_write);
    if (writes && mkdir_p(destination, 0700) != 0) fatal("destination creation failed");
    void *buffer = participates ? malloc(BUFFER_SIZE) : NULL;
    if (participates && buffer == NULL) fatal("transfer buffer allocation failed");
    unsigned long long total_bytes = 0;
    if (transfer_rank == 0) {
        struct walk_context context = {
            .communicator = transfer_communicator,
            .buffer = buffer,
            .destination = destination,
            .write_root = writes,
            .total_bytes = 0,
        };
        emit_entry(&context, source, basename, 0);
        broadcast_header(transfer_communicator, ENTRY_END, NULL, 0, 0);
        total_bytes = context.total_bytes;
    } else if (participates) {
        receive_stream(transfer_communicator, buffer, destination);
    }

    if (transfer_communicator != MPI_COMM_NULL) MPI_Comm_free(&transfer_communicator);
    free(buffer);
    free(recipient_bitmap);
    free(source);
    free(destination);
    int local_failure = 0;
    int global_failure = 0;
    MPI_Allreduce(&local_failure, &global_failure, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    clock_gettime(CLOCK_MONOTONIC, &end);
    double elapsed = get_elapsed(start, end);
    double maximum_elapsed = 0;
    MPI_Reduce(&elapsed, &maximum_elapsed, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        double gib = (double)total_bytes / (1024.0 * 1024.0 * 1024.0);
        printf("bcast: Transferred %.2f GiB in %.2f seconds (%.2f GiB/s); "
               "recipients=%d/%d; root=%s\n",
               gib, maximum_elapsed,
               maximum_elapsed > 0 ? gib / maximum_elapsed : 0.0,
               recipient_count, world_size, hostname);
    }
    MPI_Finalize();
    return global_failure ? 1 : 0;
}
