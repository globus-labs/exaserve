#define _POSIX_C_SOURCE 200809L

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <mpi.h>

#define CHECK_ERROR(cond, errstr)               \
    do {                                        \
        if (cond) {                             \
            perror(errstr);                     \
            MPI_Abort(MPI_COMM_WORLD, 1);       \
        }                                       \
    } while (0)

/* PR-004: 1 GiB/rank was a large aggregate cost for a streaming copy; a
 * bounded 64 MiB buffer is plenty for MPI_Bcast throughput. */
#define BUFFER_SIZE (64L << 20)

static double get_elapsed(struct timespec t1, struct timespec t2);

/* PR-004: recursive mkdir via syscalls instead of a shell command;
 * no shell, no injection, and errno is inspectable. Returns 0 on success. */
static int mkdir_p(const char *path) {
    char tmp[4096];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return -1;
    memcpy(tmp, path, len + 1);
    if (tmp[len - 1] == '/') tmp[len - 1] = '\0';
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
            *p = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
    return 0;
}

/* Spawn tar through an argv vector. The old shell-pipe implementation invoked
 * /bin/sh with a constructed command string; careful quoting reduced the
 * injection risk but retained an unnecessary shell and truncation boundary. */
static FILE *spawn_tar_reader(const char *parent, const char *name, pid_t *child_pid) {
    int fds[2];
    if (pipe(fds) != 0) return NULL;
    pid_t pid = fork();
    if (pid < 0) {
        int saved = errno;
        close(fds[0]);
        close(fds[1]);
        errno = saved;
        return NULL;
    }
    if (pid == 0) {
        close(fds[0]);
        if (dup2(fds[1], STDOUT_FILENO) < 0) _exit(126);
        close(fds[1]);
        execlp("tar", "tar", "-C", parent, "-chf", "-", name, (char *)NULL);
        _exit(127);
    }
    close(fds[1]);
    FILE *stream = fdopen(fds[0], "r");
    if (stream == NULL) {
        int saved = errno;
        close(fds[0]);
        kill(pid, SIGTERM);
        while (waitpid(pid, NULL, 0) < 0 && errno == EINTR) {}
        errno = saved;
        return NULL;
    }
    *child_pid = pid;
    return stream;
}

static FILE *spawn_tar_writer(const char *destination, pid_t *child_pid) {
    int fds[2];
    if (pipe(fds) != 0) return NULL;
    pid_t pid = fork();
    if (pid < 0) {
        int saved = errno;
        close(fds[0]);
        close(fds[1]);
        errno = saved;
        return NULL;
    }
    if (pid == 0) {
        close(fds[1]);
        if (dup2(fds[0], STDIN_FILENO) < 0) _exit(126);
        close(fds[0]);
        execlp("tar", "tar", "-xf", "-", "-C", destination, (char *)NULL);
        _exit(127);
    }
    close(fds[0]);
    FILE *stream = fdopen(fds[1], "w");
    if (stream == NULL) {
        int saved = errno;
        close(fds[1]);
        kill(pid, SIGTERM);
        while (waitpid(pid, NULL, 0) < 0 && errno == EINTR) {}
        errno = saved;
        return NULL;
    }
    *child_pid = pid;
    return stream;
}

/* Close the stream and require the exact child to exit successfully. */
static int finish_tar(FILE *stream, pid_t child_pid) {
    int close_failed = fclose(stream) != 0;
    int status = 0;
    pid_t waited;
    do {
        waited = waitpid(child_pid, &status, 0);
    } while (waited < 0 && errno == EINTR);
    return !close_failed && waited == child_pid && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

int main(int argc, char **argv) {
    struct timespec start, end;
    const char *destdir;
    int rank;
    unsigned long long total_bytes = 0;
    int no_root_write = 0;
    int local_fail = 0;   /* PR-004: per-rank failure flag, aggregated below */

    clock_gettime(CLOCK_MONOTONIC, &start);

    int positional[2];
    int npos = 0;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--no-root-write") == 0) {
            no_root_write = 1;
        } else {
            if (npos < 2) positional[npos] = i;
            npos++;
        }
    }

    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        if (rank == 0) perror("bcast: could not ignore SIGPIPE");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (npos < 1) {
        if (rank == 0) fprintf(stderr, "Usage: bcast [--no-root-write] <src> [dest]\n");
        MPI_Finalize();
        return 1;
    }

    char *srcpath = strdup(argv[positional[0]]);
    char *destpath = (npos >= 2) ? strdup(argv[positional[1]]) : strdup("/tmp");
    if (srcpath == NULL || destpath == NULL || srcpath[0] == '\0' || destpath[0] == '\0') {
        if (rank == 0) fprintf(stderr, "bcast: source/destination allocation or value invalid\n");
        free(srcpath);
        free(destpath);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    destdir = destpath;

    FILE *archive = NULL;
    pid_t archive_pid = -1;

    if (rank == 0) {
        size_t source_len = strlen(srcpath);
        while (source_len > 1 && srcpath[source_len - 1] == '/') {
            srcpath[--source_len] = '\0';
        }
        if (strcmp(srcpath, "/") == 0) {
            fprintf(stderr, "bcast: refusing to broadcast the filesystem root\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        char *dup = strdup(srcpath);
        CHECK_ERROR(!dup, "strdup");
        char *slash = strrchr(dup, '/');
        char *left, *right;

        if (slash != NULL) {
            if (slash == dup) {
                left = "/";
            } else {
                *slash = '\0';
                left = dup;
            }
            right = slash + 1;
        } else {
            left = ".";
            right = dup;
        }
        if (right[0] == '\0') {
            fprintf(stderr, "bcast: source basename is empty\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        archive = spawn_tar_reader(left, right, &archive_pid);
        CHECK_ERROR(!archive, "spawn tar reader");
        free(dup);

        printf("bcast: Broadcasting %s to %s ()...\n", srcpath, destdir);
    }

    int skip_write = (rank == 0 && no_root_write);
    FILE *dest = NULL;
    pid_t dest_pid = -1;

    if (!skip_write) {
        /* PR-004: checked mkdir via syscall, with no shell. */
        if (mkdir_p(destdir) != 0) {
            fprintf(stderr, "Rank %d: mkdir_p(%s) failed: %s\n",
                    rank, destdir, strerror(errno));
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        dest = spawn_tar_writer(destdir, &dest_pid);
        CHECK_ERROR(!dest, "spawn tar writer");
    }

    /* PR-004: explicit NULL check (assert is compiled out under -DNDEBUG). */
    void *buf = malloc(BUFFER_SIZE);
    if (!buf) {
        fprintf(stderr, "Rank %d: failed to allocate %ld-byte buffer\n",
                rank, (long)BUFFER_SIZE);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    while (1) {
        int chunk_size = 0;

        if (rank == 0) {
            size_t bytes_read = 0;
            while (bytes_read < BUFFER_SIZE) {
                size_t n = fread((char*)buf + bytes_read, 1, BUFFER_SIZE - bytes_read, archive);

                if (n == 0) {
                    if (ferror(archive)) {
                         perror("fread error");
                         MPI_Abort(MPI_COMM_WORLD, 1);
                    }
                    break;
                }
                bytes_read += n;
            }
            chunk_size = (int)bytes_read;
        }

        MPI_Bcast(&chunk_size, 1, MPI_INT, 0, MPI_COMM_WORLD);

        if (chunk_size == 0) {
            break;
        }

        MPI_Bcast(buf, chunk_size, MPI_BYTE, 0, MPI_COMM_WORLD);

        if (!skip_write) {
            size_t total_written = 0;
            size_t wanted = (size_t)chunk_size;
            while (total_written < wanted) {
                size_t n = fwrite((char*)buf + total_written, 1, wanted - total_written, dest);
                if (n == 0) {
                     fprintf(stderr, "Rank %d: Write error (Disk full?)\n", rank);
                     MPI_Abort(MPI_COMM_WORLD, 1);
                }
                total_written += n;
            }
        }

        total_bytes += chunk_size;
    }

    /* PR-004: close the pipes and CHECK their exit status. A tar producer or
     * extractor that exits nonzero (partial/corrupt archive, disk full at
     * flush) previously went unnoticed and the broadcast reported success. */
    if (rank == 0) {
        if (!finish_tar(archive, archive_pid)) {
            fprintf(stderr, "Rank 0: tar (producer) exited nonzero\n");
            local_fail = 1;
        }
    }
    if (dest) {
        if (!finish_tar(dest, dest_pid)) {
            fprintf(stderr, "Rank %d: tar (extractor) exited nonzero\n", rank);
            local_fail = 1;
        }
    }
    free(buf);
    free(srcpath);
    free(destpath);

    /* PR-004: aggregate failures across ALL ranks; any rank's failure makes
     * the whole broadcast fail with a nonzero exit. */
    int global_fail = 0;
    MPI_Allreduce(&local_fail, &global_fail, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &end);
    double elapsed = get_elapsed(start, end);
    double max_time;
    MPI_Reduce(&elapsed, &max_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        if (global_fail) {
            fprintf(stderr, "bcast: FAILED — one or more ranks reported a "
                            "tar/extract error; broadcast is not complete\n");
        } else {
            double gb = (double)total_bytes / (1024.0 * 1024.0 * 1024.0);
            printf("bcast: Transferred %.2f GiB in %.2f seconds (%.2f GiB/s)\n",
                gb, max_time, max_time > 0 ? gb/max_time : 0.0);
        }
    }

    MPI_Finalize();
    return global_fail ? 1 : 0;
}

static double get_elapsed(struct timespec t1, struct timespec t2)
{
    time_t sec = t2.tv_sec - t1.tv_sec;
    long nsec = t2.tv_nsec - t1.tv_nsec;
    if (nsec < 0) { sec--; nsec += 1000000000L; }
    return sec + nsec * 1e-9;
}
