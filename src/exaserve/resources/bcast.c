#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
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

/* PR-004: single-quote a path for safe use inside a /bin/sh command, so a
 * path containing spaces or shell metacharacters cannot inject commands.
 * Returns 0 on success, -1 if the result would not fit (truncation). */
static int shquote(char *dst, size_t dstsize, const char *src) {
    size_t di = 0;
    if (dstsize == 0) return -1;
    if (di + 1 >= dstsize) return -1;
    dst[di++] = '\'';
    for (const char *p = src; *p; p++) {
        if (*p == '\'') {
            /* close quote, escaped quote, reopen quote: '\'' */
            if (di + 4 >= dstsize) return -1;
            dst[di++] = '\''; dst[di++] = '\\'; dst[di++] = '\''; dst[di++] = '\'';
        } else {
            if (di + 1 >= dstsize) return -1;
            dst[di++] = *p;
        }
    }
    if (di + 2 > dstsize) return -1;
    dst[di++] = '\'';
    dst[di] = '\0';
    return 0;
}

/* PR-004: recursive mkdir via syscalls instead of system("mkdir -p ...");
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

/* Check a pclose() return: 0 iff the child ran and exited 0. */
static int pipe_ok(int status) {
    return status != -1 && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

int main(int argc, char **argv) {
    struct timespec start, end;
    const char *destdir;
    char command[8192];
    char q1[4096], q2[4096];
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

    char *srcpath = (npos >= 1) ? strdup(argv[positional[0]]) : NULL;
    char *destpath = (npos >= 2) ? strdup(argv[positional[1]]) : strdup("/tmp");
    destdir = destpath;

    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (npos < 1) {
        if (rank == 0) fprintf(stderr, "Usage: bcast [--no-root-write] <src> [dest]\n");
        MPI_Finalize();
        return 1;
    }

    FILE *archive = NULL;

    if (rank == 0) {
        int last_idx = strlen(srcpath) - 1;
        if (srcpath[last_idx] == '/') srcpath[last_idx] = '\0';

        char *dup = strdup(srcpath);
        char *slash = strrchr(dup, '/');
        char *left, *right;

        if (slash != NULL) {
            *slash = '\0';
            left = dup;
            right = slash + 1;
        } else {
            left = ".";
            right = dup;
        }

        /* PR-004: quote paths and verify no truncation before running tar. */
        if (shquote(q1, sizeof(q1), left) != 0 ||
            shquote(q2, sizeof(q2), right) != 0) {
            fprintf(stderr, "bcast: source path too long to quote safely\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        int n = snprintf(command, sizeof(command), "tar -C %s -chf - %s", q1, q2);
        if (n < 0 || (size_t)n >= sizeof(command)) {
            fprintf(stderr, "bcast: tar command truncated\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        archive = popen(command, "r");
        CHECK_ERROR(!archive, "popen (read)");
        free(dup);

        printf("bcast: Broadcasting %s to %s ()...\n", srcpath, destdir);
    }

    int skip_write = (rank == 0 && no_root_write);
    FILE *dest = NULL;

    if (!skip_write) {
        /* PR-004: mkdir via syscall (checked), not system(). */
        if (mkdir_p(destdir) != 0) {
            fprintf(stderr, "Rank %d: mkdir_p(%s) failed: %s\n",
                    rank, destdir, strerror(errno));
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        if (shquote(q1, sizeof(q1), destdir) != 0) {
            fprintf(stderr, "Rank %d: dest path too long to quote safely\n", rank);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        int n = snprintf(command, sizeof(command), "tar -xf - -C %s", q1);
        if (n < 0 || (size_t)n >= sizeof(command)) {
            fprintf(stderr, "Rank %d: extract command truncated\n", rank);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        dest = popen(command, "w");
        CHECK_ERROR(!dest, "popen (write)");
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
            chunk_size = bytes_read;
        }

        MPI_Bcast(&chunk_size, 1, MPI_INT, 0, MPI_COMM_WORLD);

        if (chunk_size == 0) {
            break;
        }

        MPI_Bcast(buf, chunk_size, MPI_BYTE, 0, MPI_COMM_WORLD);

        if (!skip_write) {
            size_t total_written = 0;
            while (total_written < chunk_size) {
                size_t n = fwrite((char*)buf + total_written, 1, chunk_size - total_written, dest);
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
        if (!pipe_ok(pclose(archive))) {
            fprintf(stderr, "Rank 0: tar (producer) exited nonzero\n");
            local_fail = 1;
        }
    }
    if (dest) {
        if (!pipe_ok(pclose(dest))) {
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
