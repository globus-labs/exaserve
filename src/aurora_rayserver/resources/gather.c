/*
 * gather - parallel log/artifact collection via MPI.
 *
 * Replaces the ssh fan-out in launch_cluster.sh's finalize_run_logs.
 * Each rank tars its local source paths into <dest>/<hostname>.tar.gz on
 * the shared filesystem. MPI is used for:
 *   - PALS-coordinated launch (mpiexec)
 *   - Synchronized start (MPI_Barrier)
 *   - Aggregate timing  (MPI_Reduce MPI_MAX)
 *   - Error aggregation (MPI_Allreduce MPI_SUM)
 *
 * Data path is rank -> Lustre (N concurrent file creates of distinct
 * filenames; MDS handles this fine). There is no rank-0 buffer collection
 * because that scales as O(N * size / BW_at_rank_0) and offers no benefit
 * for "many small archives" workloads.
 *
 * Usage:
 *   mpiexec -n N -ppn 1 gather <dest_dir> <src1> [<src2> ...]
 *
 * Each source path is included in the rank's tarball if it exists on that
 * node; missing paths are silently skipped (tar --ignore-failed-read).
 *
 * Exit code: 0 only if every rank's tar succeeded. Otherwise nonzero,
 * with rank 0 printing a list of failed hostnames.
 */

#include <mpi.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <errno.h>
#include <limits.h>

#define MAX_HOSTNAME 256
#define MAX_CMD      32768

static double get_elapsed(struct timespec t1, struct timespec t2) {
    time_t sec = t2.tv_sec - t1.tv_sec;
    long  nsec = t2.tv_nsec - t1.tv_nsec;
    if (nsec < 0) { sec--; nsec += 1000000000L; }
    return sec + nsec * 1e-9;
}

/* Append a shell-quoted token to dst[]. Returns chars written, or -1 on overflow. */
static int append_quoted(char *dst, size_t cap, size_t pos, const char *src) {
    /* Conservative single-quote escaping: each ' becomes '\'' . */
    size_t need = 2 /* surrounding quotes */ + 1 /* space */;
    for (const char *p = src; *p; p++) need += (*p == '\'') ? 4 : 1;
    if (pos + need >= cap) return -1;
    dst[pos++] = ' ';
    dst[pos++] = '\'';
    for (const char *p = src; *p; p++) {
        if (*p == '\'') {
            memcpy(&dst[pos], "'\\''", 4);
            pos += 4;
        } else {
            dst[pos++] = *p;
        }
    }
    dst[pos++] = '\'';
    dst[pos] = '\0';
    return (int)pos;
}

int main(int argc, char **argv) {
    MPI_Init(NULL, NULL);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 3) {
        if (rank == 0) {
            fprintf(stderr,
                "Usage: gather <dest_dir> <src1> [<src2> ...]\n"
                "  dest_dir   shared (Lustre) dir; each rank writes <dest>/<hostname>.tar.gz\n"
                "  srcN       local path(s); missing paths are skipped\n");
        }
        MPI_Finalize();
        return 2;
    }

    const char *dest_dir = argv[1];
    char hostname[MAX_HOSTNAME] = {0};
    if (gethostname(hostname, sizeof(hostname) - 1) != 0) {
        perror("gethostname");
        snprintf(hostname, sizeof(hostname), "rank%d", rank);
    }
    /* Strip after first dot for the "short" hostname (x4406c2s5b0n0). */
    char *dot = strchr(hostname, '.');
    if (dot) *dot = '\0';

    char out_path[PATH_MAX];
    snprintf(out_path, sizeof(out_path), "%s/%s.tar.gz", dest_dir, hostname);

    /* Build the existing-only source list. tar's --ignore-failed-read covers
     * the case where a listed source is missing, but tar still errors if its
     * ENTIRE input is missing — so we pre-filter on this rank. */
    int n_existing = 0;
    const char *existing_srcs[64];
    for (int i = 2; i < argc && n_existing < 64; i++) {
        struct stat st;
        if (stat(argv[i], &st) == 0) existing_srcs[n_existing++] = argv[i];
    }

    /* Rank 0 ensures the destination dir exists. Race-free vs sibling ranks
     * because we MPI_Barrier before writing. */
    int rank0_mkdir_ok = 1;
    if (rank == 0) {
        char mkcmd[PATH_MAX + 16];
        snprintf(mkcmd, sizeof(mkcmd), "mkdir -p %s", dest_dir);
        if (system(mkcmd) != 0) {
            fprintf(stderr, "[gather rank0] mkdir -p %s failed\n", dest_dir);
            rank0_mkdir_ok = 0;
        }
    }
    MPI_Bcast(&rank0_mkdir_ok, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (!rank0_mkdir_ok) {
        MPI_Finalize();
        return 3;
    }

    /* Synchronized start so the per-node timing is comparable. */
    MPI_Barrier(MPI_COMM_WORLD);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    int rc = 0;
    long long bytes_written = 0;

    if (n_existing == 0) {
        /* Nothing to tar — create an empty marker so callers see the node
         * was here. */
        FILE *f = fopen(out_path, "w");
        if (f) {
            fclose(f);
        } else {
            fprintf(stderr, "[gather %s] fopen empty %s: %s\n",
                    hostname, out_path, strerror(errno));
            rc = 1;
        }
    } else {
        /* tar -czf <out_path> --ignore-failed-read --warning=no-file-changed
         *      <srcs...> 2>/tmp/gather.<host>.err
         *
         * --ignore-failed-read: continue past unreadable files (e.g. files
         *   that disappear mid-tar, common in live log dirs).
         * --warning=no-file-changed: suppress chatter about live writes.
         */
        char cmd[MAX_CMD];
        int pos = snprintf(cmd, sizeof(cmd),
            "tar --ignore-failed-read --warning=no-file-changed -czf '%s'",
            out_path);
        if (pos < 0 || (size_t)pos >= sizeof(cmd)) {
            fprintf(stderr, "[gather %s] command buffer overflow on output path\n",
                    hostname);
            rc = 1;
        } else {
            for (int i = 0; i < n_existing && rc == 0; i++) {
                int p = append_quoted(cmd, sizeof(cmd), pos, existing_srcs[i]);
                if (p < 0) {
                    fprintf(stderr, "[gather %s] command buffer overflow on src %s\n",
                            hostname, existing_srcs[i]);
                    rc = 1;
                    break;
                }
                pos = p;
            }
            if (rc == 0) {
                int sys_rc = system(cmd);
                /* tar returns 1 for "some files changed" warnings, which are
                 * benign here. Treat 0 and 1 as success. */
                if (sys_rc != 0 && WEXITSTATUS(sys_rc) != 1) {
                    fprintf(stderr, "[gather %s] tar exited %d\n",
                            hostname, WEXITSTATUS(sys_rc));
                    rc = 1;
                }
                struct stat st;
                if (rc == 0 && stat(out_path, &st) == 0) {
                    bytes_written = (long long)st.st_size;
                }
            }
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = get_elapsed(t0, t1);

    /* Aggregate timing and errors. */
    double max_elapsed = 0.0;
    long long total_bytes = 0;
    int total_errors = 0;
    MPI_Reduce(&elapsed,       &max_elapsed,  1, MPI_DOUBLE,    MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&bytes_written, &total_bytes,  1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Allreduce(&rc,         &total_errors, 1, MPI_INT,       MPI_SUM, MPI_COMM_WORLD);

    if (rank == 0) {
        double mb = (double)total_bytes / (1024.0 * 1024.0);
        fprintf(stderr,
            "[gather] %d rank(s), %d failure(s), %.2f MiB total, max %.2fs\n",
            size, total_errors, mb, max_elapsed);
        if (total_errors > 0) {
            fprintf(stderr, "[gather] %d rank(s) reported tar errors; see "
                            "individual ranks' stderr above\n", total_errors);
        }
    }

    MPI_Finalize();
    return total_errors == 0 ? 0 : 4;
}
