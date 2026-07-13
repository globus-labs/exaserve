# Ray Overlay Distribution Design (perf-inst-dev)

## Current mechanism (as of 2026-04-21, commit 4ac034b)

Instrumented Ray files live in a **sparse overlay** tracked as a separate git
repo at `~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray`.
Only files under `serve/_private/` that we actually edit are present:
`constants.py` (Aurora timeout patches), `proxy.py` (ProxyActor
instrumentation), and pristine copies of `controller.py`/`proxy_state.py` as
baselines for future patches.

[src/exaserve/resources/launch_cluster.sh](../src/exaserve/resources/launch_cluster.sh) builds a per-node
**symlink farm** at `/tmp/ray_overlay/` on each compute node:

- Symlinks for every other file in `ray/*`, `ray/serve/*`, and
  `ray/serve/_private/*` → pointing at the system Ray install
  (`/opt/aurora/.../site-packages/ray/`)
- Real copies of the 4 patched files from the overlay repo

`PYTHONPATH=/tmp/ray_overlay:...` is prepended so Python finds our patched
files first and falls through to system Ray (via symlinks) for everything
else. The build script is staged on `$HOME` (Lustre) so SSHed workers can
read it; each node runs it locally to populate its own `/tmp/ray_overlay`.

Fan-out at job start: `ssh -f <node> "bash $script"` for every worker, then
`sleep 5` + `rm` the staged script.

## Known limitations of SSH fan-out

1. **O(N) serial SSH handshakes.** Each `ssh -f` is ~100ms. At 256 nodes
   that's ~25s; at 1024 nodes ~100s; at 10k ~15 min. Not yet the dominant
   Stage 0 cost, but worth measuring.
2. **No return-code checking.** `-f` backgrounds after auth; parent shell
   never reads per-worker exit status. If a worker's overlay build fails
   (disk full, transient network, stale `/tmp`), that worker silently falls
   through to system Ray. Symptom would be: some replicas use patched
   constants, others don't — confusing telemetry.
3. **Race on the `rm` of the staged script.** `sleep 5` is the workaround;
   a worker that hasn't yet started `bash` can fail.
4. **No per-worker stdout capture.** Useful error messages are lost.

None are proven bottlenecks yet. Phase 2 smoke (2-node) passed cleanly.
Worth scale-testing at 128n/256n to quantify the SSH fan-out wall time.

## Option: Copper (ALCF cooperative caching)

[Copper](https://docs.alcf.anl.gov/aurora/data-management/copper/copper/) is
a FUSE-based read-only cooperative cache designed exactly for Python
module fan-out on Aurora. First node to touch a file pulls from Lustre;
subsequent nodes get it peer-to-peer over the compute fabric.

### How Copper works
- Mount point you pick, e.g. `/tmp/$USER/copper_mount/`
- Preserves the source path: `/lus/.../foo` is visible at
  `/tmp/$USER/copper_mount/lus/.../foo`
- Requires ≥ 2 nodes; recommended per-file size 10 MiB – 100 MiB
- Lifecycle: `module load copper` → `launch_copper_aurora.sh -v <mount>`
  → use → `stop_copper_aurora.sh`

### Mismatch with our sparse overlay
The current code in [launch_cluster.sh:397-413](../src/exaserve/resources/launch_cluster.sh#L397-L413)
attempts to prepend `/tmp/$USER/copper/$OVERLAY_DIR` to PYTHONPATH. Two
problems:

1. **Mount path mismatch.** Launcher uses `/tmp/$USER/copper_mount` for the
   mount but `/tmp/$USER/copper` for PYTHONPATH — paths don't line up.
2. **Sparse overlay can't be a PYTHONPATH root.** Our overlay only has 4
   files under `ray/serve/_private/`. Python needs `ray/__init__.py` and
   every intermediate `__init__.py` to import submodules — which don't
   exist in the sparse tree. Any `import ray.non_patched_thing` fails.

`/tmp/ray_overlay`'s symlink farm solves this because every expected file
is present (most as symlinks). Copper can't operate on a symlink farm
well: symlinks resolve through VFS to their targets, which for us are
`/opt/aurora/...` (shared read-only software FS, not Lustre). Copper would
only accelerate the 4 real patched files — minimal benefit.

### A design that actually uses Copper

Build the symlink farm **on Lustre** once on the head node, then let
Copper distribute it:

```
Head node, once at job start:
  $HOME/.exaserve_ray_overlay/
    ├── ray/<non-serve modules> → symlinks to /opt/aurora/.../ray/*
    ├── ray/serve/<non-_private> → symlinks
    └── ray/serve/_private/
         ├── {api,common,...}.py → symlinks to /opt/aurora/.../_private/*
         └── {constants,controller,proxy,proxy_state}.py → real files

All nodes:
  launch_copper_aurora.sh -v /tmp/$USER/copper_mount
  PYTHONPATH="/tmp/$USER/copper_mount$HOME/.exaserve_ray_overlay:$PYTHONPATH"
```

Benefits vs SSH fan-out:
- Constant-time setup regardless of node count (no O(N) SSH)
- Explicit mount status; failures are reported
- ALCF-blessed mechanism for this exact workload

Tradeoffs:
- Copper startup overhead (10-30s, unmeasured on our stack) — loss at 2-8 nodes
- Adds Copper as a dependency for experiments
- Must cleanly stop Copper on exit to avoid stale mounts

### When to migrate

Do **not** switch yet. Scale-test the current SSH fan-out at 128n/256n
first and measure the fan-out wall time from the launch log. Migrate to
Copper only if:
- Measured SSH fan-out dominates Stage 0 at our target scale, OR
- We see flaky overlays (workers silently missing the patched files), OR
- We start targeting > 512 nodes, where SSH fan-out becomes untenable

## Rollback
Main-repo commit 2f32633 restores the pre-perf-inst-dev state
(sitecustomize/usercustomize approach, no overlay at all, no Copper code
beyond the existing-but-buggy attempt).

## References
- [ALCF Copper user guide](https://docs.alcf.anl.gov/aurora/data-management/copper/copper/)
- [argonne-lcf/copper on GitHub](https://github.com/argonne-lcf/copper)
