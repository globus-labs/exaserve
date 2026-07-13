"""Generate doc/ExaServe_Reference_Card.docx.

Matches the Academy_Framework_Reference_Card.docx visual language exactly:
same palette (rust 9C4A2E/C2613F, navy 2C3340, warm grays), same
direct-formatting idiom, same code-block/callout/table constructs. Uses the
Academy docx as the container (styles/settings/fonts/footers preserved) and
swaps document.xml. The docx is fully generated — edit THIS script, not the
Word file, or your changes will be lost on the next regeneration.

Usage:  python3 tools/make_refcard_docx.py
Needs:  tmp/ref_card/Academy_Framework_Reference_Card.docx as the template.
"""
import re
import zipfile

SRC = "/home/wenyiw/aurora_rayserver/tmp/ref_card/Academy_Framework_Reference_Card.docx"
OUT = "/home/wenyiw/aurora_rayserver/doc/ExaServe_Reference_Card.docx"

RUST = "9C4A2E"
RUST_BORDER = "C2613F"
NAVY = "2C3340"
GRAYTXT = "5B6472"
CODE_BG = "F4F2EF"
LEARN_BG = "FBF1EC"
NOTE_BG = "FCF3E2"
TBL_BORDER = "D9D4CC"
CODE_EDGE = "E2DCD4"
FULL_W = 10224


def esc(t):
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def run(text, sz=None, b=False, color=None, font=None, i=False):
    rpr = ""
    if font:
        rpr += f'<w:rFonts w:ascii="{font}" w:hAnsi="{font}" w:cs="{font}"/>'
    if b:
        rpr += "<w:b/><w:bCs/>"
    if i:
        rpr += "<w:i/><w:iCs/>"
    if color:
        rpr += f'<w:color w:val="{color}"/>'
    if sz:
        rpr += f'<w:sz w:val="{sz}"/><w:szCs w:val="{sz}"/>'
    rpr = f"<w:rPr>{rpr}</w:rPr>" if rpr else ""
    return f'<w:r>{rpr}<w:t xml:space="preserve">{esc(text)}</w:t></w:r>'


def par(runs_xml, before=None, after=None, pbdr=None, line=None, jc=None):
    ppr = ""
    if pbdr:
        sz, space = pbdr
        ppr += (f'<w:pBdr><w:bottom w:val="single" w:color="{RUST_BORDER}" '
                f'w:sz="{sz}" w:space="{space}"/></w:pBdr>')
    if before is not None or after is not None:
        b = f' w:before="{before}"' if before is not None else ""
        a = f' w:after="{after}"' if after is not None else ""
        ppr += f"<w:spacing{b}{a}/>"
    if line:
        ppr += f'<w:spacing w:line="{line}" w:lineRule="auto"/>'
    if jc:
        ppr += f'<w:jc w:val="{jc}"/>'
    ppr = f"<w:pPr>{ppr}</w:pPr>" if ppr else ""
    return f"<w:p>{ppr}{runs_xml}</w:p>"


def body_par(text, after=80):
    return par(run(text), after=after)


def bold_lead(text):
    return par(run(text, b=True), after=60)


def heading(num, text):
    return par(
        run(f"{num}  ·  {text}", sz=26, b=True, color=RUST),
        before=260, after=120, pbdr=(12, 3),
    )


def one_cell_table(cell_xml, left_color, left_sz, edge_color, edge_sz, fill, mar):
    top, lft, bot, rgt = mar
    return (
        f'<w:tbl><w:tblPr><w:tblW w:type="dxa" w:w="{FULL_W}"/>'
        "<w:tblBorders>"
        '<w:top w:val="none" w:color="auto" w:sz="0"/>'
        '<w:left w:val="none" w:color="auto" w:sz="0"/>'
        '<w:bottom w:val="none" w:color="auto" w:sz="0"/>'
        '<w:right w:val="none" w:color="auto" w:sz="0"/>'
        '<w:insideH w:val="none" w:color="auto" w:sz="0"/>'
        '<w:insideV w:val="none" w:color="auto" w:sz="0"/>'
        "</w:tblBorders></w:tblPr>"
        f'<w:tblGrid><w:gridCol w:w="{FULL_W}"/></w:tblGrid>'
        f'<w:tr><w:trPr><w:cantSplit/></w:trPr>'
        f'<w:tc><w:tcPr><w:tcW w:type="dxa" w:w="{FULL_W}"/>'
        "<w:tcBorders>"
        f'<w:top w:val="single" w:color="{edge_color}" w:sz="{edge_sz}"/>'
        f'<w:left w:val="single" w:color="{left_color}" w:sz="{left_sz}"/>'
        f'<w:bottom w:val="single" w:color="{edge_color}" w:sz="{edge_sz}"/>'
        f'<w:right w:val="single" w:color="{edge_color}" w:sz="{edge_sz}"/>'
        "</w:tcBorders>"
        f'<w:shd w:fill="{fill}" w:color="auto" w:val="clear"/>'
        f'<w:tcMar><w:top w:type="dxa" w:w="{top}"/><w:left w:type="dxa" w:w="{lft}"/>'
        f'<w:bottom w:type="dxa" w:w="{bot}"/><w:right w:type="dxa" w:w="{rgt}"/></w:tcMar>'
        f"</w:tcPr>{cell_xml}</w:tc></w:tr></w:tbl><w:p/>"
    )


def code_block(text):
    lines = text.rstrip("\n").split("\n")
    runs = []
    for i, ln in enumerate(lines):
        if i:
            runs.append("<w:r><w:br/></w:r>")
        runs.append(run(ln, sz=16, font="Consolas"))
    cell = par("".join(runs), line=248)
    return one_cell_table(cell, RUST_BORDER, 18, CODE_EDGE, 4, CODE_BG,
                          (90, 160, 90, 140))


def callout(lead, text, fill):
    runs = run(lead + "  ", b=True, color=RUST) + run(text)
    cell = f"<w:p>{runs}</w:p>"
    return one_cell_table(cell, RUST_BORDER, 20, RUST_BORDER, 4, fill,
                          (110, 170, 110, 150))


IMG_DIR = "/home/wenyiw/aurora_rayserver/doc/figures"
_images = []  # (zip_target, src_path, relationship_id)


def figure(png_name, caption, width_in):
    """Embed doc/figures/<png_name> centered at width_in inches, with an
    italic gray caption underneath (paper-figure style)."""
    import os
    import struct
    src = os.path.join(IMG_DIR, png_name)
    d = open(src, "rb").read()
    w, h = struct.unpack(">II", d[16:24])   # PNG IHDR
    rid = f"rIdFig{len(_images) + 1}"
    _images.append((f"media/{png_name}", src, rid))
    cx = int(width_in * 914400)             # EMU
    cy = int(cx * h / w)
    n = 100 + len(_images)
    drawing = (
        '<w:r><w:drawing>'
        '<wp:inline distT="0" distB="0" distL="0" distR="0" '
        'xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing">'
        f'<wp:extent cx="{cx}" cy="{cy}"/>'
        f'<wp:docPr id="{n}" name="{png_name}"/>'
        '<a:graphic xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">'
        '<a:graphicData uri="http://schemas.openxmlformats.org/drawingml/2006/picture">'
        '<pic:pic xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture">'
        f'<pic:nvPicPr><pic:cNvPr id="{n}" name="{png_name}"/><pic:cNvPicPr/></pic:nvPicPr>'
        f'<pic:blipFill><a:blip r:embed="{rid}" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"/>'
        '<a:stretch><a:fillRect/></a:stretch></pic:blipFill>'
        f'<pic:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="{cx}" cy="{cy}"/></a:xfrm>'
        '<a:prstGeom prst="rect"><a:avLst/></a:prstGeom></pic:spPr>'
        '</pic:pic></a:graphicData></a:graphic></wp:inline></w:drawing></w:r>'
    )
    fig_par = par(drawing, before=60, after=40, jc="center")
    cap_par = par(run(caption, sz=16, color=GRAYTXT, i=True), after=120, jc="center")
    return fig_par + cap_par


def data_table(rows, col_w, repeat_header=False):
    def tc(text, w, fill, header):
        rpr = run(text, sz=19, b=header, color="FFFFFF" if header else None)
        return (
            f'<w:tc><w:tcPr><w:tcW w:type="dxa" w:w="{w}"/>'
            "<w:tcBorders>"
            f'<w:top w:val="single" w:color="{TBL_BORDER}" w:sz="2"/>'
            f'<w:left w:val="single" w:color="{TBL_BORDER}" w:sz="2"/>'
            f'<w:bottom w:val="single" w:color="{TBL_BORDER}" w:sz="2"/>'
            f'<w:right w:val="single" w:color="{TBL_BORDER}" w:sz="2"/>'
            "</w:tcBorders>"
            f'<w:shd w:fill="{fill}" w:color="auto" w:val="clear"/>'
            '<w:tcMar><w:top w:type="dxa" w:w="70"/><w:left w:type="dxa" w:w="120"/>'
            '<w:bottom w:type="dxa" w:w="70"/><w:right w:type="dxa" w:w="120"/></w:tcMar>'
            f"</w:tcPr><w:p>{rpr}</w:p></w:tc>"
        )

    grid = "".join(f'<w:gridCol w:w="{w}"/>' for w in col_w)
    trs = []
    for ri, row in enumerate(rows):
        header = ri == 0
        fill = NAVY if header else ("FFFFFF" if ri % 2 == 1 else CODE_BG)
        cells = "".join(tc(c, col_w[j], fill, header) for j, c in enumerate(row))
        # no row may split across a page boundary; the header row repeats on
        # the next page only where requested (tables expected to span pages)
        trpr = ("<w:trPr><w:tblHeader/><w:cantSplit/></w:trPr>"
                if header and repeat_header
                else "<w:trPr><w:cantSplit/></w:trPr>")
        trs.append(f"<w:tr>{trpr}{cells}</w:tr>")
    borders = "".join(
        f'<w:{side} w:val="single" w:color="{TBL_BORDER}" w:sz="4"/>'
        for side in ("top", "left", "bottom", "right", "insideH", "insideV")
    )
    return (
        f'<w:tbl><w:tblPr><w:tblW w:type="dxa" w:w="{FULL_W}"/>'
        f"<w:tblBorders>{borders}</w:tblBorders></w:tblPr>"
        f"<w:tblGrid>{grid}</w:tblGrid>{''.join(trs)}</w:tbl><w:p/>"
    )


# ------------------------------------------------------------------ content
blocks = []

# title block
blocks.append(par(run("EXASERVE", sz=48, b=True, color=RUST)))
blocks.append(par(run("Framework Reference Card", sz=30, b=True), after=40))
blocks.append(par(
    run("Scaling LLM Inference on HPC  —  One OpenAI-Compatible Endpoint "
        "from N Compute Nodes  •  github.com/wenyiwang-us/ExaServe",
        sz=19, color=GRAYTXT),
    after=140, pbdr=(18, 4),
))
blocks.append(body_par(
    "ExaServe is a framework for scaling LLM inference across the compute nodes of "
    "an HPC system. From one YAML file and one command, it turns a PBS allocation into a "
    "single OpenAI-compatible inference service: it launches a Ray cluster over the "
    "allocation, stages model weights to node-local storage with an MPI broadcast, deploys "
    "one vLLM (or SGLang) replica per GPU tile as Ray Serve applications, and optionally "
    "fronts them with a head-node proxy such as HAProxy. A companion benchmarking harness "
    "measures deployments end to end and has validated them at up to 256 nodes "
    "(3,072 Intel XPU tiles) on ALCF Aurora.", after=100,
))

# 1 Why
blocks.append(heading(1, "Why ExaServe?"))
blocks.append(body_par(
    "Existing LLM serving stacks assume cloud environments; HPC systems bring PBS "
    "scheduling, MPI-only launch paths, Lustre metadata costs, exotic accelerators, and "
    "scale cliffs that only appear past a hundred nodes. ExaServe packages the "
    "engineering needed to cross that gap:"
))
why = [
    ("Turnkey N-node serving",
     "One YAML file plus one command turns a PBS allocation into an OpenAI-compatible "
     "service; clients see the standard API and need no HPC knowledge."),
    ("Validated at scale",
     "Deployments measured to 256 nodes / 3,072 replicas behind a production HAProxy "
     "front end: 27.1k non-streaming requests/s with Llama-3-8B (one replica per tile) "
     "— 96% weak-scaling efficiency at 256 nodes (27.1k of 28.2k offered, 0% errors). "
     "A quantified comparison of head-node proxies (HAProxy, Envoy, LiteLLM, Ray Serve "
     "proxy) guides the front-end choice."),
    ("Frontier-model ready",
     "Multi-node pipeline parallelism serves Llama-3.1-405B (TP8 × PP2, two nodes per "
     "replica) with shard-aware weight staging, demonstrated from 2 to 128 replicas "
     "(4 to 256 nodes) at 67% weak-scaling efficiency (streaming)."),
    ("Measurement built in",
     "A declarative benchmark harness generates traces and PBS jobs, replays load through "
     "a Go client, and scores per-request TTFT/TBT against latency SLOs."),
]
for lead, text in why:
    blocks.append(bold_lead(lead))
    blocks.append(body_par(text))

# 2 What's in a deployment
blocks.append(heading(2, "What’s in a deployment?"))
blocks.append(body_par(
    "A deployment is a small stack of cooperating pieces, all driven from one "
    "configuration file:"
))
blocks.append(data_table([
    ["Component", "Description"],
    ["Launcher",
     "aurora-serve-submit (batch, from a login node) or aurora-launch-cluster "
     "(interactive) bring up the whole stack on a PBS allocation"],
    ["MPI weight staging",
     "One-source-many-sinks MPI broadcast from the shared file system (e.g., Lustre) to "
     "node-local storage; shard-aware per pipeline stage for very large models"],
    ["Inference replicas",
     "vLLM or SGLang replicas as independent Ray Serve applications — one per GPU "
     "tile (12 per Aurora node) for single-tile models, or spanning multiple tiles and "
     "nodes via tensor/pipeline parallelism for larger models"],
    ["Head-node proxy",
     "HAProxy, LiteLLM, and others — a single client-facing endpoint with load "
     "balancing, plus auth and rate limits with LiteLLM"],
    ["Site patches",
     "Small Ray/vLLM fixes required at 256+ nodes, applied automatically at launch"],
    ["Benchmark harness",
     "eval/: declarative specs → traces + PBS jobs → replay → SLO scoring"],
], [2800, 7424], repeat_header=True))

# 3 Installation
blocks.append(heading(3, "Installation"))
blocks.append(body_par(
    "The package installs into the Python provided by Aurora’s frameworks module, "
    "which already ships Ray, vLLM, MPI, and the oneAPI toolchain; pip adds only the "
    "aurora-rayserver package itself.", after=60,
))
blocks.append(code_block(
    "module load frameworks\n"
    "git clone https://github.com/wenyiwang-us/ExaServe && cd ExaServe\n"
    "python3 -m pip install --user .\n"
    "\n"
    "# console scripts land in a frameworks-versioned bin dir; add it to PATH:\n"
    "export PATH=\"$(python3 -c 'import sysconfig; "
    "print(sysconfig.get_path(\"scripts\", \"posix_user\"))'):$PATH\"\n"
    "which aurora-serve-submit   # verify"
))
blocks.append(body_par(
    "One-time extras, each needed only for the feature that uses it: build HAProxy with "
    "scripts/build_haproxy.sh (proxied deployments); create a separate LiteLLM venv (its "
    "dependencies conflict with Ray/vLLM); build the benchmark load generator with "
    "module load go && bash eval/go_client/build.sh."
))

# 4 Example
blocks.append(heading(4, "Example"))
blocks.append(body_par(
    "One YAML file describes a deployment — the Ray cluster, the model deployment, "
    "and the client-facing proxy. Submit it from a login node (no allocation needed), "
    "then query the printed URL from anywhere that can reach the head node:", after=60,
))
blocks.append(code_block(
    "aurora-serve-submit my_config.yaml --project-account YOUR_PROJECT --wait\n"
    "# 8470123.aurora-pbs-0001...          <- PBS job id\n"
    "# http://x4310c1s0b0n0:4001           <- service URL\n"
    "\n"
    "curl -sS -X POST \"http://x4310c1s0b0n0:4001/v1/chat/completions\" \\\n"
    "     -H 'Content-Type: application/json' \\\n"
    "     -d '{\"model\":\"meta-llama/Meta-Llama-3-8B-Instruct\",\n"
    "          \"messages\":[{\"role\":\"user\",\"content\":\"hello\"}],\"max_tokens\":8}'\n"
    "# {\"id\":\"chatcmpl-...\",\"choices\":[{\"message\":{\"content\":\"Hi!\"...}}],...}\n"
    "\n"
    "qdel 8470123                           # tear down"
))
blocks.append(body_par(
    "The configuration it references: Llama-3-8B on two nodes (24 replicas) behind "
    "HAProxy. model_storage_path (your model directory on the shared file system) and "
    "local_stage_path (node-local staging) are typically the only fields that vary per "
    "user.", after=60,
))
blocks.append(code_block(
    "# my_config.yaml\n"
    "ray_cluster_config:\n"
    "  head_ip: \"\"              # filled in at runtime\n"
    "  port: 6379\n"
    "  node_cpus: 64\n"
    "model_deployment_config:\n"
    "  num_nodes: 2\n"
    "  model_storage_path: /lus/flare/projects/<PROJECT>/<user>/models\n"
    "  local_stage_path: /tmp/hf_home\n"
    "  num_gpus_per_node: 12\n"
    "  model_configs:\n"
    "    - model_id: meta-llama/Meta-Llama-3-8B-Instruct\n"
    "      tensor_parallel_size: 1\n"
    "      pipeline_parallel_size: 1\n"
    "      max_model_len: 4096\n"
    "      size: 8                # billions of params; drives replica planning\n"
    "      gpu_memory_utilization: 0.90\n"
    "      enforce_eager: true\n"
    "      max_num_seqs: 64\n"
    "proxy_config:\n"
    "  type: haproxy              # haproxy | litellm | none\n"
    "  port: 4001                 # client-facing port\n"
    "  backend_port: 8000         # Ray Serve HTTP port on each node"
))
blocks.append(callout(
    "Learn more:",
    "the README covers interactive launches inside qsub -I, custom PBS integration via "
    "--dry-run, and every configuration field (examples/config.reference.yaml is fully "
    "annotated).",
    LEARN_BG,
))

# 5 Scaling
blocks.append(heading(5, "Scaling to hundreds of nodes"))
blocks.append(bold_lead("Front-end proxy at scale"))
blocks.append(body_par(
    "All client traffic enters through the head-node proxy. Non-streaming completions "
    "scale nearly linearly through a single HAProxy — 27.1k requests/s with Llama-3-8B "
    "at 256 nodes / 3,072 single-tile replicas, 0% errors. Streaming (SSE) is harder on "
    "a centralized front end: the per-token delivery path saturates the head node’s "
    "network at large node counts, so requests still complete but slowly — streaming "
    "throughput plateaus around 4.7k requests/s from 128 nodes, and p99 end-to-end "
    "latency reaches 17 s at 256 nodes while non-streaming p99 stays near 2 s at every "
    "scale. Budget streaming capacity per proxy and prefer non-streaming completions at "
    "extreme scale.", after=60,
))
blocks.append(figure(
    "fig1_proxy_scaling.png",
    "Weak scaling, 1–256 nodes (Llama-3-8B, 64 in / 64 out, 110 QPS/node offered): "
    "successful throughput and SLO attainment per front end, streaming (solid) vs "
    "non-streaming (dashed). Non-streaming HAProxy reaches 27.1k QPS; streaming through "
    "any centralized proxy plateaus or collapses. “Direct” is the benchmark-only "
    "backend-isolation diagnostic.",
    7.0,
))
blocks.append(callout(
    "Note:",
    "for benchmarking only, the harness’s client can bypass the proxy and dispatch "
    "to per-node endpoints (client.dest: direct in a spec) to measure backend capacity "
    "without a front end — backends themselves scale linearly to 256 nodes when the "
    "proxy is bypassed. This mode exposes one endpoint per node and is not a deployment "
    "path.",
    NOTE_BG,
))
blocks.append(bold_lead("Multi-node models"))
blocks.append(body_par(
    "Models larger than a node are served with pipeline parallelism across node pairs "
    "(Llama-3.1-405B: TP8 within a node × PP2 across two nodes). Shard-aware staging "
    "gives each pipeline stage only its own weight shard (~380 GiB, which fits node-local "
    "tmpfs) and pins each replica to its nodes. All 405B measurements are streaming: at a "
    "fixed per-replica offered rate, aggregate successful throughput grows from "
    "0.7 query/s at 2 replicas to 31.0 query/s at 128 replicas (4 to 256 nodes) — 67% "
    "weak-scaling efficiency vs the 4-node base, sublinear rather than linear. Through a "
    "single HAProxy the service tracks the proxy-bypass diagnostic up to 64 nodes "
    "(9.3 query/s, 80%) before the same head-node streaming ceiling appears; a "
    "non-streaming 405B configuration has not been measured.", after=60,
))
blocks.append(figure(
    "fig7_pp405b.png",
    "Llama-3.1-405B (TP8 × PP2) weak scaling, 2–128 replicas at a fixed offered rate "
    "per replica. Point labels: successful throughput and weak-scaling efficiency vs "
    "the 4-node base.",
    5.2,
))
blocks.append(bold_lead("Robustness across workloads and models"))
blocks.append(body_par(
    "Holding the 8B baseline fixed and varying one axis at a time (each row’s offered "
    "rate pinned at 90% of its single-node saturation), SLO attainment — the fraction "
    "of requests meeting TTFT ≤ 2 s and P99 TBT ≤ 250 ms — holds at 64 nodes for "
    "long-context and moderate-rate workloads and for the 120B model. The two rows that "
    "degrade (the short/high-rate baseline and Poisson arrivals at the same mean) lose "
    "their latency margin at the saturation knee once 64 nodes share the streaming path: "
    "the failure is load-shape specific, not model- or workload-specific.", after=60,
))
blocks.append(data_table([
    ["Perturbation", "attain (N=1)", "attain (N=64)", "Δ"],
    ["(baseline) 8B, 64/64, fixed-interval", "0.91", "0.43", "−0.48"],
    ["Workload: ShareGPT 2K/2K", "1.00", "0.90", "−0.10"],
    ["Workload: ShareGPT 4K/4K", "1.00", "1.00", "0.00"],
    ["Workload: Code (HumanEval)", "1.00", "1.00", "0.00"],
    ["Workload: Chat (ShareGPT natural)", "0.99", "0.99", "0.00"],
    ["Workload: Summarization", "1.00", "1.00", "0.00"],
    ["Model: 120B (TP=8, rate 9)", "0.98", "0.97", "−0.01"],
    ["Arrival: Poisson", "0.89", "0.35", "−0.54"],
    ["Arrival: BurstGPT trace †", "0.46", "0.46", "0.00"],
], [4424, 2000, 2000, 1800]))
blocks.append(par(run(
    "† BurstGPT replays a fixed total arrival rate not scaled by N, so its N=64 cell is "
    "not a weak-scaling stress; reported for completeness.",
    sz=16, color=GRAYTXT), after=100))
blocks.append(bold_lead("Steady-state serving vs. startup time"))
blocks.append(body_par(
    "Everything above is steady-state serving, measured after the cluster reports ready. "
    "Cluster bring-up behaves differently: it does not scale, and this is Ray’s key "
    "limitation at HPC scale. Model staging (~50 s for the 8B model), Ray cluster start "
    "(~45 s), and first-request warm-up (~8 s) are roughly flat in node count, but "
    "Ray Serve’s serve.run phase grows superlinearly — 171 s at 64 nodes, 470 s at 128, "
    "and 1,857 s (~31 min) at 256 — and a 512-node bring-up fails outright. The growth is "
    "concentrated in the proxy readiness wait: on every deployment broadcast, every "
    "Ray Serve proxy resolves every replica handle against the Ray control store (GCS), "
    "work that is quadratic in node count and reaches 1.38 million GCS lookups at 256 "
    "nodes. Budget PBS walltime as bring-up plus serving window (405B weight loading "
    "adds more), and reuse a running cluster across experiments where possible."
))
blocks.append(bold_lead("Launch-time knobs"))
blocks.append(body_par(
    "Environment variables read by the launcher select engines and staging behavior:",
    after=60,
))
blocks.append(data_table([
    ["Environment knob", "Effect"],
    ["AURORA_ENGINE=sglang", "SGLang instead of vLLM as the inference engine"],
    ["AURORA_PP_SHARD_AWARE=1",
     "Shard-aware multi-node pipeline-parallel staging and node-pinned replicas "
     "(405B-class models)"],
    ["AURORA_PP_UMBRELLA=1", "Single root-route ingress over the per-replica PP routes"],
    ["AURORA_NULL_COMPUTE=1",
     "Skip the engine and simulate latency — control-plane stress tests"],
    ["AURORA_CLEAN_STAGE=1", "Wipe node-local staged weights first (cold-start timing)"],
], [3300, 6924]))
blocks.append(body_par(
    "Scale cliffs and their fixes (Ray/vLLM patches at 256+ nodes, thread-pool clamps) "
    "are catalogued in doc/KNOWN_ISSUES.md; the launcher applies the fixes automatically."
))

# 6 Clients
blocks.append(heading(6, "Serving agents and OpenAI-compatible clients"))
blocks.append(body_par(
    "The deployment exposes the standard OpenAI API, so any compatible client works unchanged "
    "— LangChain’s ChatOpenAI, Academy LLM agents, litellm, or the openai SDK — "
    "by pointing the usual environment variables at the service URL. This makes the framework "
    "the self-hosted, on-machine complement to a hosted gateway such as MAG: the same agent "
    "code runs against either.", after=60,
))
blocks.append(code_block(
    "export OPENAI_BASE_URL=http://<head_node>:4001/v1\n"
    "export OPENAI_API_KEY=EMPTY"
))
blocks.append(body_par(
    "There is no authentication by default; use proxy_config.type: litellm to add API keys, "
    "rate limiting, and usage tracking."
))

# 7 Examples and Resources
blocks.append(heading(7, "Examples and Resources"))
blocks.append(body_par(
    "Runnable configurations and benchmark specifications live in the repository; code is "
    "linked rather than copied here. Paths are relative to "
    "github.com/wenyiwang-us/ExaServe."
))
blocks.append(data_table([
    ["Resource", "Location"],
    ["Source code", "github.com/wenyiwang-us/ExaServe"],
    ["Documentation", "README.md — install, configuration, console-script reference"],
    ["Machine-actionable card",
     "doc/exaserve.md — markdown version of this card for coding agents"],
    ["Deployment templates",
     "examples/ — HAProxy, LiteLLM, and a fully annotated reference config"],
    ["Benchmark specs",
     "eval/specs/refcard/ — the smoke, weak-scaling, and 405B pipeline-parallel "
     "specs referenced in this card"],
    ["Scaling analyses",
     "findings/ and doc/KNOWN_ISSUES.md — root-cause write-ups, scale cliffs and "
     "fixes"],
], [2800, 7424]))

# 8 Citation
blocks.append(heading(8, "Citation"))
blocks.append(body_par(
    "The system and its 1–256-node evaluation are described in an SC26 workshop "
    "paper (in preparation):", after=60,
))
blocks.append(code_block(
    "@misc{wang2026exaserve,\n"
    "    title = {ExaServe: Deploying and Measuring Large-Scale\n"
    "             Ray Serve for LLM Inference on Aurora System},\n"
    "    author = {Wenyi Wang and Shu Shi and Yadu Nand Babuji and\n"
    "              Ian Foster and Kyle Chard},\n"
    "    note = {SC26 workshop paper, in preparation},\n"
    "    year = {2026}\n"
    "}"
))

# ------------------------------------------------------------- assemble docx
with zipfile.ZipFile(SRC) as z:
    old_doc = z.read("word/document.xml").decode("utf-8")

sectpr = re.search(r"<w:sectPr.*?</w:sectPr>", old_doc, re.S).group(0)
header = old_doc[: old_doc.index("<w:body>") + len("<w:body>")]
new_doc = header + "".join(blocks) + sectpr + "</w:body></w:document>"

rel_entries = "".join(
    f'<Relationship Id="{rid}" '
    'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" '
    f'Target="{target}"/>'
    for target, _, rid in _images
)

with zipfile.ZipFile(SRC) as zin, zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as zout:
    for item in zin.infolist():
        data = zin.read(item.filename)
        if item.filename == "word/document.xml":
            data = new_doc.encode("utf-8")
        elif item.filename == "word/_rels/document.xml.rels":
            data = data.decode("utf-8").replace(
                "</Relationships>", rel_entries + "</Relationships>").encode("utf-8")
        elif item.filename.startswith("word/footer"):
            data = data.replace(b"Academy", b"ExaServe")
        zout.writestr(item, data)
    for target, src, _ in _images:
        zout.writestr(f"word/{target}", open(src, "rb").read())

print("wrote", OUT, f"({len(_images)} figures embedded)")
