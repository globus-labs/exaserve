from pathlib import Path

from clientlab.utils import atomic_write_text


def render_report(study_manifest, points, envelope):
    lines = [
        f"# ClientLab Report: {study_manifest['study']['name']}",
        "",
        f"- Suite: `{study_manifest['study']['suite']}`",
        f"- Point count: `{len(points)}`",
        f"- Execution mode: `{study_manifest['execution']['mode']}`",
        "",
        "## Operating Envelope",
        "",
        f"- Max stable RPS: `{envelope['max_stable_rps']:.2f}`",
        f"- Safe active budget estimate: `{envelope['safe_active_budget']}`",
    ]
    for note in envelope.get("notes", []):
        lines.append(f"- {note}")
    lines.extend(
        [
            "",
            "## Point Summaries",
            "",
            "| Point | Requested RPS | Achieved RPS | Diagnosis | Queue Fraction | Safe Active Budget |",
            "|---|---:|---:|---|---:|---:|",
        ]
    )
    for point in points:
        summary = point.get("summary", {})
        lines.append(
            "| {point_id} | {requested_rps:.2f} | {achieved_rps:.2f} | {diagnosis} | {queue_fraction:.3f} | {safe_budget} |".format(
                point_id=point.get("point_id", "?"),
                requested_rps=float(summary.get("requested_rps", 0)),
                achieved_rps=float(summary.get("achieved_rps", 0)),
                diagnosis=summary.get("diagnosis", "error"),
                queue_fraction=float(summary.get("queue_fraction", 0)),
                safe_budget=summary.get("safe_active_budget_estimate", 0),
            )
        )
    lines.extend(["", "## Key Questions", ""])
    lines.extend(render_key_questions(points))
    return "\n".join(lines) + "\n"


def render_key_questions(points):
    valid_points = [p for p in points if p.get("summary", {}).get("diagnosis") != "error"]
    if not valid_points:
        return ["No completed points were available."]
    by_active = sorted(
        valid_points,
        key=lambda item: (
            item["run_config"]["client"]["max_active_requests"],
            item["summary"].get("requested_rps", 0),
        ),
    )
    first = by_active[0]
    last = by_active[-1]
    delta_rps = last["summary"].get("achieved_rps", 0) - first["summary"].get("achieved_rps", 0)
    concurrency_answer = (
        f"- What changed when concurrency increased? Achieved RPS changed by `{delta_rps:.2f}` "
        f"between active budgets `{first['run_config']['client']['max_active_requests']}` and "
        f"`{last['run_config']['client']['max_active_requests']}`, while queue fraction moved from "
        f"`{first['summary'].get('queue_fraction', 0):.3f}` to `{last['summary'].get('queue_fraction', 0):.3f}`."
    )
    bottleneck_answer = (
        f"- Was the client stalled by its own queue, transport, the server, or the network? "
        f"The strongest observed diagnosis in the completed study was `{last['summary'].get('diagnosis', '?')}`."
    )
    safe_budget_answer = (
        f"- What active-request and connection budget is safe for this regime? "
        f"A conservative estimate from the completed points is "
        f"`{last['summary'].get('safe_active_budget_estimate', 0)}` active requests."
    )
    return [concurrency_answer, bottleneck_answer, safe_budget_answer]


def write_report(path, content):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(target, content)
