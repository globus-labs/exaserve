from pathlib import Path
from typing import Any, Dict, List


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
        summary = point["summary"]
        lines.append(
            "| {point_id} | {requested_rps:.2f} | {achieved_rps:.2f} | {diagnosis} | {queue_fraction:.3f} | {safe_budget} |".format(
                point_id=point["point_id"],
                requested_rps=summary["requested_rps"],
                achieved_rps=summary["achieved_rps"],
                diagnosis=summary["diagnosis"],
                queue_fraction=summary["queue_fraction"],
                safe_budget=summary["safe_active_budget_estimate"],
            )
        )
    lines.extend(["", "## Key Questions", ""])
    lines.extend(render_key_questions(points))
    return "\n".join(lines) + "\n"


def render_key_questions(points):
    if not points:
        return ["No completed points were available."]
    by_active = sorted(points, key=lambda item: (item["run_config"]["client"]["max_active_requests"], item["summary"]["requested_rps"]))
    first = by_active[0]
    last = by_active[-1]
    delta_rps = last["summary"]["achieved_rps"] - first["summary"]["achieved_rps"]
    concurrency_answer = (
        f"- What changed when concurrency increased? Achieved RPS changed by `{delta_rps:.2f}` "
        f"between active budgets `{first['run_config']['client']['max_active_requests']}` and "
        f"`{last['run_config']['client']['max_active_requests']}`, while queue fraction moved from "
        f"`{first['summary']['queue_fraction']:.3f}` to `{last['summary']['queue_fraction']:.3f}`."
    )
    bottleneck_answer = (
        f"- Was the client stalled by its own queue, transport, the server, or the network? "
        f"The strongest observed diagnosis in the completed study was `{last['summary']['diagnosis']}`."
    )
    safe_budget_answer = (
        f"- What active-request and connection budget is safe for this regime? "
        f"A conservative estimate from the completed points is "
        f"`{last['summary']['safe_active_budget_estimate']}` active requests."
    )
    return [concurrency_answer, bottleneck_answer, safe_budget_answer]


def write_report(path, content):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
