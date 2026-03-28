from html import escape
from pathlib import Path


def render_report_html(study_manifest, points, envelope, plot_paths):
    study_name = escape(study_manifest["study"]["name"])
    suite = escape(study_manifest["study"]["suite"])
    mode = escape(study_manifest["execution"]["mode"])

    parts = [
        "<!doctype html>",
        "<html>",
        "<head>",
        '<meta charset="utf-8" />',
        "<title>ClientLab Report</title>",
        "<style>",
        "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 2rem auto; max-width: 1100px; color: #111827; }",
        "h1, h2 { margin-bottom: 0.4rem; }",
        ".meta { color: #4b5563; margin-bottom: 1.5rem; }",
        ".card { border: 1px solid #d1d5db; border-radius: 12px; padding: 1rem 1.25rem; margin: 1rem 0; background: #fafafa; }",
        "table { border-collapse: collapse; width: 100%; margin-top: 1rem; }",
        "th, td { border-bottom: 1px solid #e5e7eb; padding: 0.6rem; text-align: left; }",
        "th { background: #f3f4f6; }",
        "img { max-width: 100%; border: 1px solid #e5e7eb; border-radius: 10px; background: white; margin: 0.75rem 0; }",
        "code { background: #f3f4f6; padding: 0.1rem 0.3rem; border-radius: 4px; }",
        "</style>",
        "</head>",
        "<body>",
        "<h1>ClientLab Report: %s</h1>" % study_name,
        '<div class="meta">Suite: <code>%s</code> | Execution mode: <code>%s</code> | Point count: <code>%d</code></div>'
        % (suite, mode, len(points)),
        '<div class="card"><h2>Operating Envelope</h2><p>Max stable RPS: <code>%.2f</code><br/>Safe active budget estimate: <code>%s</code></p>'
        % (float(envelope.get("max_stable_rps", 0.0)), escape(str(envelope.get("safe_active_budget", 0)))),
    ]
    for note in envelope.get("notes", []):
        parts.append("<p>%s</p>" % escape(str(note)))
    parts.append("</div>")

    parts.append("<h2>Plots</h2>")
    for name in sorted(plot_paths):
        rel = Path(plot_paths[name]).name
        parts.append('<div class="card"><h3>%s</h3><img src="plots/%s" alt="%s" /></div>' % (escape(name.replace("_", " ").title()), escape(rel), escape(name)))

    parts.append("<h2>Point Summaries</h2>")
    parts.append("<table><thead><tr><th>Point</th><th>Requested RPS</th><th>Achieved RPS</th><th>Diagnosis</th><th>Queue Fraction</th><th>Safe Active Budget</th></tr></thead><tbody>")
    for point in points:
        summary = point["summary"]
        parts.append(
            "<tr><td><code>%s</code></td><td>%.2f</td><td>%.2f</td><td>%s</td><td>%.3f</td><td>%s</td></tr>"
            % (
                escape(point["point_id"]),
                float(summary["requested_rps"]),
                float(summary["achieved_rps"]),
                escape(summary["diagnosis"]),
                float(summary["queue_fraction"]),
                escape(str(summary["safe_active_budget_estimate"])),
            )
        )
    parts.append("</tbody></table>")
    parts.append("</body></html>")
    return "\n".join(parts) + "\n"


def write_html_report(path, content):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
