import math
from pathlib import Path
from typing import Dict, List


PLOT_COLORS = [
    "#0f766e",
    "#b45309",
    "#1d4ed8",
    "#be123c",
    "#4338ca",
    "#4d7c0f",
]


def _safe_float(value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(result) or math.isinf(result):
        return 0.0
    return result


def _point_label(point):
    axis_values = point.get("axis_values", {})
    if axis_values:
        return " ".join("%s=%s" % (k, v) for k, v in sorted(axis_values.items()))
    run_config = point.get("run_config", {})
    client_cfg = run_config.get("client", {})
    active = client_cfg.get("max_active_requests")
    rate = client_cfg.get("rate")
    if active is not None and rate is not None:
        return "a%s-r%s" % (active, rate)
    if active is not None:
        return "a%s" % active
    return point.get("point_id", "point")


def _summary_value(summary, key):
    current = summary
    for part in key.split("."):
        if not isinstance(current, dict):
            return 0.0
        current = current.get(part, 0.0)
    return current


def _series_for_points(points, key):
    values = []
    for point in points:
        summary = point.get("summary", {})
        values.append(_safe_float(_summary_value(summary, key)))
    return values


def _svg_escape(value):
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def render_line_plot_svg(title, labels, series_map, y_label):
    max_label_len = max((len(str(l)) for l in labels), default=0)
    width = 960
    margin_left = 68
    margin_right = 24
    margin_top = 42
    margin_bottom = max(86, 40 + max_label_len * 5)
    height = 420 - 86 + margin_bottom
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    x_count = max(len(labels), 1)
    x_step = plot_width / max(x_count-1, 1)
    all_values = []
    for values in series_map.values():
        all_values.extend(_safe_float(value) for value in values)
    ymax = max(all_values) if all_values else 1.0
    if ymax <= 0:
        ymax = 1.0
    ymax *= 1.10

    def x_pos(index):
        return margin_left + (x_step * index if x_count > 1 else plot_width / 2.0)

    def y_pos(value):
        normalized = _safe_float(value) / ymax
        normalized = min(max(normalized, 0.0), 1.0)
        return margin_top + plot_height - (normalized * plot_height)

    lines = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">' % (width, height, width, height),
        '<rect x="0" y="0" width="%d" height="%d" fill="white" />' % (width, height),
        '<text x="%d" y="24" font-size="18" font-family="sans-serif" font-weight="700">%s</text>' % (margin_left, _svg_escape(title)),
        '<text x="16" y="%d" font-size="12" font-family="sans-serif" transform="rotate(-90 16,%d)">%s</text>' % (
            margin_top + plot_height / 2.0,
            margin_top + plot_height / 2.0,
            _svg_escape(y_label),
        ),
        '<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#111827" stroke-width="1.5" />' % (
            margin_left,
            margin_top + plot_height,
            margin_left + plot_width,
            margin_top + plot_height,
        ),
        '<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#111827" stroke-width="1.5" />' % (
            margin_left,
            margin_top,
            margin_left,
            margin_top + plot_height,
        ),
    ]

    for tick_idx in range(6):
        value = ymax * tick_idx / 5.0
        y = y_pos(value)
        lines.append(
            '<line x1="%d" y1="%.2f" x2="%d" y2="%.2f" stroke="#e5e7eb" stroke-width="1" />'
            % (margin_left, y, margin_left + plot_width, y)
        )
        lines.append(
            '<text x="%d" y="%.2f" font-size="11" font-family="monospace" text-anchor="end" dominant-baseline="middle">%.3f</text>'
            % (margin_left - 8, y, value)
        )

    for idx, label in enumerate(labels):
        x = x_pos(idx)
        lines.append(
            '<line x1="%.2f" y1="%d" x2="%.2f" y2="%d" stroke="#d1d5db" stroke-width="1" />'
            % (x, margin_top, x, margin_top + plot_height)
        )
        label_y = margin_top + plot_height + 14
        lines.append(
            '<text x="%.2f" y="%d" font-size="10" font-family="monospace" text-anchor="end" transform="rotate(-45 %.2f,%d)">%s</text>'
            % (x + 2, label_y, x + 2, label_y, _svg_escape(label))
        )

    for series_idx, name in enumerate(sorted(series_map)):
        values = series_map[name]
        color = PLOT_COLORS[series_idx % len(PLOT_COLORS)]
        points = ["%.2f,%.2f" % (x_pos(idx), y_pos(value)) for idx, value in enumerate(values)]
        lines.append(
            '<polyline fill="none" stroke="%s" stroke-width="2.5" points="%s" />'
            % (color, " ".join(points))
        )
        for idx, value in enumerate(values):
            lines.append(
                '<circle cx="%.2f" cy="%.2f" r="3.5" fill="%s"><title>%s: %.6f</title></circle>'
                % (x_pos(idx), y_pos(value), color, _svg_escape(name), _safe_float(value))
            )

    legend_x = margin_left + plot_width - 180
    legend_y = margin_top + 6
    for series_idx, name in enumerate(sorted(series_map)):
        color = PLOT_COLORS[series_idx % len(PLOT_COLORS)]
        y = legend_y + series_idx * 18
        lines.append('<rect x="%d" y="%d" width="10" height="10" fill="%s" />' % (legend_x, y, color))
        lines.append(
            '<text x="%d" y="%d" font-size="11" font-family="sans-serif" dominant-baseline="hanging">%s</text>'
            % (legend_x + 16, y - 1, _svg_escape(name))
        )

    lines.append("</svg>")
    return "\n".join(lines) + "\n"


def generate_plots(study_dir, points):
    plot_dir = Path(study_dir) / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    def _sort_key(point):
        axis_values = point.get("axis_values", {})
        if axis_values:
            return tuple(_safe_float(v) for _, v in sorted(axis_values.items()))
        return (
            int(point.get("run_config", {}).get("client", {}).get("max_active_requests", 0)),
            float(point.get("summary", {}).get("requested_rps", 0.0)),
        )

    ordered_points = sorted(points, key=_sort_key)
    labels = [_point_label(point) for point in ordered_points]
    plots = {}

    plot_specs = [
        (
            "throughput.svg",
            "Throughput",
            {"achieved_rps": _series_for_points(ordered_points, "achieved_rps")},
            "RPS",
        ),
        (
            "queue_fraction.svg",
            "Queue Fraction",
            {"queue_fraction": _series_for_points(ordered_points, "queue_fraction")},
            "fraction",
        ),
        (
            "phase_means.svg",
            "Client Phase Means",
            {
                "dispatch_lag_s": _series_for_points(ordered_points, "dispatch_lag_mean_s"),
                "queue_wait_s": _series_for_points(ordered_points, "queue_wait_mean_s"),
                "time_to_headers_s": _series_for_points(ordered_points, "time_to_headers_mean_s"),
                "body_read_s": _series_for_points(ordered_points, "body_read_mean_s"),
                "connect_s": _series_for_points(ordered_points, "connect_mean_s"),
            },
            "seconds",
        ),
        (
            "connection_behavior.svg",
            "Connection Behavior",
            {
                "churn_ratio": _series_for_points(ordered_points, "connection_churn_ratio"),
                "reuse_ratio": _series_for_points(ordered_points, "reuse_ratio"),
            },
            "ratio",
        ),
        (
            "target_pressure.svg",
            "Target Pressure",
            {
                "target_queue_peak": _series_for_points(ordered_points, "target_queue_peak"),
                "target_rejections": _series_for_points(ordered_points, "target_rejections"),
                "target_error_fraction": _series_for_points(ordered_points, "target_error_fraction"),
            },
            "mixed units",
        ),
    ]

    network_values = _series_for_points(ordered_points, "network.max_bandwidth_fraction")
    if any(value > 0 for value in network_values):
        plot_specs.append(
            (
                "network_utilization.svg",
                "Network Utilization",
                {"max_bandwidth_fraction": network_values},
                "fraction of 25 GB/s",
            )
        )

    for filename, title, series_map, y_label in plot_specs:
        svg = render_line_plot_svg(title, labels, series_map, y_label)
        path = plot_dir / filename
        path.write_text(svg, encoding="utf-8")
        plots[filename[:-4]] = str(path)

    return plots
