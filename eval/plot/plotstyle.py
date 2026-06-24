"""Shared plotting style for the Aurora Ray Serve eval figures.

Single source of truth so the SAME experiment type looks identical across every
figure:
  * colour + marker  -> dispatch/proxy (direct, haproxy, envoy, rayserve, litellm)
  * line style       -> mode            (stream = solid, non-stream = dashed)

Global look-and-feel (fonts, grid, sizes) lives in eval/plot/matplotlibrc, loaded
by apply(). Per-series choices live here. Helpers:
  apply()                      -> load rc, return pyplot
  proxy_kw(proxy, mode)        -> dict(color, marker, linestyle) for plot/errorbar
  line(ax, xs, ys, proxy, ...) -> consistent (error-bar) line
  AnnotationStacker(ax)        -> value labels that STACK when they collide;
                                  dashed box border for dashed (non-stream) series
  titles(fig, title, subtitle) -> bold title + multi-line italic subtitle
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import numpy as np


# --- registries -------------------------------------------------------------

PROXY_COLORS = {
    "direct": "#1b9e77",
    "haproxy": "#d95f02",
    "envoy": "#7570b3",
    "rayserve": "#e7298a",
    "litellm": "#66a61e",
}
PROXY_MARKERS = {
    "direct": "o", "haproxy": "s", "envoy": "^", "rayserve": "D", "litellm": "v",
}
# Pretty display names for legends / axis ticks.
PROXY_LABEL = {
    "direct": "Direct", "haproxy": "HAProxy", "envoy": "Envoy",
    "rayserve": "RayServe", "litellm": "LiteLLM",
}
# mode -> line style (consistent everywhere)
MODE_LS = {"stream": "-", "nonstream": "--"}
MODE_LABEL = {"stream": "streaming", "nonstream": "non-stream"}


def plabel(proxy, mode=None):
    name = PROXY_LABEL.get(proxy, proxy)
    return f"{name} ({MODE_LABEL[mode]})" if mode else name

MS, LW, MEW = 9, 2.6, 1.8


def apply():
    import matplotlib
    matplotlib.use("Agg")
    rc = Path(__file__).resolve().parent / "matplotlibrc"
    if rc.exists():
        matplotlib.rc_file(rc)
    import matplotlib.pyplot as plt
    return plt


def proxy_kw(proxy, mode="stream"):
    return dict(color=PROXY_COLORS[proxy], marker=PROXY_MARKERS[proxy],
                linestyle=MODE_LS[mode])


def is_dashed(mode):
    return MODE_LS.get(mode, "-") != "-"


def line(ax, xs, ys, proxy, mode="stream", label=None, yerr=None, alpha=0.95,
         lw=LW, zorder=3):
    """Consistent (optionally error-barred) line for a proxy/mode series."""
    kw = proxy_kw(proxy, mode)
    if yerr is not None:
        return ax.errorbar(xs, ys, yerr=yerr, label=label, lw=lw, markersize=MS,
                           markerfacecolor=kw["color"], markeredgecolor="white",
                           markeredgewidth=MEW, alpha=alpha, zorder=zorder,
                           capsize=4, capthick=1.4, **kw)
    return ax.plot(xs, ys, label=label, lw=lw, markersize=MS,
                   markerfacecolor=kw["color"], markeredgecolor="white",
                   markeredgewidth=MEW, alpha=alpha, zorder=zorder, **kw)


def legend(ax, **kw):
    kw.setdefault("fontsize", 9)
    leg = ax.legend(**kw)
    if leg:
        leg.get_frame().set_linewidth(1.4)
    return leg


def plain_log_y(ax):
    from matplotlib.ticker import ScalarFormatter
    f = ScalarFormatter(); f.set_scientific(False)
    ax.yaxis.set_major_formatter(f); ax.yaxis.set_minor_formatter(f)


# --- annotations that stack on collision ------------------------------------

class AnnotationStacker:
    """Collect value labels per axis; at render, labels sharing an x position are
    STACKED into a vertical column (ordered by value) so overlapping series are
    individually legible and you can see several lines meet there. Dashed-line
    (non-stream) series get a dashed box border."""

    def __init__(self, ax, fmt="{:.2f}", fontsize=7.5):
        self.ax = ax
        self.fmt = fmt
        self.fs = fontsize
        self._items = []  # (x, y, color, dashed, text)

    def add(self, x, y, color, dashed=False, text=None):
        if y is None or (isinstance(y, float) and np.isnan(y)):
            return
        self._items.append((x, y, color, dashed,
                             text if text is not None else self.fmt.format(y)))

    def add_series(self, xs, ys, color, dashed=False):
        for x, y in zip(xs, ys):
            self.add(x, y, color, dashed)

    def render(self, base_pts=8, min_gap_pts=None):
        """Place labels just off their point; bump apart ONLY when they'd collide
        (within a shared-x group). Stacks upward normally, but flips a group to
        stack DOWNWARD when stacking up would run past the axes ceiling (so labels
        never cover the title). MUST be called after the layout is final (see
        finalize()), so transData reflects the drawn positions."""
        if not self._items:
            return
        ax = self.ax
        if min_gap_pts is None:
            min_gap_pts = self.fs + 5.0
        px_to_pt = 72.0 / ax.figure.dpi
        y_top = ax.transAxes.transform((0, 1))[1] * px_to_pt
        groups = defaultdict(list)
        for it in self._items:
            groups[round(float(it[0]), 6)].append(it)

        def emit(x, y, color, dashed, text, off, va):
            ax.annotate(text, (x, y), textcoords="offset points", xytext=(0, off),
                        ha="center", va=va, fontsize=self.fs, fontweight="bold",
                        color=color, zorder=6,
                        bbox=dict(boxstyle="round,pad=0.25", fc="white", ec=color,
                                  lw=1.1, ls="--" if dashed else "-", alpha=0.92))

        for _, its in groups.items():
            ypts = {id(it): ax.transData.transform((it[0], it[1]))[1] * px_to_pt for it in its}
            up = sorted(its, key=lambda it: ypts[id(it)])     # ascending
            tops, last = [], None
            for it in up:
                t = ypts[id(it)] + base_pts
                if last is not None and t < last + min_gap_pts:
                    t = last + min_gap_pts
                last = t; tops.append(t)
            if not tops or max(tops) <= y_top - (self.fs + 4):
                for it, t in zip(up, tops):       # fits: stack upward
                    emit(*it, off=t - ypts[id(it)], va="bottom")
            else:                                  # would hit ceiling: stack downward
                last = None
                for it in sorted(its, key=lambda it: -ypts[id(it)]):  # descending
                    b = ypts[id(it)] - base_pts
                    if last is not None and b > last - min_gap_pts:
                        b = last - min_gap_pts
                    last = b
                    emit(*it, off=b - ypts[id(it)], va="top")


def finalize(fig, stackers, out, rect=None):
    """Lay out, THEN render value-label stackers against the final transform,
    then save. Stackers rendered post-layout so collision detection is accurate."""
    import matplotlib.pyplot as plt
    fig.tight_layout(rect=rect) if rect else fig.tight_layout()
    fig.canvas.draw()
    for s in stackers:
        s.render()
    fig.savefig(out)
    plt.close(fig)
    return out


# --- titles -----------------------------------------------------------------

def titles(fig, title, subtitle):
    """Bold title + multi-line italic subtitle, placed by absolute inches from
    the top so it works at any figure height. `subtitle` may be a string (split
    on ' · ' / newlines) or a list of lines. Returns rect-top for tight_layout."""
    if isinstance(subtitle, str):
        lines = [s.strip() for s in subtitle.replace("\n", "·").split("·") if s.strip()]
    else:
        lines = list(subtitle)
    H = fig.get_size_inches()[1]
    fig.suptitle(title, fontsize=15, fontweight="bold", color="#1a1a1a", y=1 - 0.30 / H)
    y = 1 - 0.62 / H
    for ln in lines:
        fig.text(0.5, y, ln, ha="center", fontsize=9.5, style="italic", color="#555555")
        y -= 0.26 / H
    # reserve header room (inches): title band + each subtitle line + a gap
    return 1 - (0.78 + 0.26 * len(lines)) / H
