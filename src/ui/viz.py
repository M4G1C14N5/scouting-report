"""Plotly figures in the charcoal style.

Ported from the basketball dashboard (lib/viz.py: _layout, PLOTLY_CONFIG,
percentile_bars, compare_dumbbell, _empty). Chart rules: transparent
background, Inter, small margins, no modebar, every axis fixedrange (with
no modebar a zoom could not be undone), GRID hairlines, hovertemplates
ending in <extra></extra>. Text uses the ink tokens, never a series colour.
"""

from __future__ import annotations

import html

import plotly.graph_objects as go

from src.ui import stats
from src.ui import theme as T

PLOTLY_CONFIG = {"displayModeBar": False}
ROW_PX = 46            # percentile bar row height (room for the larger value label)
DUMBBELL_ROW_PX = 34
LINK = "rgba(238,240,242,0.22)"  # dumbbell connector
LABEL_SHIFT = 14       # px above/below a dot for its direct label
LABEL_X_MIN, LABEL_X_MAX = 10, 90  # keep centred labels inside the plot
# Percentile-bar x range upper bound: the bars use 0-100, the rest is room for
# the two-line value label ("654.36 · 99th" over "Elite") drawn past the bar end.
# Lower gives the 0-100 track (and its five ticks) more width; too low and a
# label on a 99th-percentile bar is cut off at the card edge on a half-width card.
PCT_X_MAX = 185
# Metric names longer than this wrap onto two lines, so one long name
# ("Progressive Pass Distance / 90") cannot squeeze the plot of its whole card.
PCT_LABEL_WRAP = 20
VALUE_SIZE = 13     # px, the value label beside each percentile bar
VALUE_WEIGHT = 600  # semibold: the number reads first without shouting
NAME_SIZE = 13      # px, the metric names on the percentile bars' y axis


def _axis(**overrides) -> dict:
    # automargin reserves room for tick labels; without it long metric
    # names get clipped by the small fixed margins.
    base = dict(gridcolor=T.GRID, zeroline=False, linecolor=T.BORDER,
                tickfont=dict(color=T.MUTED, size=11), fixedrange=True, automargin=True)
    base.update(overrides)
    return base


def _layout(**overrides) -> dict:
    base = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=T.FONT_BODY, color=T.INK_2, size=12),
        margin=dict(l=8, r=8, t=8, b=8),
        dragmode=False,
        xaxis=_axis(),
        yaxis=_axis(),
        hoverlabel=dict(bgcolor=T.SURFACE_2, bordercolor=T.BORDER,
                        font=dict(family=T.FONT_BODY, color=T.INK)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(color=T.INK_2)),
        showlegend=False,
        height=320,
    )
    base.update(overrides)
    return base


def _empty(message: str) -> go.Figure:
    """A sentence instead of an empty axis box."""
    fig = go.Figure()
    fig.add_annotation(text=message, showarrow=False, font=dict(color=T.MUTED, size=13),
                       xref="paper", yref="paper", x=0.5, y=0.5)
    fig.update_layout(**_layout(height=160, xaxis=dict(visible=False, fixedrange=True),
                                yaxis=dict(visible=False, fixedrange=True)))
    return fig


def _wrap_label(text: str, width: int = PCT_LABEL_WRAP) -> str:
    """Split a long axis label at the space nearest its middle ("a b c d" -> "a b<br>c d")."""
    if len(text) <= width or " " not in text:
        return text
    mid = len(text) / 2
    cut = min((i for i, ch in enumerate(text) if ch == " "), key=lambda i: abs(i - mid))
    return f"{text[:cut]}<br>{text[cut + 1:]}"


def percentile_bars(rows: list[tuple[str, float | None, str]]) -> go.Figure:
    """Horizontal 0-100 percentile bars, one per metric, coloured by band.

    The label reads "value · 87th" with the band ("Elite") on a second line:
    the raw value because a percentile alone hides what it means, and the
    band in words so colour is never the only cue. A row with no percentile
    (None: the stat is missing for this player) keeps its place, with no bar
    and "n/a" as its label."""
    if not rows:
        return _empty("Not enough players in the pool to rank against")
    labels = [r[0] for r in rows]
    pcts = [None if r[1] is None else float(r[1]) for r in rows]
    values = [r[2] for r in rows]
    x, colours, texts, custom = [], [], [], []
    for name, pct, value in zip(labels, pcts, values):
        if pct is None:
            x.append(0)
            colours.append(T.PCT_RAMP[0])
            texts.append("n/a")
            custom.append(["n/a", "not available for this player", "", name])
            continue
        b, o = stats.band(pct), stats.pct_label(pct)
        x.append(pct)
        colours.append(T.PCT_RAMP[b.index])
        # two lines ("0.64 · 92nd" over "Elite"): one long line gets clipped at
        # the card edge when the metric names are long and the card is half width
        # the value leads in semibold so the number reads before the bar; the
        # band in words sits under it, muted
        texts.append(f"<span style='font-weight:{VALUE_WEIGHT}'>{value}</span> · {o}"
                     f"<br><span style='color:{T.MUTED}'>{b.label}</span>")
        custom.append([value, f"{o} percentile", f" · {b.label}", name])

    fig = go.Figure()
    fig.add_bar(
        y=[_wrap_label(name) for name in labels], x=x, orientation="h",
        marker=dict(color=colours, cornerradius=2),
        text=texts,
        textposition="outside", cliponaxis=False,
        # keep the label at full size: by default Plotly shrinks outside text to the bar's thickness
        constraintext="none",
        textfont=dict(color=T.INK, size=VALUE_SIZE, family=T.FONT_BODY),
        customdata=custom,
        hovertemplate=("%{customdata[3]}: %{customdata[0]}<br>"
                       "%{customdata[1]}%{customdata[2]}<extra></extra>"),
        showlegend=False,
    )
    fig.add_vline(x=50, line_color=T.MUTED, line_width=1, line_dash="dot")
    fig.update_layout(**_layout(
        height=ROW_PX * len(rows) + 60,
        bargap=0.62,  # thin bars: the numbers carry the reading, the bar shows where it sits
        # headroom past 100 (PCT_X_MAX) so the outside labels are never clipped
        # tickangle 0: a long metric name narrows the plot and Plotly would rotate the ticks
        xaxis=_axis(range=[0, PCT_X_MAX], tickvals=[0, 25, 50, 75, 100], tickangle=0,
                    title=dict(text="Percentile in position pool", font=dict(color=T.MUTED, size=11))),
        yaxis=_axis(autorange="reversed", showgrid=False, tickfont=dict(color=T.INK_2, size=NAME_SIZE)),
    ))
    return fig


def compare_dumbbell(metrics: list[str], a_vals: list[float], b_vals: list[float],
                     a_name: str, b_name: str) -> go.Figure:
    """Two players' percentiles on the same metrics: dots joined by a line, so
    the gap is the first thing you read."""
    if not (len(metrics) == len(a_vals) == len(b_vals)):
        raise ValueError(
            f"compare_dumbbell needs equal-length inputs, got {len(metrics)} metrics, "
            f"{len(a_vals)} values for {a_name} and {len(b_vals)} for {b_name}")
    if not metrics:
        return _empty("No shared metrics to compare")
    # Plotly renders "<" as markup in names, so escape what we pass as text.
    a_name, b_name = html.escape(a_name), html.escape(b_name)
    fig = go.Figure()
    for metric, a, b in zip(metrics, a_vals, b_vals):
        fig.add_scatter(x=[a, b], y=[metric, metric], mode="lines",
                        line=dict(color=LINK, width=2), hoverinfo="skip", showlegend=False)
    hover = "%{fullData.name}<br>%{y}: %{customdata} percentile<extra></extra>"
    # B is a hollow ring drawn first, A a smaller filled dot drawn on top: tied
    # or near-tied percentiles show the ring around the dot instead of one
    # marker hiding the other. legendrank keeps the legend reading A then B.
    fig.add_scatter(
        x=list(b_vals), y=list(metrics), mode="markers", name=b_name, legendrank=2,
        marker=dict(symbol="circle-open", color=T.COMPARE, size=17, line=dict(color=T.COMPARE, width=2.5)),
        customdata=[stats.pct_label(v) for v in b_vals], hovertemplate=hover,
    )
    fig.add_scatter(
        x=list(a_vals), y=list(metrics), mode="markers", name=a_name, legendrank=1,
        marker=dict(symbol="circle", color=T.PLAYER, size=10, line=dict(color=T.SURFACE, width=1.5)),
        customdata=[stats.pct_label(v) for v in a_vals], hovertemplate=hover,
    )
    # Direct labels on the first row, in ink. Placement does not depend on the
    # pair: A's label sits above its dot, B's below, both centred, so two close
    # or tied dots never overlap their labels. The label x is clamped so centred
    # text cannot run past the plot edge (the dot keeps its true value).
    for value, name, yshift in ((a_vals[0], a_name, LABEL_SHIFT), (b_vals[0], b_name, -LABEL_SHIFT)):
        fig.add_annotation(x=min(max(value, LABEL_X_MIN), LABEL_X_MAX), y=metrics[0], text=name,
                           showarrow=False, xanchor="center", yshift=yshift,
                           font=dict(color=T.INK_2, size=11))
    fig.add_vline(x=50, line_color=T.MUTED, line_width=1, line_dash="dot")
    fig.update_layout(**_layout(
        height=max(220, DUMBBELL_ROW_PX * len(metrics) + 90),
        showlegend=True,
        hovermode="y unified",  # tied dots hover as one tooltip listing both players
        margin=dict(l=8, r=8, t=48, b=8),  # room for the first row's label above its dot
        # slight padding so the end dots (and their outline) are not cut at 0/100
        xaxis=_axis(range=[-4, 104], tickvals=[0, 25, 50, 75, 100],
                    title=dict(text="Percentile in position pool", font=dict(color=T.MUTED, size=11))),
        yaxis=_axis(autorange="reversed", showgrid=False, tickfont=dict(color=T.INK_2, size=11)),
    ))
    return fig
