"""Plotly figures in the Floodlight style.

Ported from the basketball dashboard (lib/viz.py: _layout, PLOTLY_CONFIG,
percentile_bars, compare_dumbbell, _empty). Chart rules: transparent
background, Inter, small margins, no modebar, every axis fixedrange (with
no modebar a zoom could not be undone), GRID hairlines, hovertemplates
ending in <extra></extra>. Text uses the ink tokens, never a series colour.
"""

from __future__ import annotations

import plotly.graph_objects as go

from src.ui import stats
from src.ui import theme as T

PLOTLY_CONFIG = {"displayModeBar": False}
ROW_PX = 38            # percentile bar row height
DUMBBELL_ROW_PX = 34
LINK = "rgba(236,242,237,0.22)"  # dumbbell connector


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


def percentile_bars(rows: list[tuple[str, float, str]]) -> go.Figure:
    """Horizontal 0-100 percentile bars, one per metric, coloured by band.

    The label reads "value · 87th · Elite": the raw value because a
    percentile alone hides what it means, and the band in words so colour
    is never the only cue."""
    if not rows:
        return _empty("Not enough players in the pool to rank against")
    labels = [r[0] for r in rows]
    pcts = [float(r[1]) for r in rows]
    values = [r[2] for r in rows]
    bands = [stats.band(p) for p in pcts]
    ords = [stats.pct_label(p) for p in pcts]

    fig = go.Figure()
    fig.add_bar(
        y=labels, x=pcts, orientation="h",
        marker=dict(color=[T.PCT_RAMP[b.index] for b in bands], cornerradius=2),
        text=[f"{v} · {o} · {b.label}" for v, o, b in zip(values, ords, bands)],
        textposition="outside", cliponaxis=False,
        textfont=dict(color=T.INK_2, size=11, family=T.FONT_BODY),
        customdata=[[v, o, b.label] for v, o, b in zip(values, ords, bands)],
        hovertemplate="%{y}: %{customdata[0]}<br>%{customdata[1]} percentile · %{customdata[2]}<extra></extra>",
        showlegend=False,
    )
    fig.add_vline(x=50, line_color=T.MUTED, line_width=1, line_dash="dot")
    fig.update_layout(**_layout(
        height=ROW_PX * len(rows) + 60,
        bargap=0.34,
        # headroom past 100 so the outside labels are never clipped
        xaxis=_axis(range=[0, 165], tickvals=[0, 25, 50, 75, 100],
                    title=dict(text="Percentile in position pool", font=dict(color=T.MUTED, size=11))),
        yaxis=_axis(autorange="reversed", showgrid=False, tickfont=dict(color=T.INK_2, size=12)),
    ))
    return fig


def compare_dumbbell(metrics: list[str], a_vals: list[float], b_vals: list[float],
                     a_name: str, b_name: str) -> go.Figure:
    """Two players' percentiles on the same metrics: dots joined by a line, so
    the gap is the first thing you read."""
    if not metrics:
        return _empty("No shared metrics to compare")
    fig = go.Figure()
    for metric, a, b in zip(metrics, a_vals, b_vals):
        fig.add_scatter(x=[a, b], y=[metric, metric], mode="lines",
                        line=dict(color=LINK, width=2), hoverinfo="skip", showlegend=False)
    for vals, name, color in ((a_vals, a_name, T.PLAYER), (b_vals, b_name, T.COMPARE)):
        fig.add_scatter(
            x=list(vals), y=list(metrics), mode="markers", name=name,
            marker=dict(color=color, size=12, line=dict(color=T.SURFACE, width=2)),
            customdata=[stats.pct_label(v) for v in vals],
            hovertemplate=name + "<br>%{y}: %{customdata} percentile<extra></extra>",
        )
    # Direct labels on the first row, in ink, each on the outer side of its dot.
    a0, b0 = a_vals[0], b_vals[0]
    a_left = a0 <= b0
    for value, name, is_left in ((a0, a_name, a_left), (b0, b_name, not a_left)):
        fig.add_annotation(x=value, y=metrics[0], text=name, showarrow=False, yshift=16,
                           xanchor="right" if is_left else "left",
                           font=dict(color=T.INK_2, size=11))
    fig.add_vline(x=50, line_color=T.MUTED, line_width=1, line_dash="dot")
    fig.update_layout(**_layout(
        height=max(220, DUMBBELL_ROW_PX * len(metrics) + 90),
        showlegend=True,
        margin=dict(l=8, r=8, t=40, b=8),
        xaxis=_axis(range=[0, 100], tickvals=[0, 25, 50, 75, 100],
                    title=dict(text="Percentile in position pool", font=dict(color=T.MUTED, size=11))),
        yaxis=_axis(autorange="reversed", showgrid=False, tickfont=dict(color=T.INK_2, size=11)),
    ))
    return fig
