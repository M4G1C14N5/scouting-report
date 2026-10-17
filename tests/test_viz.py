from __future__ import annotations

import pytest

from src.ui import theme, viz

ROWS = [("A", 10.0, "1"), ("B", 30.0, "2"), ("C", 60.0, "3"), ("D", 90.0, "4")]


def test_layout_is_transparent_inter_and_fixed():
    layout = viz._layout()
    assert layout["paper_bgcolor"] == "rgba(0,0,0,0)" and layout["plot_bgcolor"] == "rgba(0,0,0,0)"
    assert "Inter" in layout["font"]["family"]
    assert layout["xaxis"]["fixedrange"] and layout["yaxis"]["fixedrange"]
    assert layout["xaxis"]["gridcolor"] == theme.GRID
    assert layout["dragmode"] is False
    assert viz.PLOTLY_CONFIG == {"displayModeBar": False}


def test_percentile_bars_colour_each_bar_by_band():
    fig = viz.percentile_bars(ROWS)
    assert list(fig.data[0].marker.color) == list(theme.PCT_RAMP)


def test_percentile_bars_label_value_percentile_and_band_as_text():
    fig = viz.percentile_bars(ROWS)
    assert list(fig.data[0].text) == ["1 · 10th<br>Poor", "2 · 30th<br>Below avg",
                                      "3 · 60th<br>Above avg", "4 · 90th<br>Elite"]
    assert fig.data[0].hovertemplate.endswith("<extra></extra>")
    assert fig.data[0].textfont.color == theme.INK_2  # text never wears a series colour


def test_percentile_bars_size_axis_and_reference_line():
    fig = viz.percentile_bars(ROWS)
    assert fig.layout.height == viz.ROW_PX * 4 + 60
    assert tuple(fig.layout.xaxis.range) == (0, 215)  # headroom for the outside labels
    assert fig.layout.xaxis.fixedrange is True and fig.layout.yaxis.fixedrange is True
    assert fig.layout.yaxis.autorange == "reversed"
    assert any(shape.x0 == 50 for shape in fig.layout.shapes)


def test_percentile_bars_keep_x_ticks_horizontal():
    assert viz.percentile_bars(ROWS).layout.xaxis.tickangle == 0


def test_percentile_bars_show_only_0_50_100_ticks_horizontally():
    # five ticks crowd into "0 255075100" on narrow cards with long metric names
    xaxis = viz.percentile_bars(ROWS).layout.xaxis
    assert list(xaxis.tickvals) == [0, 50, 100]
    assert xaxis.tickangle == 0
    assert xaxis.title.text == "Percentile in position pool"


def test_percentile_bars_empty_state():
    fig = viz.percentile_bars([])
    assert fig.layout.annotations[0].text == "Not enough players in the pool to rank against"
    assert len(fig.data) == 0


def test_dumbbell_uses_player_and_compare_colours_with_legend():
    fig = viz.compare_dumbbell(["Goals / 90", "xG / 90"], [80.0, 40.0], [30.0, 70.0], "Salah", "Kane")
    markers = [t for t in fig.data if t.mode == "markers"]
    by_name = {t.name: t for t in markers}
    assert by_name["Salah"].marker.color == theme.PLAYER and by_name["Kane"].marker.color == theme.COMPARE
    assert [t.name for t in sorted(markers, key=lambda t: t.legendrank)] == ["Salah", "Kane"]
    assert fig.layout.showlegend is True
    assert all(t.hovertemplate.endswith("<extra></extra>") for t in markers)
    assert len([t for t in fig.data if t.mode == "lines"]) == 2  # one connector per metric


def test_dumbbell_direct_labels_on_first_row_in_ink():
    fig = viz.compare_dumbbell(["Goals / 90", "xG / 90"], [70.0, 40.0], [30.0, 70.0], "Salah", "Kane")
    labels = {a.text: a for a in fig.layout.annotations}
    assert set(labels) == {"Salah", "Kane"}
    assert all(a.y == "Goals / 90" for a in labels.values())
    assert all(a.font.color == theme.INK_2 for a in labels.values())
    assert all(a.xanchor == "center" for a in labels.values())
    assert labels["Salah"].yshift > 0 > labels["Kane"].yshift  # A above its dot, B below
    assert (labels["Salah"].x, labels["Kane"].x) == (70.0, 30.0)  # unclamped mid-range


def test_dumbbell_empty_state():
    fig = viz.compare_dumbbell([], [], [], "A", "B")
    assert fig.layout.annotations[0].text == "No shared metrics to compare"


def _labels(a_vals, b_vals):
    fig = viz.compare_dumbbell(["Goals / 90"], a_vals, b_vals, "Salah", "Kane")
    return {a.text: a for a in fig.layout.annotations}


@pytest.mark.parametrize("a, b, a_x, b_x", [
    (97.0, 3.0, 90, 10),      # both ends clamp
    (92.0, 97.0, 90, 90),     # both high: same x, but opposite sides
    (5.0, 20.0, 10, 20),      # both low
    (100.0, 100.0, 90, 90),   # tie at the edge
    (50.0, 50.0, 50, 50),     # tie mid-range
])
def test_dumbbell_labels_clamp_x_and_split_vertically(a, b, a_x, b_x):
    labels = _labels([a], [b])
    assert (labels["Salah"].x, labels["Kane"].x) == (a_x, b_x)
    assert labels["Salah"].yshift > 0 > labels["Kane"].yshift  # never the same side
    assert all(l.xanchor == "center" for l in labels.values())


def test_dumbbell_dots_keep_their_true_value_when_label_is_clamped():
    fig = viz.compare_dumbbell(["m"], [97.0], [3.0], "Salah", "Kane")
    markers = [t for t in fig.data if t.mode == "markers"]
    by_name = {t.name: t for t in markers}
    assert (list(by_name["Salah"].x), list(by_name["Kane"].x)) == ([97.0], [3.0])


def test_dumbbell_top_margin_leaves_room_for_the_label_above():
    fig = viz.compare_dumbbell(["m"], [50.0], [60.0], "Salah", "Kane")
    assert fig.layout.margin.t >= 48


def test_dumbbell_escapes_names_in_legend_and_labels():
    fig = viz.compare_dumbbell(["m"], [10.0], [20.0], "A<b>", "K&Co")
    markers = [t for t in fig.data if t.mode == "markers"]
    assert {t.name for t in markers} == {"A&lt;b&gt;", "K&amp;Co"}
    assert {a.text for a in fig.layout.annotations} == {"A&lt;b&gt;", "K&amp;Co"}


def test_dumbbell_range_is_padded_but_ticks_stay_0_to_100():
    fig = viz.compare_dumbbell(["m"], [0.0], [100.0], "A", "B")
    assert tuple(fig.layout.xaxis.range) == (-4, 104)
    assert tuple(fig.layout.xaxis.tickvals) == (0, 25, 50, 75, 100)


def test_dumbbell_rejects_mismatched_lengths():
    with pytest.raises(ValueError, match="equal-length"):
        viz.compare_dumbbell(["a", "b"], [1.0], [1.0, 2.0], "A", "B")
    with pytest.raises(ValueError, match="equal-length"):
        viz.compare_dumbbell([], [1.0], [], "A", "B")


def test_dumbbell_hover_does_not_splice_the_name():
    nasty = "X%{y}<b>"
    fig = viz.compare_dumbbell(["m"], [10.0], [20.0], nasty, "Kane")
    markers = [t for t in fig.data if t.mode == "markers"]
    assert all("%{fullData.name}" in t.hovertemplate and nasty not in t.hovertemplate for t in markers)

def test_dumbbell_tied_percentiles_stay_both_visible():
    fig = viz.compare_dumbbell(["m"], [40.2], [40.2], "A", "B")
    markers = [t for t in fig.data if t.mode == "markers"]
    assert len(markers) == 2
    a_idx = next(i for i, t in enumerate(fig.data) if t.name == "A")
    b_idx = next(i for i, t in enumerate(fig.data) if t.name == "B")
    assert a_idx > b_idx  # A is drawn after (on top of) B
    a, b = fig.data[a_idx], fig.data[b_idx]
    assert a.marker.symbol != b.marker.symbol and b.marker.symbol == "circle-open"
    assert b.marker.size > a.marker.size
    assert b.marker.color == theme.COMPARE and a.marker.line.color == theme.SURFACE
    assert (a.legendrank, b.legendrank) == (1, 2)
    assert fig.layout.hovermode == "y unified"


def test_percentile_bands_are_grey_sage_then_lime():
    # option A: neutral until good; lime is reserved for the Elite band
    assert theme.PCT_RAMP[3] == theme.ACCENT
    assert theme.ACCENT not in theme.PCT_RAMP[:3]
    assert viz.LINK == "rgba(238,240,242,0.22)"
