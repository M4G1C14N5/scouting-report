from __future__ import annotations

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
    assert list(fig.data[0].text) == ["1 · 10th · Poor", "2 · 30th · Below avg",
                                      "3 · 60th · Above avg", "4 · 90th · Elite"]
    assert fig.data[0].hovertemplate.endswith("<extra></extra>")
    assert fig.data[0].textfont.color == theme.INK_2  # text never wears a series colour


def test_percentile_bars_size_axis_and_reference_line():
    fig = viz.percentile_bars(ROWS)
    assert fig.layout.height == viz.ROW_PX * 4 + 60
    assert tuple(fig.layout.xaxis.tickvals) == (0, 25, 50, 75, 100)
    assert fig.layout.xaxis.fixedrange is True and fig.layout.yaxis.fixedrange is True
    assert fig.layout.yaxis.autorange == "reversed"
    assert any(shape.x0 == 50 for shape in fig.layout.shapes)


def test_percentile_bars_empty_state():
    fig = viz.percentile_bars([])
    assert fig.layout.annotations[0].text == "Not enough players in the pool to rank against"
    assert len(fig.data) == 0


def test_dumbbell_uses_player_and_compare_colours_with_legend():
    fig = viz.compare_dumbbell(["Goals / 90", "xG / 90"], [80.0, 40.0], [30.0, 70.0], "Salah", "Kane")
    markers = [t for t in fig.data if t.mode == "markers"]
    assert [t.name for t in markers] == ["Salah", "Kane"]
    assert [t.marker.color for t in markers] == [theme.PLAYER, theme.COMPARE]
    assert fig.layout.showlegend is True
    assert all(t.hovertemplate.endswith("<extra></extra>") for t in markers)
    assert len([t for t in fig.data if t.mode == "lines"]) == 2  # one connector per metric


def test_dumbbell_direct_labels_on_first_row_in_ink():
    fig = viz.compare_dumbbell(["Goals / 90", "xG / 90"], [70.0, 40.0], [30.0, 70.0], "Salah", "Kane")
    labels = {a.text: a for a in fig.layout.annotations}
    assert set(labels) == {"Salah", "Kane"}
    assert all(a.y == "Goals / 90" for a in labels.values())
    assert all(a.font.color == theme.INK_2 for a in labels.values())
    assert labels["Kane"].xanchor == "right" and labels["Salah"].xanchor == "left"  # mid-range: lower dot's label goes left


def test_dumbbell_empty_state():
    fig = viz.compare_dumbbell([], [], [], "A", "B")
    assert fig.layout.annotations[0].text == "No shared metrics to compare"


def _anchors(a_vals, b_vals):
    fig = viz.compare_dumbbell(["Goals / 90"], a_vals, b_vals, "Salah", "Kane")
    return {a.text: a.xanchor for a in fig.layout.annotations}


def test_dumbbell_labels_flip_inward_near_the_edges():
    assert _anchors([95.0], [60.0])["Salah"] == "right"   # high edge: label runs inward
    assert _anchors([5.0], [40.0])["Salah"] == "left"      # low edge: label runs inward
    assert _anchors([60.0], [95.0])["Kane"] == "right"
    assert _anchors([50.0], [50.0]) == {"Salah": "right", "Kane": "left"}  # tie stays opposite
    assert _anchors([95.0], [95.0]) == {"Salah": "right", "Kane": "left"}  # tie at the edge too


def test_dumbbell_range_is_padded_but_ticks_stay_0_to_100():
    fig = viz.compare_dumbbell(["m"], [0.0], [100.0], "A", "B")
    assert tuple(fig.layout.xaxis.range) == (-4, 104)
    assert tuple(fig.layout.xaxis.tickvals) == (0, 25, 50, 75, 100)


def test_dumbbell_rejects_mismatched_lengths():
    import pytest
    with pytest.raises(ValueError, match="equal-length"):
        viz.compare_dumbbell(["a", "b"], [1.0], [1.0, 2.0], "A", "B")
    with pytest.raises(ValueError, match="equal-length"):
        viz.compare_dumbbell([], [1.0], [], "A", "B")


def test_dumbbell_hover_does_not_splice_the_name():
    nasty = "X%{y}<b>"
    fig = viz.compare_dumbbell(["m"], [10.0], [20.0], nasty, "Kane")
    markers = [t for t in fig.data if t.mode == "markers"]
    assert all("%{fullData.name}" in t.hovertemplate and nasty not in t.hovertemplate for t in markers)
