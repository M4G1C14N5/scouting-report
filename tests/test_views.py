"""Each view rendered on its own with fake data (AppTest.from_function; no BigQuery)."""

from __future__ import annotations

import html
import json

import pytest
from streamlit.testing.v1 import AppTest

from src.ui import build, data
from tests import fakes

TIMEOUT = 60


def _markdown(at) -> str:
    return "\n".join(m.value for m in at.markdown)


def _captions(at) -> str:
    return "\n".join(c.value for c in at.caption)


# --- scripts (each runs as its own Streamlit page) ---------------------------

def header_script(player: str, season: str) -> None:
    from src.ui.views import header
    from tests import fakes

    header.render(fakes.player_stats(player, season))


def profile_script(player: str, season: str, group: str, empty_pool: bool) -> None:
    from src.ui.views import profile
    from tests import fakes

    pool = fakes.position_pool(group, season)
    if empty_pool:
        pool = pool.iloc[0:0]
    profile.render(pool, player, season, group)


def stats_script(player: str, season: str) -> None:
    from src.ui.views import stats_tab
    from tests import fakes

    row = fakes.player_stats(player, season)
    stats_tab.render(row, row["position_group"])


def similar_script(player: str, season: str) -> None:
    from src.ui.views import similar
    from tests import fakes

    row = fakes.player_stats(player, season)
    similar.render(object(), fakes.position_pool(row["position_group"], season), row)


def similar_switch_script() -> None:
    import streamlit as st

    from src.ui.views import similar
    from tests import fakes

    row = fakes.player_stats(st.session_state.get("who", "Mohamed Salah"), fakes.LATEST)
    similar.render(object(), fakes.position_pool(row["position_group"], fakes.LATEST), row)


def _run(script, *args) -> AppTest:
    at = AppTest.from_function(script, args=args, default_timeout=TIMEOUT).run()
    assert not at.exception, at.exception
    return at


@pytest.fixture
def stub_similar(monkeypatch):
    monkeypatch.setattr(data, "similar_players",
                        lambda _bq, player, season, group: fakes.similar_players(player, season, group))


# --- header ------------------------------------------------------------------

def test_header_outfield_identity_and_kpis():
    md = _markdown(_run(header_script, "Mohamed Salah", fakes.LATEST))
    for text in ("Mohamed Salah", ">MS<", "Forward", "Liverpool", "Premier League", "2023-24",
                 "Age 31", "EGY", "Minutes", "Goals", "Assists", "xG", "xAG", "fl-kpi-row"):
        assert text in md, text


def test_header_goalkeeper_kpis():
    md = _markdown(_run(header_script, "Alisson", fakes.LATEST))
    for text in ("Goalkeeper", "Save %", "GA / 90", "CS %", "Saves / 90"):
        assert text in md, text


# --- profile -----------------------------------------------------------------

def test_profile_outfield_cards_and_pool_caption():
    at = _run(profile_script, "Mohamed Salah", fakes.LATEST, "ATT", False)
    assert "vs 6 forwards in 2023-24, 450+ minutes" in _captions(at)
    md = _markdown(at)
    assert "Shooting" in md and "Passing" in md and "Progression" in md
    assert len(at.get("plotly_chart")) == 3
    # the band key is a stacked HTML block now, not a one-line caption
    assert "Elite 75th+" not in _captions(at)
    assert md.count('class="fl-band-row"') == 4
    assert "Counting stats are ranked per 90 minutes." in md


def test_profile_goalkeeper():
    at = _run(profile_script, "Alisson", fakes.LATEST, "GK", False)
    assert "vs 4 goalkeepers in 2023-24, 450+ minutes" in _captions(at)
    assert len(at.get("plotly_chart")) == 1
    assert _markdown(at).count('class="fl-band-row"') == 4


def test_profile_player_below_threshold_gets_empty_state():
    at = _run(profile_script, "Bench Striker", fakes.LATEST, "ATT", False)
    # theme helpers html-escape their text (isn&#x27;t), so unescape before matching prose
    assert "Bench Striker isn't in the percentile pool" in html.unescape(_markdown(at))
    assert len(at.get("plotly_chart")) == 0


def test_profile_empty_pool_gets_empty_state():
    at = _run(profile_script, "Mohamed Salah", fakes.LATEST, "ATT", True)
    assert "No comparison pool" in _markdown(at)


# --- stats -------------------------------------------------------------------

def test_stats_outfield_has_six_category_tables():
    at = _run(stats_script, "Mohamed Salah", fakes.LATEST)
    assert len(at.dataframe) == 6
    assert list(at.dataframe[0].value.columns) == list(build.STAT_TABLE_COLUMNS)
    md = _markdown(at)
    for category in ("Shooting", "Passing", "Defending", "Progression", "Discipline", "Playing time"):
        assert category in md
    assert "Per 90 = per 90 minutes played" in _captions(at)


def test_stats_goalkeeper_has_two_tables():
    at = _run(stats_script, "Alisson", fakes.LATEST)
    assert len(at.dataframe) == 2
    assert "Goalkeeping" in _markdown(at)


# --- similar -----------------------------------------------------------------

def test_similar_table_and_method_line(stub_similar):
    at = _run(similar_script, "Mohamed Salah", fakes.LATEST)
    assert "cosine similarity on z-scored stats within the position pool" in _captions(at)
    table = at.dataframe[0].value
    assert list(table.columns) == list(build.SIMILAR_COLUMNS)
    assert len(table) == 5 and table.iloc[0]["Player"] == "Erling Haaland"
    assert at.slider(key="top_n").value == 10
    assert len(at.get("plotly_chart")) == 0  # nothing selected yet
    assert not table["Season"].str.contains("-20").any()  # short form, "2023-24"


def test_similar_selection_shows_dumbbell_and_view_profile_jumps(stub_similar):
    at = _run(similar_script, "Mohamed Salah", fakes.LATEST)
    at.session_state["similar_table"] = {"selection": {"rows": [0], "columns": []}}
    at.run()
    assert not at.exception
    assert len(at.get("plotly_chart")) == 1
    assert "Mohamed Salah vs Erling Haaland" in _markdown(at)
    traces = json.loads(at.get("plotly_chart")[0].proto.spec)["data"]
    markers = [t for t in traces if t.get("mode") == "markers"]
    assert len(markers) == 2 and len(markers[0]["x"]) == len(markers[1]["x"]) > 0
    at.button(key="view_profile").click().run()
    assert not at.exception
    assert at.session_state["player_pick"] == "Erling Haaland"
    assert at.session_state["f_season"] == fakes.LATEST
    assert at.session_state["f_league"] == "All leagues"
    assert at.session_state["f_club"] == "All clubs"
    assert at.session_state["season_switch"] == fakes.LATEST


def test_similar_below_threshold(stub_similar):
    at = _run(similar_script, "Bench Striker", fakes.LATEST)
    assert "below the 450-minute threshold" in _markdown(at)
    assert len(at.dataframe) == 0


def test_similar_error_is_readable(monkeypatch):
    def boom(_bq, player, season, group):
        raise RuntimeError("bigquery down")

    monkeypatch.setattr(data, "similar_players", boom)
    at = _run(similar_script, "Mohamed Salah", fakes.LATEST)
    assert at.error[0].value == "Couldn't compute similar players."


def test_similar_selection_is_cleared_when_the_player_changes(stub_similar):
    at = AppTest.from_function(similar_switch_script, default_timeout=TIMEOUT).run()
    at.session_state["similar_table"] = {"selection": {"rows": [0], "columns": []}}
    at.run()
    assert not at.exception
    assert len(at.get("plotly_chart")) == 1 and at.button(key="view_profile")
    # AppTest drops the table's selection between runs, so re-assert it in the
    # same run that switches player: the stale selection a real browser keeps.
    at.session_state["similar_table"] = {"selection": {"rows": [0], "columns": []}}
    at.session_state["who"] = "Harry Kane"
    at.run()
    assert not at.exception, at.exception
    assert len(at.get("plotly_chart")) == 0
    assert all(b.key != "view_profile" for b in at.button)


def test_stats_display_table_leaves_missing_values_blank():
    import pandas as pd
    from src.ui.views import stats_tab

    table = pd.DataFrame({"Stat": ["Goals", "Shot Accuracy %", "xG / 90"],
                          "Total": [1234.0, 43.9, float("nan")],
                          "Per 90": [0.64, float("nan"), 0.75]})
    out = stats_tab.display_table(table)
    assert list(out["Total"]) == ["1,234", "43.9", ""]
    assert list(out["Per 90"]) == ["0.64", "", "0.75"]
