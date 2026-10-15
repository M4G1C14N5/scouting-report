"""Smoke tests: the whole app renders with the data layer stubbed (no BigQuery)."""

from __future__ import annotations

import html
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from src.ui import data
from tests import fakes

ROOT = Path(__file__).resolve().parents[1]
APP = str(ROOT / "app.py")
TIMEOUT = 60


@pytest.fixture
def stub_data(monkeypatch):
    monkeypatch.setattr(data, "get_bq_client", lambda: object())
    monkeypatch.setattr(data, "load_player_index", lambda _bq: fakes.player_index())
    monkeypatch.setattr(data, "player_stats",
                        lambda _bq, player, season: fakes.player_stats(player, season))
    monkeypatch.setattr(data, "position_pool",
                        lambda _bq, group, season: fakes.position_pool(group, season))
    monkeypatch.setattr(data, "similar_players",
                        lambda _bq, player, season, group: fakes.similar_players(player, season, group))


def _app() -> AppTest:
    at = AppTest.from_file(APP, default_timeout=TIMEOUT).run()
    assert not at.exception, at.exception
    return at


def _select(at: AppTest, player: str) -> AppTest:
    at.selectbox(key="player_pick").set_value(player).run()
    assert not at.exception, at.exception
    return at


def _markdown(at) -> str:
    return "\n".join(m.value for m in at.markdown)


def _captions(at) -> str:
    return "\n".join(c.value for c in at.caption)


def test_no_selection_shows_hero_onboarding_and_footer(stub_data):
    at = _app()
    md = _markdown(at)
    assert "Football Player <em>Profiler</em>" in md
    assert "Big 5 leagues · 2017-18 to 2023-24 · 16.5k player-seasons" in md
    assert "Set the scope" in md and "Find a player" in md and "Read the profile" in md
    assert at.button(key="try_example").label == "Try an example: Mohamed Salah"
    assert "Thomas Tsangou and Ajaiah Darlington" in md
    assert "FBref → GCS → BigQuery" in md
    assert not at.tabs
    assert "Showing 11 players" in " ".join(c.value for c in at.sidebar.caption)
    assert at.selectbox(key="player_pick").value is None  # nobody auto-selected
    assert at.button_group(key="f_season").value == fakes.LATEST  # sidebar defaults to the latest season


def test_example_button_opens_a_profile(stub_data):
    at = _app()
    at.button(key="try_example").click().run()
    assert not at.exception
    assert at.session_state["player_pick"] == "Mohamed Salah"
    assert [t.label for t in at.tabs] == ["Profile", "Stats", "Similar players"]


def test_outfield_player_renders_every_tab(stub_data):
    at = _select(_app(), "Mohamed Salah")
    md = _markdown(at)
    assert "Forward" in md and "xAG" in md
    assert "vs 6 forwards in 2023-24, 450+ minutes" in _captions(at)
    assert len(at.get("plotly_chart")) == 3
    assert len(at.dataframe) == 7  # 6 stat cards + similar table
    assert not at.error


def test_goalkeeper_renders_every_tab(stub_data):
    at = _select(_app(), "Alisson")
    md = _markdown(at)
    assert "Goalkeeper" in md and "Save %" in md and "Saves / 90" in md
    assert "vs 4 goalkeepers in 2023-24, 450+ minutes" in _captions(at)
    assert len(at.get("plotly_chart")) == 1
    assert len(at.dataframe) == 3  # 2 stat cards + similar table
    assert not at.error


def test_season_switcher_defaults_to_latest_and_sticks(stub_data):
    at = _app()
    at.button_group(key="f_season").set_value("All").run()
    at = _select(at, "Mohamed Salah")
    assert at.button_group(key="season_switch").value == fakes.LATEST
    at.button_group(key="season_switch").set_value(fakes.PREVIOUS).run()
    assert not at.exception
    assert "vs 1 forwards in 2022-23, 450+ minutes" in _captions(at)
    at.slider(key="top_n").set_value(5).run()  # any rerun keeps the chosen season
    assert at.button_group(key="season_switch").value == fakes.PREVIOUS


def test_low_minutes_player_gets_empty_states(stub_data):
    md = _markdown(_select(_app(), "Bench Striker"))
    # theme helpers html-escape their text (isn&#x27;t), so unescape before matching prose
    assert "isn't in the percentile pool" in html.unescape(md)
    assert "below the 450-minute threshold" in md


def test_filters_with_no_players_show_an_empty_state(stub_data):
    at = _app()
    at.button_group(key="f_season").set_value(fakes.PREVIOUS).run()
    at.selectbox(key="f_league").set_value("Bundesliga").run()
    assert not at.exception
    assert "No players match these filters" in _markdown(at)
    assert "Thomas Tsangou" in _markdown(at)


def test_club_resets_when_league_changes(stub_data):
    at = _app()
    at.selectbox(key="f_club").set_value("Sevilla").run()
    at.selectbox(key="f_league").set_value("Bundesliga").run()
    assert not at.exception
    assert at.selectbox(key="f_club").value == "All clubs"


def test_player_cleared_when_league_filter_excludes_them(stub_data):
    at = _select(_app(), "Mohamed Salah")
    at.selectbox(key="f_league").set_value("Bundesliga").run()
    assert not at.exception
    assert at.session_state["player_pick"] is None
    assert not at.tabs
    assert "Find a player" in _markdown(at)  # onboarding shows, not a stale profile


def test_season_switch_recovers_when_season_filter_narrows(stub_data):
    at = _app()
    at.button_group(key="f_season").set_value("All").run()
    at = _select(at, "Mohamed Salah")
    at.button_group(key="season_switch").set_value(fakes.PREVIOUS).run()
    assert not at.exception
    at.button_group(key="f_season").set_value(fakes.LATEST).run()
    assert not at.exception
    assert at.session_state["player_pick"] == "Mohamed Salah"
    assert at.session_state["season_switch"] == fakes.LATEST  # fell back from the filtered-out season
    assert "vs 6 forwards in 2023-24, 450+ minutes" in _captions(at)  # only 2023-24 is in view


def test_view_profile_switches_player(stub_data):
    at = _select(_app(), "Mohamed Salah")
    at.session_state["similar_table"] = {"selection": {"rows": [0], "columns": []}}
    at.run()
    at.button(key="view_profile").click().run()
    assert not at.exception
    assert at.session_state["player_pick"] == "Erling Haaland"
    assert 'class="fl-name">Erling Haaland<' in _markdown(at)


def test_bigquery_failure_shows_a_readable_error(stub_data, monkeypatch):
    def boom(_bq):
        raise RuntimeError("no credentials")

    monkeypatch.setattr(data, "load_player_index", boom)
    at = _app()
    assert at.error[0].value.startswith("Couldn't load the player list from BigQuery")
    assert "Thomas Tsangou" in _markdown(at)


def test_no_deprecated_use_container_width():
    files = [ROOT / "app.py", *(ROOT / "src" / "ui").rglob("*.py")]
    offenders = [str(f) for f in files if "use_container_width" in f.read_text(encoding="utf-8")]
    assert not offenders
