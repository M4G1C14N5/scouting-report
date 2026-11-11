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


def header_row_script(row: dict) -> None:
    import pandas as pd

    from src.ui.views import header

    header.render(pd.Series(row))


def profile_script(player: str, season: str, group: str, empty_pool: bool) -> None:
    from src.ui.views import profile
    from tests import fakes

    pool = fakes.position_pool(group, season)
    if empty_pool:
        pool = pool.iloc[0:0]
    profile.render(pool, player, season, group)


def profile_gap_script(player: str, season: str, group: str, metric: str, league: str) -> None:
    """Profile with `metric` filled only for `league`, so other leagues lack it."""
    import numpy as np

    from src.ui.views import profile
    from tests import fakes

    pool = fakes.position_pool(group, season)
    pool.loc[pool["League"] != league, metric] = np.nan
    profile.render(pool, player, season, group)


def profile_sparse_script(player: str, season: str, group: str, league: str) -> None:
    """Every stat blank outside `league`, so no metric has coverage across the season."""
    import numpy as np

    from src.ui import metrics
    from src.ui.views import profile
    from tests import fakes

    pool = fakes.position_pool(group, season)
    keys = [k for k in metrics.profile_keys(group) if k in pool.columns]
    pool.loc[pool["League"] != league, keys] = np.nan
    profile.render(pool, player, season, group)


def stats_script(player: str, season: str) -> None:
    from src.ui.views import stats_tab
    from tests import fakes

    row = fakes.player_stats(player, season)
    stats_tab.render(row, row["position_group"])


def stats_row_script(row: dict) -> None:
    import pandas as pd

    from src.ui.views import stats_tab

    stats_tab.render(pd.Series(row), row["position_group"])


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
                        lambda _bq, player, season, group, mode=None: fakes.similar_players(
                            player, season, group))


# --- header ------------------------------------------------------------------

def test_header_outfield_identity_and_kpis():
    md = _markdown(_run(header_script, "Mohamed Salah", fakes.LATEST))
    for text in ("Mohamed Salah", ">MS<", "Forward", "Liverpool", "Premier League", "2023-24",
                 "Age 31", "EGY", "Minutes", "Goals", "Assists", "xG", "xAG", "fl-kpi-row"):
        assert text in md, text


def _new_source_row(**extra) -> dict:
    row = fakes.player_stats("Mohamed Salah", fakes.LATEST).to_dict()
    row.update(Season="2026", League="USL League One", Squad="Greenville Triumph",
               Source="American Soccer Analysis", **extra)
    return row


def test_header_shows_the_source_as_a_chip():
    md = _markdown(_run(header_row_script, _new_source_row()))
    assert '<span class="fl-chip">American Soccer Analysis</span>' in md
    assert '<span class="fl-chip">2026</span>' in md  # a US calendar season stays "2026"
    assert md.index("USL League One") < md.index("American Soccer Analysis")


def test_header_fbref_rows_get_the_fbref_chip():
    md = _markdown(_run(header_script, "Mohamed Salah", fakes.LATEST))
    assert '<span class="fl-chip">FBref</span>' in md


def test_header_without_a_source_has_no_source_chip():
    row = _new_source_row()
    row.pop("Source")
    without = _markdown(_run(header_row_script, row))
    assert "American Soccer Analysis" not in without and "FBref" not in without
    row["Source"] = None
    blank = _markdown(_run(header_row_script, row))
    assert ">None<" not in blank
    assert blank.count('class="fl-chip"') == without.count('class="fl-chip"')


def test_header_goalkeeper_kpis():
    md = _markdown(_run(header_script, "Alisson", fakes.LATEST))
    for text in ("Goalkeeper", "Save %", "GA / 90", "CS %", "Saves / 90"):
        assert text in md, text


# --- profile -----------------------------------------------------------------

def test_profile_outfield_cards_and_pool_caption():
    at = _run(profile_script, "Mohamed Salah", fakes.LATEST, "ATT", False)
    assert "Ranked against 2 forwards in Premier League 2023-24 with 450+ minutes" in _captions(at)
    md = _markdown(at)
    for category in ("Shooting", "Chance creation", "Passing", "Possession and dribbling"):
        assert category in md, category
    # FBref rows have none of the newer-source stats: no Duels or Goals added card
    assert "Duels" not in md and "Goals added" not in md
    assert len(at.get("plotly_chart")) == 4
    assert md.index("Shooting") < md.index("Chance creation") < md.index("Passing")
    # the band key is a stacked HTML block now, not a one-line caption
    assert "Elite 75th+" not in _captions(at)
    assert md.count('class="fl-band-row"') == 4
    assert "Counting stats are ranked per 90 minutes." in md


def test_profile_goalkeeper():
    at = _run(profile_script, "Alisson", fakes.LATEST, "GK", False)
    assert "Ranked against 2 goalkeepers in Premier League 2023-24 with 450+ minutes" in _captions(at)
    assert len(at.get("plotly_chart")) == 1
    assert _markdown(at).count('class="fl-band-row"') == 4


def _bar_labels(at) -> list[str]:
    labels: list[str] = []
    for chart in at.get("plotly_chart"):
        for trace in json.loads(chart.proto.spec)["data"]:
            if trace.get("type") == "bar":
                labels.extend(y.replace("<br>", " ") for y in trace.get("y", []))
    return labels


def test_profile_compares_with_the_same_league_by_default():
    at = _run(profile_script, "Mohamed Salah", fakes.LATEST, "ATT", False)
    control = at.selectbox(key="profile_compare")
    assert control.value == "Same league"
    assert control.options == ["Same league", "All leagues in this season"]
    assert "Compare with" in _markdown(at)


def test_profile_all_leagues_ranks_across_the_whole_pool():
    at = _run(profile_script, "Mohamed Salah", fakes.LATEST, "ATT", False)
    same = _bar_values(at)
    at.selectbox(key="profile_compare").set_value("All leagues in this season").run()
    assert not at.exception
    assert "Ranked against 6 forwards in all leagues in 2023-24 with 450+ minutes" in _captions(at)
    assert _bar_values(at) != same  # a different pool gives different percentiles
    at.selectbox(key="profile_compare").set_value("Same league").run()
    assert _bar_values(at) == same
    assert "Ranked against 2 forwards in Premier League 2023-24" in _captions(at)


def _bar_values(at) -> list:
    values: list = []
    for chart in at.get("plotly_chart"):
        for trace in json.loads(chart.proto.spec)["data"]:
            if trace.get("type") == "bar":
                values.extend(trace.get("x", []))
    return values


def test_profile_same_league_with_one_player_says_one_player():
    at = _run(profile_script, "Kylian Mbappé", fakes.LATEST, "ATT", False)
    assert "Ranked against 1 forward in Ligue 1 2023-24 with 450+ minutes" in _captions(at)


def test_profile_hides_metrics_the_chosen_pool_mostly_lacks():
    args = ("Mohamed Salah", fakes.LATEST, "ATT", "key_passes", "Premier League")
    at = _run(profile_gap_script, *args)
    assert [l for l in _bar_labels(at) if l.startswith("Key Passes")]  # all of the league has it
    at.selectbox(key="profile_compare").set_value("All leagues in this season").run()
    assert not at.exception
    assert not [l for l in _bar_labels(at) if l.startswith("Key Passes")]  # 2 of 6 rows have it
    assert len(_bar_labels(at)) > 0


def test_profile_with_no_metric_coverage_says_so_instead_of_drawing_nothing():
    at = _run(profile_sparse_script, "Mohamed Salah", fakes.LATEST, "ATT", "Premier League")
    assert len(at.get("plotly_chart")) == 4
    at.selectbox(key="profile_compare").set_value("All leagues in this season").run()
    assert not at.exception
    assert len(at.get("plotly_chart")) == 0
    assert "Not enough data to rank" in _markdown(at)


def test_profile_player_below_threshold_gets_empty_state():
    at = _run(profile_script, "Bench Striker", fakes.LATEST, "ATT", False)
    # theme helpers html-escape their text (isn&#x27;t), so unescape before matching prose
    assert "Bench Striker isn't in the percentile pool" in html.unescape(_markdown(at))
    assert len(at.get("plotly_chart")) == 0


def test_profile_empty_pool_gets_empty_state():
    at = _run(profile_script, "Mohamed Salah", fakes.LATEST, "ATT", True)
    assert "No comparison pool" in _markdown(at)


# --- stats -------------------------------------------------------------------

def test_stats_outfield_fbref_row_has_seven_category_tables_in_order():
    at = _run(stats_script, "Mohamed Salah", fakes.LATEST)
    assert len(at.dataframe) == 7
    assert list(at.dataframe[0].value.columns) == list(build.STAT_TABLE_COLUMNS)
    md = _markdown(at)
    shown = ("Shooting", "Chance creation", "Passing", "Possession and dribbling", "Defending",
             "Discipline", "Playing time")
    positions = [md.index(category) for category in shown]
    assert positions == sorted(positions)
    assert "Duels" not in md and "Goals added" not in md  # FBref rows lack them


def test_stats_us_row_shows_the_us_set():
    row = _new_source_row(duels_total=120.0, duels_won=66.0, duels_won_pct=55.0,
                          goals_added=1.8, ga_shooting=0.9, offsides=4.0)
    at = _run(stats_row_script, row)
    md = _markdown(at)
    assert "Goals added" in md
    assert "Duels" not in md  # not in the US set, even when the row has it
    assert len(at.dataframe) == 6
    stats_shown = {s for df in at.dataframe for s in df.value["Stat"]}
    assert "Offsides" not in stats_shown and "Tackles" not in stats_shown
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
    def boom(_bq, player, season, group, mode=None):
        raise RuntimeError("bigquery down")

    monkeypatch.setattr(data, "similar_players", boom)
    at = _run(similar_script, "Mohamed Salah", fakes.LATEST)
    assert at.error[0].value == "Couldn't compute similar players."


def test_similar_not_enough_stats_shows_the_message_not_an_error(monkeypatch):
    from src.similarity import NotEnoughStatsError

    def not_enough(_bq, player, season, group, mode=None):
        raise NotEnoughStatsError()

    monkeypatch.setattr(data, "similar_players", not_enough)
    at = _run(similar_script, "Mohamed Salah", fakes.LATEST)
    md = _markdown(at)
    assert "Not enough comparable stats for this player in this season" in md
    assert "fl-empty" in md
    assert not at.error
    assert len(at.dataframe) == 0


def test_similar_method_line_says_only_shared_stats_are_used(stub_similar):
    at = _run(similar_script, "Mohamed Salah", fakes.LATEST)
    assert "compared only with players who have all of those stats" in _captions(at)


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


def test_stats_display_table_shows_n_a_when_a_row_has_no_value():
    import pandas as pd
    from src.ui.views import stats_tab

    table = pd.DataFrame({"Stat": ["Dribbled Past"], "Total": [float("nan")], "Per 90": [float("nan")]})
    out = stats_tab.display_table(table)
    assert list(out["Total"]) == ["n/a"] and list(out["Per 90"]) == [""]


def test_stats_display_table_leaves_missing_values_blank():
    import pandas as pd
    from src.ui.views import stats_tab

    table = pd.DataFrame({"Stat": ["Goals", "Shot Accuracy %", "xG / 90"],
                          "Total": [1234.0, 43.9, float("nan")],
                          "Per 90": [0.64, float("nan"), 0.75]})
    out = stats_tab.display_table(table)
    assert list(out["Total"]) == ["1,234", "43.9", ""]
    assert list(out["Per 90"]) == ["0.64", "", "0.75"]


# --- newer seasons: fixed family sets ----------------------------------------

def profile_new_script(player: str, season: str, group: str) -> None:
    from src.ui.views import profile
    from tests import fakes

    profile.render(fakes.new_position_pool(group, season), player, season, group)


def _bar_texts(at) -> dict[str, str]:
    out: dict[str, str] = {}
    for chart in at.get("plotly_chart"):
        for trace in json.loads(chart.proto.spec)["data"]:
            if trace.get("type") == "bar":
                for y, text in zip(trace.get("y", []), trace.get("text", [])):
                    out[y.replace("<br>", " ")] = text
    return out


def _family_labels(keys) -> set[str]:
    from src.ui import metrics

    labels = set()
    for key in keys:
        label = metrics.label(key)
        if metrics.KIND[key] in metrics.PER90_COMPUTED_KINDS and not label.endswith("/ 90"):
            label = f"{label} / 90"
        labels.add(label)
    return labels


def test_profile_europe_player_shows_exactly_the_europe_set():
    from src.ui import metrics

    at = _run(profile_new_script, "Two Forward 0", fakes.EUROPE_SEASON, "ATT")
    assert set(_bar_labels(at)) == _family_labels(metrics.EUROPE_OUTFIELD)
    assert len(_bar_labels(at)) == 20
    md = _markdown(at)
    for category in ("Shooting", "Chance creation", "Passing", "Possession and dribbling",
                     "Defending", "Duels", "Discipline", "Playing time"):
        assert category in md, category
    assert len(at.get("plotly_chart")) == 8
    assert "Big five only" not in md
    assert "Offsides" not in " ".join(_bar_labels(at))
    assert at.selectbox(key="profile_compare").options == ["Same league", "All leagues in this season"]


def test_profile_missing_metric_shows_n_a_instead_of_disappearing():
    at = _run(profile_new_script, "Gappy Forward", fakes.EUROPE_SEASON, "ATT")
    texts = _bar_texts(at)
    assert texts["Dribbled Past / 90"] == "n/a"
    assert len(texts) == 20


def test_profile_big_five_player_gets_a_big_five_only_card():
    from src.ui import metrics

    at = _run(profile_new_script, "Prem Forward 3", fakes.EUROPE_SEASON, "ATT")
    labels = _bar_labels(at)
    assert set(labels) == _family_labels(metrics.EUROPE_OUTFIELD) | _family_labels(
        metrics.BIG_FIVE_EXTRAS)
    assert len(at.get("plotly_chart")) == 9
    md = _markdown(at)
    assert "Big five only" in md
    assert ("These stats come from Understat and exist only for the Premier League, La Liga, "
            "Serie A, Bundesliga and Ligue 1, so they are ranked against big-five players only."
            ) in md + _captions(at)


def _big_five_pct(at) -> float:
    for chart in at.get("plotly_chart"):
        for trace in json.loads(chart.proto.spec)["data"]:
            ys = [y.replace("<br>", " ") for y in trace.get("y", [])]
            if trace.get("type") == "bar" and "xG / 90" in ys:
                return trace["x"][ys.index("xG / 90")]
    raise AssertionError("no xG / 90 bar")


def test_big_five_extras_rank_against_big_five_players_only():
    from src.ui import stats

    at = _run(profile_new_script, "Prem Forward 3", fakes.EUROPE_SEASON, "ATT")
    pool = fakes.new_position_pool("ATT", fakes.EUROPE_SEASON)
    me = pool[pool["Player"] == "Prem Forward 3"].iloc[0]
    premier = pool[pool["League"] == "Premier League"]
    assert _big_five_pct(at) == pytest.approx(stats.percentile_of(premier["xg_per_90"], me["xg_per_90"]))
    at.selectbox(key="profile_compare").set_value("All leagues in this season").run()
    assert not at.exception
    five = pool[pool["League"].isin(["Premier League", "La Liga"])]  # not League Two
    assert len(five) == 12
    assert _big_five_pct(at) == pytest.approx(stats.percentile_of(five["xg_per_90"], me["xg_per_90"]))


def test_profile_us_player_shows_exactly_the_us_set():
    from src.ui import metrics

    at = _run(profile_new_script, "US Forward 2", fakes.US_SEASON, "ATT")
    assert set(_bar_labels(at)) == _family_labels(metrics.US_OUTFIELD)
    assert len(_bar_labels(at)) == 21
    md = _markdown(at)
    assert "Goals added" in md and "Big five only" not in md
    assert "Duels" not in md


def test_profile_europe_goalkeeper_uses_the_keeper_set():
    from src.ui import metrics

    at = _run(profile_new_script, "Prem Keeper 1", fakes.EUROPE_SEASON, "GK")
    assert set(_bar_labels(at)) == _family_labels(metrics.EUROPE_GOALKEEPER)
    assert len(at.get("plotly_chart")) == 2  # Goalkeeping, Playing time
    assert "Big five only" not in _markdown(at)


def stats_new_script(player: str, season: str) -> None:
    from src.ui.views import stats_tab
    from tests import fakes

    row = fakes.new_player_stats(player, season)
    stats_tab.render(row, row["position_group"])


def _stat_rows(at) -> dict[str, tuple[str, str]]:
    out = {}
    for df in at.dataframe:
        for _, r in df.value.iterrows():
            out[r["Stat"]] = (r["Total"], r["Per 90"])
    return out


def _stats_labels(keys) -> set[str]:
    from src.ui import metrics

    return {metrics.label(k) for k in metrics.stats_keys(keys)}


def test_stats_europe_player_shows_the_europe_set_and_hides_patchy_stats():
    from src.ui import metrics

    at = _run(stats_new_script, "Two Forward 1", fakes.EUROPE_SEASON)
    rows = _stat_rows(at)
    assert set(rows) == _stats_labels(metrics.EUROPE_OUTFIELD) | {"Minutes", "90s", "Appearances"}
    for patchy in ("Offsides", "Penalties Won", "Penalties Conceded", "Penalties Missed"):
        assert patchy not in rows, patchy  # the fake row has them filled
    assert len(at.dataframe) == 8 and "Big five only" not in _markdown(at)


def test_stats_big_five_player_gets_a_big_five_only_section():
    from src.ui import metrics

    at = _run(stats_new_script, "Liga Forward 0", fakes.EUROPE_SEASON)
    md = _markdown(at)
    assert "Big five only" in md and len(at.dataframe) == 9
    big = at.dataframe[-1].value
    assert set(big["Stat"]) == _stats_labels(metrics.BIG_FIVE_EXTRAS)
    assert "Understat" in md + _captions(at)


def test_stats_missing_metric_shows_n_a():
    at = _run(stats_new_script, "Gappy Forward", fakes.EUROPE_SEASON)
    assert _stat_rows(at)["Dribbled Past"] == ("n/a", "")


def test_stats_europe_goalkeeper_uses_the_keeper_set():
    at = _run(stats_new_script, "Two Keeper 0", fakes.EUROPE_SEASON)
    assert set(_stat_rows(at)) == {"Save %", "Goals Against", "Clean Sheet %", "Saves", "Minutes",
                                   "90s", "Appearances", "Match Rating"}


def test_header_tiles_follow_the_family():
    europe = _markdown(_run(header_row_script, fakes.new_player_stats("Two Forward 0", fakes.EUROPE_SEASON).to_dict()))
    for text in ("Goals", "Assists", "Shots", "Match Rating"):
        assert text in europe, text
    assert ">xG<" not in europe and ">xAG<" not in europe  # most European leagues have no xG
    us = _markdown(_run(header_row_script, fakes.new_player_stats("US Forward 0", fakes.US_SEASON).to_dict()))
    assert ">xG<" in us and ">xA<" in us


# --- similar: compare modes on the newer seasons -------------------------------

def similar_new_script(player: str, season: str) -> None:
    from src.ui.views import similar
    from tests import fakes

    row = fakes.new_player_stats(player, season)
    similar.render(object(), fakes.new_position_pool(row["position_group"], season), row)


@pytest.fixture
def real_similarity(monkeypatch):
    """The real similarity model on the fake newer-season pools."""
    from src import similarity

    def pool(_bq, group, season, paired=False):
        return fakes.new_position_pool(group, season, paired=paired)

    def similar_players(_bq, player, season, group, mode=similarity.MODE_SHARED):
        frame = fakes.new_position_pool(group, season, paired=mode == similarity.MODE_TRIAL)
        return similarity.similar_in_pool(frame, player, season, mode, data.SIMILAR_MAX)

    monkeypatch.setattr(data, "position_pool", pool)
    monkeypatch.setattr(data, "similar_players", similar_players)


def _mode(at, mode: str) -> None:
    at.selectbox(key="similar_mode").set_value(mode).run()
    assert not at.exception, at.exception


def test_similar_big_five_player_has_three_modes(real_similarity):
    from src import similarity

    at = _run(similar_new_script, "Prem Forward 3", fakes.EUROPE_SEASON)
    control = at.selectbox(key="similar_mode")
    assert control.options == [similarity.MODE_SHARED, similarity.MODE_BIG_FIVE, similarity.MODE_TRIAL]
    assert control.value == similarity.MODE_SHARED
    assert "Compare on" in _markdown(at)
    assert "the same 20 stats" in _captions(at)
    assert set(at.dataframe[0].value["League"]) >= {"League Two"}
    assert similarity.BIG_FIVE_NOTE not in _captions(at)

    _mode(at, similarity.MODE_BIG_FIVE)
    assert similarity.BIG_FIVE_NOTE in _captions(at)
    assert set(at.dataframe[0].value["League"]) <= {"Premier League", "La Liga"}
    assert "the same 25 stats" in _captions(at)

    _mode(at, similarity.MODE_TRIAL)
    assert similarity.TRIAL_NOTE in "\n".join(i.value for i in at.info)
    assert "2025" in set(at.dataframe[0].value["Season"])  # US players of the paired season


def test_similar_other_european_player_has_no_big_five_mode(real_similarity):
    from src import similarity

    at = _run(similar_new_script, "Two Forward 0", fakes.EUROPE_SEASON)
    assert at.selectbox(key="similar_mode").options == [similarity.MODE_SHARED, similarity.MODE_TRIAL]


def test_similar_us_player_modes_and_trial(real_similarity):
    from src import similarity

    at = _run(similar_new_script, "US Forward 1", fakes.US_SEASON)
    assert at.selectbox(key="similar_mode").options == [similarity.MODE_SHARED, similarity.MODE_TRIAL]
    assert set(at.dataframe[0].value["League"]) == {"MLS"}
    _mode(at, similarity.MODE_TRIAL)
    assert "2025-26" in set(at.dataframe[0].value["Season"])


def test_similar_incomplete_player_gets_the_message_and_can_switch_mode(real_similarity):
    from src import similarity

    at = _run(similar_new_script, "Gappy Forward", fakes.EUROPE_SEASON)
    assert ("This player&#x27;s data is incomplete for this comparison (missing: Dribbled Past). "
            "Try another mode.") in _markdown(at)
    assert len(at.dataframe) == 0 and not at.error
    _mode(at, similarity.MODE_TRIAL)
    assert len(at.dataframe) == 1


def test_similar_keeper_has_the_shared_mode_only(real_similarity):
    from src import similarity

    at = _run(similar_new_script, "Two Keeper 1", fakes.EUROPE_SEASON)
    control = at.selectbox(key="similar_mode")
    assert control.options == [similarity.MODE_SHARED] and control.disabled
    assert len(at.dataframe[0].value) == 6


def test_similar_fbref_player_keeps_the_fbref_method(stub_similar):
    from src import similarity

    at = _run(similar_script, "Mohamed Salah", fakes.LATEST)
    assert at.selectbox(key="similar_mode").options == [similarity.MODE_SHARED]
    assert "compared only with players who have all of those stats" in _captions(at)


def test_similar_trial_without_paired_season_says_so(real_similarity):
    from src import similarity

    at = _run(similar_new_script, "Late US Forward 0", fakes.US_LATE)
    _mode(at, similarity.MODE_TRIAL)
    assert "The paired season (2026-27) is not in the data yet" in _captions(at)
    assert "2026 players only" in _captions(at)
    assert set(at.dataframe[0].value["Season"]) == {"2026"}


def test_similar_trial_compare_view_reaches_the_paired_season(real_similarity):
    from src import similarity

    at = _run(similar_new_script, "US Forward 1", fakes.US_SEASON)
    _mode(at, similarity.MODE_TRIAL)
    seasons = list(at.dataframe[0].value["Season"])
    row = next(i for i, season in enumerate(seasons) if season == "2025-26")
    at.session_state["similar_table"] = {"selection": {"rows": [row], "columns": []}}
    at.run()
    assert not at.exception
    assert "Comparison unavailable" not in _captions(at)
    assert len(at.get("plotly_chart")) == 1
    traces = json.loads(at.get("plotly_chart")[0].proto.spec)["data"]
    markers = [t for t in traces if t.get("mode") == "markers"]
    assert len(markers[0]["x"]) == 7  # the seven shared stats
