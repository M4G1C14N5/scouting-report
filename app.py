"""
app.py
======
Streamlit scouting app — queries BigQuery, displays player stats
and similar players with percentile rankings.

Run locally:
    .venv/Scripts/python -m streamlit run app.py
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from src.similarity import find_similar_players, get_player_stats, GCP_PROJECT, BQ_DATASET

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Scouting Report", layout="wide")
st.title("Football Scouting Report")

# ── BigQuery client ───────────────────────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=GCP_PROJECT)


# ── Load full player index once ───────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_player_index() -> pd.DataFrame:
    bq = get_bq_client()
    sql = f"""
        SELECT Player, Season, Squad, League, position_group, minutes
        FROM `{GCP_PROJECT}.{BQ_DATASET}.outfield_features`
        UNION ALL
        SELECT Player, Season, Squad, League, position_group, minutes
        FROM `{GCP_PROJECT}.{BQ_DATASET}.goalkeeper_features`
        ORDER BY Player, Season
    """
    return bq.query(sql).to_dataframe()


@st.cache_data(ttl=3600)
def load_percentiles(position_group: str, season: str) -> pd.DataFrame:
    bq = get_bq_client()
    view_map = {
        "ATT": "attacker_percentiles",
        "MID": "midfielder_percentiles",
        "DEF": "defender_percentiles",
        "GK":  "goalkeeper_percentiles",
    }
    sql = f"""
        SELECT *
        FROM `{GCP_PROJECT}.{BQ_DATASET}.{view_map[position_group]}`
        WHERE Season = '{season}'
    """
    return bq.query(sql).to_dataframe()


# ── Percentile bar renderer ───────────────────────────────────────────────────
def percentile_bar(label: str, value: float):
    col1, col2, col3 = st.columns([3, 1, 6])
    col1.write(label)
    col2.write(f"{value:.0f}th")
    col3.progress(int(value) / 100)


PERCENTILE_LABELS = {
    "goals_per_90_pct":                 "Goals / 90",
    "assists_per_90_pct":               "Assists / 90",
    "nonpen_goals_per_90_pct":          "Non-pen Goals / 90",
    "xg_per_90_pct":                    "xG / 90",
    "npxg_per_90_pct":                  "npxG / 90",
    "shots_total_per_90_pct":           "Shots / 90",
    "shots_on_target_per_90_pct":       "Shots on Target / 90",
    "goals_per_shot_pct":               "Goals per Shot",
    "npxg_per_shot_pct":                "npxG per Shot",
    "key_passes_pct":                   "Key Passes",
    "passes_into_penalty_area_pct":     "Passes into Box",
    "progressive_carries_pct":          "Progressive Carries",
    "xag_per_90_pct":                   "xAG / 90",
    "passes_completion_pct_pct":        "Pass Completion %",
    "progressive_passes_pct":           "Progressive Passes",
    "passes_into_final_third_pct":      "Passes into Final Third",
    "tackles_total_pct":                "Tackles",
    "interceptions_pct":                "Interceptions",
    "tackles_plus_interceptions_pct":   "Tackles + Interceptions",
    "tackles_won_pct":                  "Tackles Won",
    "dribblers_tackled_pct_pct":        "Dribbler Tackle %",
    "blocks_total_pct":                 "Blocks",
    "clearances_pct":                   "Clearances",
    "errors_leading_to_shot_pct":       "Errors Leading to Shot",
    "progressive_passing_distance_pct": "Progressive Pass Distance",
    "save_pct_pct":                     "Save %",
    "goals_against_per_90_pct":         "Goals Against / 90",
    "clean_sheet_pct_pct":              "Clean Sheet %",
    "clean_sheets_pct":                 "Clean Sheets",
    "pk_save_pct_pct":                  "Penalty Save %",
    "shots_on_target_against_pct":      "SoT Against",
    "saves_pct":                        "Saves",
}


# ── Load data ─────────────────────────────────────────────────────────────────
player_index = load_player_index()
all_seasons  = sorted(player_index["Season"].unique(), reverse=True)
all_leagues  = sorted(player_index["League"].unique())

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")

# Season — checkbox for all, otherwise multiselect
all_seasons_checked = st.sidebar.checkbox("All seasons", value=False)
if all_seasons_checked:
    selected_seasons = all_seasons
else:
    selected_seasons = st.sidebar.multiselect(
        "Season(s)",
        options=all_seasons,
        default=[all_seasons[0]],
    )
    if not selected_seasons:
        selected_seasons = [all_seasons[0]]

# League — "All leagues" as first option
league_options   = ["All leagues"] + all_leagues
selected_league  = st.sidebar.selectbox("League", league_options)

# Team — filtered by selected league
if selected_league == "All leagues":
    league_filtered = player_index
else:
    league_filtered = player_index[player_index["League"] == selected_league]

teams_in_league = sorted(league_filtered["Squad"].unique())
team_options    = ["All teams"] + teams_in_league
selected_team   = st.sidebar.selectbox("Team", team_options)

# Apply season + league + team filters to build the player pool
filtered = player_index[player_index["Season"].isin(selected_seasons)]
if selected_league != "All leagues":
    filtered = filtered[filtered["League"] == selected_league]
if selected_team != "All teams":
    filtered = filtered[filtered["Squad"] == selected_team]

# Player — text search box filters the selectbox list
st.sidebar.header("Player Search")
search_text = st.sidebar.text_input("Search player name", placeholder="e.g. Salah")

available_players = sorted(filtered["Player"].unique())
if search_text:
    available_players = [p for p in available_players if search_text.lower() in p.lower()]

if not available_players:
    st.sidebar.warning("No players match your filters.")
    st.stop()

selected_player = st.sidebar.selectbox("Select player", available_players)

# ── Options ───────────────────────────────────────────────────────────────────
st.sidebar.header("Options")
top_n     = st.sidebar.slider("Similar players to show", min_value=5, max_value=20, value=10)
stat_mode = st.sidebar.radio("Stat display", ["Per 90", "Raw"])


# ── Main panel ────────────────────────────────────────────────────────────────
# The player may appear in multiple selected seasons — let user pick which to analyse
player_seasons = sorted(
    filtered[filtered["Player"] == selected_player]["Season"].unique(),
    reverse=True,
)

if len(player_seasons) > 1:
    selected_season = st.selectbox(
        f"{selected_player} appeared in multiple seasons — select one to analyse:",
        player_seasons,
    )
else:
    selected_season = player_seasons[0]

player_row     = player_index[
    (player_index["Player"] == selected_player) &
    (player_index["Season"] == selected_season)
].iloc[0]
position_group = player_row["position_group"]

bq = get_bq_client()

# Header metrics
st.subheader(selected_player)
info_cols = st.columns(4)
info_cols[0].metric("Club",    player_row["Squad"])
info_cols[1].metric("League",  player_row["League"])
info_cols[2].metric("Season",  selected_season)
info_cols[3].metric("Minutes", int(player_row["minutes"]))

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["Stats", "Percentile Ranks", "Similar Players"])

# ── Tab 1: Stats ──────────────────────────────────────────────────────────────
with tab1:
    try:
        stats    = get_player_stats(selected_player, selected_season, bq)
        stats_df = stats.to_frame(name="Value").reset_index()
        stats_df.columns = ["Stat", "Value"]

        identity_cols = {"Player", "Nation", "Position", "Squad", "League",
                         "Season", "position_group", "Age", "Born"}

        if stat_mode == "Per 90":
            mask = (
                stats_df["Stat"].str.endswith("_per_90") |
                stats_df["Stat"].str.endswith("_pct")
            ) & ~stats_df["Stat"].isin(identity_cols)
        else:
            mask = ~stats_df["Stat"].isin(identity_cols)

        st.dataframe(
            stats_df[mask].reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )
    except Exception as e:
        st.error(f"Could not load stats: {e}")

# ── Tab 2: Percentile ranks ───────────────────────────────────────────────────
with tab2:
    try:
        pct_df     = load_percentiles(position_group, selected_season)
        player_pct = pct_df[pct_df["Player"] == selected_player]

        if player_pct.empty:
            st.info("Percentile data not available (player may be below the 450-minute threshold).")
        else:
            row      = player_pct.iloc[0]
            pct_cols = [c for c in row.index if c.endswith("_pct") and c in PERCENTILE_LABELS]
            for col in pct_cols:
                val = row[col]
                if pd.notna(val):
                    percentile_bar(PERCENTILE_LABELS[col], float(val))
    except Exception as e:
        st.error(f"Could not load percentiles: {e}")

# ── Tab 3: Similar players ────────────────────────────────────────────────────
with tab3:
    try:
        if player_row["minutes"] < 450:
            st.warning(
                f"{selected_player} has only {int(player_row['minutes'])} minutes — "
                "below the 450-minute threshold for similarity search."
            )
        else:
            similar = find_similar_players(
                player=selected_player,
                season=selected_season,
                top_n=top_n,
                bq_client=bq,
            )
            similar["similarity_score"] = similar["similarity_score"].round(3)
            st.dataframe(similar, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Could not compute similarity: {e}")
