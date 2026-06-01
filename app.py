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
        SELECT Player, Season, Squad, League, Position, position_group, minutes
        FROM `{GCP_PROJECT}.{BQ_DATASET}.outfield_features`
        UNION ALL
        SELECT Player, Season, Squad, League, Position, position_group, minutes
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
    col1, col2, col3, col4 = st.columns([2.5, 0.8, 5, 1.5])
    col1.write(label)
    col2.write(f"{value:.0f}th")
    col3.progress(int(value) / 100)

    if value >= 66:
        col4.markdown(":green[**Excellent**]")
    elif value >= 33:
        col4.markdown(":orange[**Average**]")
    else:
        col4.markdown(":red[**Below Avg**]")


STAT_CATEGORIES = {
    # Playing Time
    "minutes": "Playing Time",
    "nineties_played": "Playing Time",
    # Shooting (includes Expected Goals)
    "std_goals": "Shooting",
    "goals_per_90": "Shooting",
    "nonpen_goals": "Shooting",
    "nonpen_goals_per_90": "Shooting",
    "penalty_kicks_made": "Shooting",
    "shots_total": "Shooting",
    "shots_total_per_90": "Shooting",
    "shots_on_target": "Shooting",
    "shots_on_target_per_90": "Shooting",
    "shots_on_target_pct": "Shooting",
    "goals_per_shot": "Shooting",
    "goals_per_shot_on_target": "Shooting",
    "avg_shot_distance": "Shooting",
    "std_xg": "Shooting",
    "xg_per_90": "Shooting",
    "npxg": "Shooting",
    "npxg_per_90": "Shooting",
    "npxg_per_shot": "Shooting",
    "goals_minus_xg": "Shooting",
    "nonpen_goals_minus_npxg": "Shooting",
    "npxg_plus_xag_per_90": "Shooting",
    # Passing & Assisting (includes Expected Assists)
    "std_assists": "Passing & Assisting",
    "assists_per_90": "Passing & Assisting",
    "xag": "Passing & Assisting",
    "xag_per_90": "Passing & Assisting",
    "expected_assisted_goals": "Passing & Assisting",
    "expected_assists": "Passing & Assisting",
    "passes_completed_total": "Passing & Assisting",
    "passes_attempted_total": "Passing & Assisting",
    "passes_completion_pct_total": "Passing & Assisting",
    "total_passing_distance": "Passing & Assisting",
    "passes_completion_pct_short": "Passing & Assisting",
    "passes_completion_pct_medium": "Passing & Assisting",
    "passes_completion_pct_long": "Passing & Assisting",
    "key_passes": "Passing & Assisting",
    "passes_into_final_third": "Passing & Assisting",
    "passes_into_penalty_area": "Passing & Assisting",
    "crosses_into_penalty_area": "Passing & Assisting",
    # Ball Progression
    "progressive_passes_received": "Ball Progression",
    "std_progressive_passes": "Ball Progression",
    "pass_progressive_passes": "Ball Progression",
    "progressive_passing_distance": "Ball Progression",
    "progressive_carries": "Ball Progression",
    # Dribbling & Possession
    "dribblers_tackled": "Dribbling & Possession",
    "dribblers_tackled_pct": "Dribbling & Possession",
    # Defending
    "tackles_total": "Defending",
    "tackles_won": "Defending",
    "tackles_def_3rd": "Defending",
    "tackles_mid_3rd": "Defending",
    "tackles_att_3rd": "Defending",
    "blocks_total": "Defending",
    "interceptions": "Defending",
    "tackles_plus_interceptions": "Defending",
    "clearances": "Defending",
    "errors_leading_to_shot": "Defending",
    # Discipline
    "yellow_cards": "Discipline",
    "red_cards": "Discipline",
    # Goalkeeper
    "goals_against": "Goalkeeper",
    "goals_against_per_90": "Goalkeeper",
    "shots_on_target_against": "Goalkeeper",
    "saves": "Goalkeeper",
    "save_pct": "Goalkeeper",
    "wins": "Goalkeeper",
    "draws": "Goalkeeper",
    "losses": "Goalkeeper",
    "clean_sheets": "Goalkeeper",
    "clean_sheet_pct": "Goalkeeper",
    "penalty_kicks_faced": "Goalkeeper",
    "penalty_kicks_allowed": "Goalkeeper",
    "penalty_kicks_saved": "Goalkeeper",
    "pk_save_pct": "Goalkeeper",
}

STAT_LABELS = {
    "minutes":                          "Minutes",
    "nineties_played":                  "90s",
    "std_goals":                        "Goals",
    "std_assists":                      "Assists",
    "nonpen_goals":                     "Non-Pen Goals",
    "penalty_kicks_made":               "Penalties Made",
    "yellow_cards":                     "Yellow Cards",
    "red_cards":                        "Red Cards",
    "std_xg":                           "xG",
    "npxg":                             "npxG",
    "xag":                              "xAG",
    "progressive_carries":              "Progressive Carries",
    "std_progressive_passes":           "Progressive Passes",
    "progressive_passes_received":      "Progressive Passes Received",
    "goals_per_90":                     "Goals / 90",
    "assists_per_90":                   "Assists / 90",
    "nonpen_goals_per_90":              "Non-Pen Goals / 90",
    "xg_per_90":                        "xG / 90",
    "xag_per_90":                       "xAG / 90",
    "npxg_per_90":                      "npxG / 90",
    "npxg_plus_xag_per_90":             "npxG + xAG / 90",
    "shots_total":                      "Shots",
    "shots_on_target":                  "Shots on Target",
    "shots_on_target_pct":              "Shot Accuracy %",
    "shots_total_per_90":               "Shots / 90",
    "shots_on_target_per_90":           "Shots on Target / 90",
    "goals_per_shot":                   "Goals per Shot",
    "goals_per_shot_on_target":         "Goals per SoT",
    "avg_shot_distance":                "Avg Shot Distance",
    "npxg_per_shot":                    "npxG per Shot",
    "goals_minus_xg":                   "Goals - xG",
    "nonpen_goals_minus_npxg":          "Non-Pen Goals - npxG",
    "passes_completed_total":           "Passes Completed",
    "passes_attempted_total":           "Passes Attempted",
    "passes_completion_pct_total":      "Pass Completion %",
    "total_passing_distance":           "Total Pass Distance",
    "progressive_passing_distance":     "Progressive Pass Distance",
    "passes_completion_pct_short":      "Short Pass Completion %",
    "passes_completion_pct_medium":     "Medium Pass Completion %",
    "passes_completion_pct_long":       "Long Pass Completion %",
    "expected_assisted_goals":          "Expected Assisted Goals",
    "expected_assists":                 "Expected Assists",
    "key_passes":                       "Key Passes",
    "passes_into_final_third":          "Passes into Final Third",
    "passes_into_penalty_area":         "Passes into Box",
    "crosses_into_penalty_area":        "Crosses into Box",
    "pass_progressive_passes":          "Progressive Passes",
    "tackles_total":                    "Tackles",
    "tackles_won":                      "Tackles Won",
    "tackles_def_3rd":                  "Tackles (Def 3rd)",
    "tackles_mid_3rd":                  "Tackles (Mid 3rd)",
    "tackles_att_3rd":                  "Tackles (Att 3rd)",
    "dribblers_tackled":                "Dribblers Tackled",
    "dribblers_tackled_pct":            "Dribbler Tackle %",
    "blocks_total":                     "Blocks",
    "interceptions":                    "Interceptions",
    "tackles_plus_interceptions":       "Tackles + Interceptions",
    "clearances":                       "Clearances",
    "errors_leading_to_shot":           "Errors Leading to Shot",
    "goals_against":                    "Goals Against",
    "goals_against_per_90":             "Goals Against / 90",
    "shots_on_target_against":          "Shots on Target Against",
    "saves":                            "Saves",
    "save_pct":                         "Save %",
    "wins":                             "Wins",
    "draws":                            "Draws",
    "losses":                           "Losses",
    "clean_sheets":                     "Clean Sheets",
    "clean_sheet_pct":                  "Clean Sheet %",
    "penalty_kicks_faced":              "Penalties Faced",
    "penalty_kicks_allowed":            "Penalties Allowed",
    "penalty_kicks_saved":              "Penalties Saved",
    "pk_save_pct":                      "Penalty Save %",
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

# ── Player Search (Main page, under title) ────────────────────────────────────
st.markdown("---")
col_search = st.columns([1, 2, 1])[1]
available_players = sorted(filtered["Player"].unique())
with col_search:
    selected_player = st.selectbox(
        "Search player",
        available_players,
        placeholder="Type to search (e.g. Salah)",
        key="player_select"
    )

# ── Options ───────────────────────────────────────────────────────────────────
st.sidebar.header("Options")
top_n     = st.sidebar.slider("Similar players to show", min_value=5, max_value=20, value=10)


# ── Main panel ────────────────────────────────────────────────────────────────
# The player may appear in multiple selected seasons — let user pick which to analyse
player_seasons = sorted(
    filtered[filtered["Player"] == selected_player]["Season"].unique(),
    reverse=False,
)

if len(player_seasons) > 1:
    st.write(f"{selected_player} appeared in multiple seasons — select one:")
    cols = st.columns(len(player_seasons))
    selected_season = None
    for col, season in zip(cols, player_seasons):
        if col.button(season, key=f"season_{season}", width='stretch'):
            selected_season = season

    if selected_season is None:
        selected_season = player_seasons[0]
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
info_cols = st.columns(5)
info_cols[0].metric("Position", player_row["Position"])
info_cols[1].metric("Club",    player_row["Squad"])
info_cols[2].metric("League",  player_row["League"])
info_cols[3].metric("Season",  selected_season)
info_cols[4].metric("Minutes", int(player_row["minutes"]))

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

        display_df = stats_df[~stats_df["Stat"].isin(identity_cols)].copy()
        display_df["Category"] = display_df["Stat"].map(lambda x: STAT_CATEGORIES.get(x, "Other"))
        display_df["Stat"] = display_df["Stat"].map(lambda x: STAT_LABELS.get(x, x))

        # Sort by category then by stat name
        display_df = display_df.sort_values(["Category", "Stat"]).reset_index(drop=True)

        # Display by category
        for category in display_df["Category"].unique():
            st.subheader(category)
            cat_df = display_df[display_df["Category"] == category][["Stat", "Value"]].reset_index(drop=True)
            st.dataframe(cat_df, width='stretch', hide_index=True, height='content')
    except Exception as e:
        st.error(f"Could not load stats: {e}")

# ── Tab 2: Percentile ranks ───────────────────────────────────────────────────
with tab2:
    try:
        if position_group == "GK":
            table = f"`{GCP_PROJECT}.{BQ_DATASET}.goalkeeper_features`"
        else:
            table = f"`{GCP_PROJECT}.{BQ_DATASET}.outfield_features`"

        pool_sql = f"""
            SELECT *
            FROM {table}
            WHERE position_group = '{position_group}'
              AND Season = '{selected_season}'
              AND minutes >= 450
        """
        pool_df = bq.query(pool_sql).to_dataframe()

        if pool_df.empty:
            st.info("Not enough players in this position/season to compute percentiles.")
        else:
            player_data = pool_df[pool_df["Player"] == selected_player]
            if player_data.empty:
                st.info("Player not in percentile pool (may be below 450-minute threshold).")
            else:
                player_row = player_data.iloc[0]

                identity_cols = {"Player", "Nation", "Position", "Squad", "League", "Season", "position_group", "Age", "Born"}
                numeric_cols = [c for c in pool_df.columns if c not in identity_cols and pool_df[c].dtype in ['float64', 'int64']]

                # Group stats by category
                stats_by_category = {}
                for col in numeric_cols:
                    val = player_row[col]
                    if pd.notna(val):
                        pct = (pool_df[col] < val).sum() / len(pool_df) * 100
                        category = STAT_CATEGORIES.get(col, "Other")
                        label = STAT_LABELS.get(col, col)
                        if category not in stats_by_category:
                            stats_by_category[category] = []
                        stats_by_category[category].append((label, pct))

                # Display by category
                for category in sorted(stats_by_category.keys()):
                    st.subheader(category)
                    for label, pct in sorted(stats_by_category[category]):
                        percentile_bar(label, pct)
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
            st.dataframe(similar, width='stretch', hide_index=True)
    except Exception as e:
        st.error(f"Could not compute similarity: {e}")
