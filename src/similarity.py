"""
similarity.py
=============
Player similarity model using cosine similarity on z-scored per-90 stats.

How it works
------------
1. Pull the outfield or goalkeeper rows from BigQuery (the outfield_all /
   goalkeeper_all views by default: FBref plus the newer sources).
2. Filter to players with >= 450 minutes (serious sample size).
3. For the queried player's position group, select a position-specific
   set of features (feature template). Newer sources leave some stats
   empty (no progressive passing after 2023-24, no tackles in the US
   leagues, no xG outside the Big 5 for most leagues), so keep only the
   features the queried player has and compare only with candidates who
   have every one of them. While there are fewer than 20 candidates, drop
   the feature the pool has least often and look again. Fewer than 3
   features left raises NotEnoughStatsError. See select_features.
4. Z-score the kept feature columns over the candidates and the queried
   player, so no single stat dominates by scale.
5. Compute cosine similarity between the queried player and every
   candidate.
6. Return the top N most similar players with their similarity score.

Position feature templates
--------------------------
Each template is a curated list of stats relevant to that position group:
stored per-90 columns, rates and percentages, and season totals. Every
season total (key passes, tackles, saves, dribbles, xG Chain, goals
added...: PER90_FEATURES) is divided by 90s played before step 4, so a
player with more minutes is not favoured. Earlier versions compared the
FBref totals (key passes, progressive carries, tackles...) as raw season
totals, so FBref results changed when they moved to per 90.

The original FBref features come first; the newer-source features
(API-Football duels and dribbles, Understat xG Chain, ASA goals added)
follow. A season has only its own sources' stats, so step 3 picks the
right set for each era on its own.
"""

import os
import numpy as np
import pandas as pd
from google.cloud import bigquery

from src.ui import metrics
from src.ui.metrics import KIND

GCP_PROJECT = os.environ.get("GCP_PROJECT", "scouting-466001")
BQ_DATASET  = os.environ.get("BQ_DATASET", "scouting")

# Tables (or views) the app reads. The defaults are the views that union the
# FBref tables with the soccer-data tables and add a Source column.
OUTFIELD_TABLE = os.environ.get("BQ_OUTFIELD_TABLE", "outfield_all")
GOALKEEPER_TABLE = os.environ.get("BQ_GOALKEEPER_TABLE", "goalkeeper_all")

MIN_MINUTES = 450
MIN_FEATURES = 3          # fewer usable template stats than this: no similarity
MIN_CANDIDATES = 20       # drop the rarest stat while fewer players than this have them all
NOT_ENOUGH_STATS = "Not enough comparable stats for this player in this season"


class NotEnoughStatsError(ValueError):
    """The player-season shares too few template stats with its pool."""

    def __init__(self, message: str = NOT_ENOUGH_STATS) -> None:
        super().__init__(message)

# Feature templates: one list of columns per position group.
# All columns must exist in outfield_features / goalkeeper_features.
# Rows from the newer sources may leave some of them null; see select_features.
FEATURE_TEMPLATES = {
    "ATT": [
        # Output metrics (reduced weight by including fewer)
        "goals_per_90",
        "xg_per_90",
        "npxg_per_90",
        "goals_per_shot",
        # Playmaking & progression (increased weight — what wingers do)
        "assists_per_90",
        "xag_per_90",
        "key_passes",
        "passes_into_final_third",
        "passes_into_penalty_area",
        "crosses_into_penalty_area",
        "progressive_carries",
        "progressive_passing_distance",
        # Efficiency & involvement
        "shots_total_per_90",
        "passes_completion_pct_total",
        "npxg_plus_xag_per_90",
        # Newer sources: dribbling and duels (API-Football), xG Chain (Understat),
        # goals added by action (ASA; the total is left out, it is the sum of the parts)
        "dribbles_completed",
        "dribble_success_pct",
        "fouls_drawn",
        "duels_won_pct",
        "xg_chain",
        "ga_shooting",
        "ga_receiving",
    ],
    "MID": [
        # Progression & dribbling (heavily weighted — defines midfielder style)
        "progressive_carries",
        "progressive_passing_distance",
        "pass_progressive_passes",
        "passes_into_final_third",
        # Playmaking
        "assists_per_90",
        "xag_per_90",
        "key_passes",
        "passes_into_penalty_area",
        "crosses_into_penalty_area",
        # Involvement & efficiency
        "passes_completion_pct_total",
        "goals_per_90",
        "shots_total_per_90",
        "npxg_per_90",
        # Defending
        "tackles_plus_interceptions",
        "interceptions",
        # Newer sources: build-up and creation (Understat), carrying and duels
        # (API-Football), passing over expected and goals added (ASA)
        "xg_chain",
        "xg_buildup",
        "dribbles_completed",
        "duels_won_pct",
        "passes_completed_over_expected_p100",
        "ga_passing",
        "ga_interrupting",
    ],
    "DEF": [
        "tackles_total",
        "tackles_won",
        "tackles_def_3rd",
        "tackles_mid_3rd",
        "dribblers_tackled_pct",
        "interceptions",
        "tackles_plus_interceptions",
        "blocks_total",
        "clearances",
        "errors_leading_to_shot",
        "passes_completion_pct_total",
        "progressive_passing_distance",
        "passes_into_final_third",
        "goals_per_90",
        "assists_per_90",
        # Newer sources: duels, dribbled past and fouls (API-Football), build-up
        # (Understat), passing over expected and length, goals added (ASA)
        "duels_won_pct",
        "dribbled_past",
        "fouls_committed",
        "xg_buildup",
        "passes_completed_over_expected_p100",
        "avg_pass_distance",
        "avg_vertical_pass_distance",
        "ga_interrupting",
        "ga_passing",
    ],
    "GK": [
        "save_pct",
        "goals_against_per_90",
        "clean_sheet_pct",
        "pk_save_pct",
        # shots on target against is left out: it equals saves plus goals against
        "saves",
        # Newer sources: average API-Football match rating
        "match_rating",
    ],
}

# Metric kinds stored as season totals (src/ui/metrics.py). Similarity compares
# them per 90 minutes, so a player with more minutes does not look different.
SEASON_TOTAL_KINDS = frozenset({"count", "total", "tally"})

# --- Compare modes (newer seasons) ----------------------------------------------
# FBref seasons keep FEATURE_TEMPLATES and the feature selection above. The
# newer seasons use fixed templates, the same stats for every player: a
# candidate needs every one of them, nothing is dropped, and a query player
# missing one gets IncompleteDataError instead of a ranking on fewer stats.
MODE_SHARED = "All leagues, shared stats"
MODE_BIG_FIVE = "Big five, all stats"
MODE_TRIAL = "Europe and US together (trial)"
MODES = (MODE_SHARED, MODE_BIG_FIVE, MODE_TRIAL)

BIG_FIVE_NOTE = "Big five only: uses Understat stats that the other leagues do not have."
TRIAL_NOTE = ("Trial: compares European and US players on the seven stats both regions have "
              "(goals, assists, shots, shots on target, key passes, passes, pass completion). "
              "League strength differs a lot between these leagues, so read a match as a "
              "similar style of play, not a similar level. Seasons are paired by the year they "
              "end: US 2026 with the European 2025/26, US 2025 with 2024/25.")
NO_PAIRED_SEASON = "No paired European season with matching stats for this season."
INCOMPLETE = ("This player's data is incomplete for this comparison (missing: {missing}). "
              "Try another mode.")

# Family stats left out of the similarity templates (they stay Profile and
# Stats bars):
#   goals_added          the sum of its six parts, which are all in the US set:
#                        comparing on both would count it twice
#   dribble_success_pct  0/0 (null) for a player who attempted no dribbles, which
#                        would exclude him; dribbles attempted and completed
#                        (per 90) carry the dribbling instead
NOT_IN_TEMPLATES = frozenset({"goals_added", "dribble_success_pct"})
# Newer-season stats that say little about how a player plays, left out of the
# role-aware comparison (they stay Profile and Stats bars).
STYLE_DROP = frozenset({"yellow_cards", "fouls_committed", "match_rating", "blocks_total"})

# Pool key for every outfield player (ATT, MID and DEF) of a season: newer-season
# outfielders are compared across positions on style plus role mix.
OUTFIELD = "OUT"
OUTFIELD_GROUPS = ("ATT", "MID", "DEF")
# Weight of the role-mix block against the z-scored style block. Each role share
# is centred on the pool mean and multiplied by this; the style stats have unit
# variance each. Set on a 2025/26 sanity set: at 0-1 a fullback (Grimaldo) still
# reached Foden's top 5; at 3 hybrids lost their wide matches; 2 kept strikers,
# holding mids and centre-backs in role while hybrids matched hybrids.
ROLE_WEIGHT = 2.0
# Style z-scores are capped at +/- this, so one freak stat cannot decide a match on
# its own: uncapped, Yamal's dribbling (+7 SD) and Olise's assists (+6 SD) pointed
# their vectors apart and Yamal ranked 17th for Olise; capped at 3 he is first.
Z_CAP = 3.0


class IncompleteDataError(ValueError):
    """The query player lacks a stat of the mode's fixed template."""

    def __init__(self, missing: list[str]) -> None:
        self.missing = list(missing)
        super().__init__(INCOMPLETE.format(missing=", ".join(metrics.label(c) for c in missing)))


class NoPairedSeasonError(ValueError):
    """The trial mode has no other-region season with the shared stats (it is
    missing, or an FBref season)."""

    def __init__(self) -> None:
        super().__init__(NO_PAIRED_SEASON)


def paired_season(season: str) -> str | None:
    """The other region's season for the trial mode, by the year the season
    ends: US year Y pairs with the European (Y-1)-Y, and the European (Y-1)-Y
    with US Y (US 2026 <-> 2025-26)."""
    text = str(season)
    if text.isdigit() and len(text) == 4:
        return f"{int(text) - 1}-{text}"
    head, sep, tail = text.partition("-")
    if sep and head.isdigit() and tail.isdigit() and len(head) == len(tail) == 4:
        return tail
    return None


def modes_for(family: str, group: str, big_five: bool) -> list[str]:
    """Compare modes offered for a player. FBref seasons and goalkeepers have
    the default only; big five only for big-five players."""
    if family == metrics.FBREF or group == "GK":
        return [MODE_SHARED]
    modes = [MODE_SHARED]
    if big_five:
        modes.append(MODE_BIG_FIVE)
    modes.append(MODE_TRIAL)
    return modes


def mode_template(mode: str, family: str, group: str) -> list[str]:
    """The features a mode compares on."""
    if family == metrics.FBREF:
        return list(FEATURE_TEMPLATES[group])
    if mode == MODE_TRIAL:
        return list(metrics.SHARED_SET)
    keys = list(metrics.family_set(family, group))
    if mode == MODE_BIG_FIVE:
        keys += list(metrics.BIG_FIVE_EXTRAS)
    drop = NOT_IN_TEMPLATES | (STYLE_DROP if group != "GK" else frozenset())
    return [k for k in keys if k not in drop]


def _all_templates() -> list[list[str]]:
    out = [list(t) for t in FEATURE_TEMPLATES.values()]
    for family in (metrics.EUROPE, metrics.US):
        for group in ("ATT", "MID", "DEF", "GK"):
            for mode in MODES:
                out.append(mode_template(mode, family, group))
            # the whole family set, STYLE_DROP included: compare views rank on it per 90 too
            out.append(list(metrics.family_set(family, group)))
    return out


# Every season total in a template (key passes, tackles, saves, dribbles,
# passes, cards, xG Chain, goals added...). Rates, percentages, distances and
# the stored per-90 columns are compared as they are.
PER90_FEATURES = frozenset(
    c for template in _all_templates() for c in template
    if KIND.get(c) in SEASON_TOTAL_KINDS
)


def mode_pool(pool: pd.DataFrame, query_row, mode: str, same_group: bool = True) -> pd.DataFrame:
    """The rows a mode compares with: the same position group (or, with
    same_group=False, every outfield group); the query's family and season
    (shared), big-five players of that season (big five), or both regions in
    the season and its paired season (trial)."""
    season = query_row["Season"]
    family = metrics.family_of(query_row.get("Source"))
    fam = (pool["Source"].map(metrics.family_of) if "Source" in pool.columns
           else pd.Series(metrics.FBREF, index=pool.index))
    keep = (pool["position_group"] == query_row["position_group"] if same_group
            else pool["position_group"].isin(OUTFIELD_GROUPS))
    if mode == MODE_TRIAL:
        keep &= fam.isin([metrics.EUROPE, metrics.US])
        keep &= pool["Season"].isin([season, paired_season(season)])
    else:
        keep &= (fam == family) & (pool["Season"] == season)
        if mode == MODE_BIG_FIVE:
            keep &= pool["League"].isin(metrics.BIG_FIVE)
    return pool[keep]


def table_ref(table: str) -> str:
    """Fully qualified, backquoted BigQuery name of a table in the dataset."""
    return f"`{GCP_PROJECT}.{BQ_DATASET}.{table}`"


def pool_sql(position_group: str, season: str, paired: bool = False) -> str:
    """The position-pool query: one season, or with paired=True the season and
    its paired season (the trial mode)."""
    table = table_ref(GOALKEEPER_TABLE if position_group == "GK" else OUTFIELD_TABLE)
    other = paired_season(season) if paired else None
    seasons = f"Season IN ('{season}', '{other}')" if other else f"Season = '{season}'"
    group = (f"position_group IN ({', '.join(repr(g) for g in OUTFIELD_GROUPS)})"
             if position_group == OUTFIELD else f"position_group = '{position_group}'")
    return f"""
        SELECT *
        FROM {table}
        WHERE {group}
          AND {seasons}
          AND minutes >= {MIN_MINUTES}
    """


def _fetch_features(bq_client: bigquery.Client, position_group: str, season: str,
                    paired: bool = False) -> pd.DataFrame:
    """Pull feature table rows for a given position group and season from BigQuery."""
    return bq_client.query(pool_sql(position_group, season, paired)).to_dataframe(create_bqstorage_client=False)


def _zscore_matrix(df: pd.DataFrame, feature_cols: list[str]) -> np.ndarray:
    """
    Z-score each feature column across the pool.
    NaN values are filled with 0 (pool mean after z-scoring).
    """
    X = df[feature_cols].copy().astype(float)
    means = X.mean()
    stds  = X.std().replace(0, 1)   # avoid divide-by-zero for constant columns
    X = (X - means) / stds
    return X.fillna(0).values


def _cosine_similarity(query_vec: np.ndarray, matrix: np.ndarray) -> np.ndarray:
    """Cosine similarity between one vector and every row in a matrix."""
    query_norm = query_vec / (np.linalg.norm(query_vec) + 1e-9)
    norms      = np.linalg.norm(matrix, axis=1, keepdims=True) + 1e-9
    return (matrix / norms) @ query_norm


def with_per90(pool_df: pd.DataFrame, template: list[str]) -> pd.DataFrame:
    """A copy of the pool with the template's PER90_FEATURES divided by 90s
    played (minutes / 90). Null when minutes are missing or zero."""
    out = pool_df.copy()
    nineties = pd.to_numeric(out["minutes"], errors="coerce") / 90
    nineties = nineties.where(nineties > 0)
    for c in template:
        if c in PER90_FEATURES and c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce") / nineties
    return out


def player_features(pool_df: pd.DataFrame, query_row, template: list[str]) -> list[str]:
    """Template features the query player has (non-null), in template order."""
    return [c for c in template if c in pool_df.columns and not pd.isna(query_row.get(c))]


def select_features(
    pool_df: pd.DataFrame,
    query_mask: pd.Series,
    template: list[str],
) -> tuple[list[str], pd.Series]:
    """(features, candidates) for the query row marked by `query_mask`.

    features: the template features the query player has. candidates: the
    other pool rows that have every one of those features. While there are
    fewer than MIN_CANDIDATES candidates, the feature with the lowest
    coverage in the pool is dropped (the first in template order on a tie)
    and the candidates are recomputed. Dropping stops early when every other
    row is already a candidate, since no drop could add one: a small pool
    with every stat present keeps the full template.

    Raises NotEnoughStatsError when fewer than MIN_FEATURES features remain."""
    query_row = pool_df[query_mask].iloc[0]
    features = player_features(pool_df, query_row, template)
    others = ~query_mask
    while True:
        if len(features) < MIN_FEATURES:
            raise NotEnoughStatsError()
        candidates = pool_df[features].notna().all(axis=1) & others
        n = int(candidates.sum())
        if n >= MIN_CANDIDATES or n == int(others.sum()):
            return features, candidates
        rarest = pool_df[features].notna().mean().idxmin()
        features = [c for c in features if c != rarest]


def rank_similar(
    pool_df: pd.DataFrame,
    player: str,
    season: str,
    template: list[str],
    top_n: int,
) -> pd.DataFrame:
    """Steps 3-6 on a fetched pool: the season totals per 90 (with_per90),
    select the features and candidates, z-score over the candidates and the
    query player, cosine, top N. Raises
    NotEnoughStatsError (see select_features). With every stat present for
    every row this is exactly the original full-template ranking."""
    pool_reset = with_per90(pool_df.reset_index(drop=True), template)
    query_mask = (pool_reset["Player"] == player) & (pool_reset["Season"] == season)
    feature_cols, candidates = select_features(pool_reset, query_mask, template)

    scored = pool_reset[candidates | query_mask].reset_index(drop=True).copy()
    scored_query = (scored["Player"] == player) & (scored["Season"] == season)
    positional_idx = scored.index[scored_query][0]

    # ── 4. Z-score the feature matrix ─────────────────────────────────────────
    X         = _zscore_matrix(scored, feature_cols)
    query_vec = X[positional_idx]

    # ── 5. Cosine similarity against the candidates ───────────────────────────
    scored["similarity_score"] = _cosine_similarity(query_vec, X)

    # ── 6. Exclude the query player themselves, sort, return top N ────────────
    return (
        scored[~scored_query]
        .sort_values("similarity_score", ascending=False)
        .head(top_n)[["Player", "Squad", "League", "Season", "position_group", "minutes", "similarity_score"]]
        .reset_index(drop=True)
    )


def rank_fixed(
    pool_df: pd.DataFrame,
    player: str,
    season: str,
    template: list[str],
    top_n: int,
) -> pd.DataFrame:
    """A fixed-template ranking: season totals per 90, the candidates are the
    rows with every template feature, z-score over them and the query player,
    cosine, top N. Nothing is dropped. Raises IncompleteDataError when the
    query player lacks a feature, NotEnoughStatsError for an empty template."""
    if not template:
        raise NotEnoughStatsError()
    pool_reset = with_per90(pool_df.reset_index(drop=True), template)
    query_mask = (pool_reset["Player"] == player) & (pool_reset["Season"] == season)
    query_row = pool_reset[query_mask].iloc[0]
    missing = [c for c in template if c not in pool_reset.columns or pd.isna(query_row[c])]
    if missing:
        raise IncompleteDataError(missing)
    candidates = pool_reset[template].notna().all(axis=1) & ~query_mask

    scored = pool_reset[candidates | query_mask].reset_index(drop=True).copy()
    scored_query = (scored["Player"] == player) & (scored["Season"] == season)
    X = _zscore_matrix(scored, template)
    scored["similarity_score"] = _cosine_similarity(X[scored.index[scored_query][0]], X)
    return (
        scored[~scored_query]
        .sort_values("similarity_score", ascending=False)
        .head(top_n)[["Player", "Squad", "League", "Season", "position_group", "minutes", "similarity_score"]]
        .reset_index(drop=True)
    )


RESULT_COLUMNS = ["Player", "Squad", "League", "Season", "position_group", "minutes"]


def has_role_mix(row) -> bool:
    return all(not pd.isna(row.get(c)) for c in metrics.ROLE_COLUMNS)


def rank_role_aware(
    pool_df: pd.DataFrame,
    player: str,
    season: str,
    template: list[str],
    top_n: int,
    role_weight: float | None = None,
) -> pd.DataFrame:
    """Rank on style plus role mix. Candidates: rows with every template stat
    and a role mix (any position group). Style block: the template, season
    totals per 90, z-scored over the candidates and the query player. Role
    block: the five role shares minus their mean over the same rows, times
    role_weight (ROLE_WEIGHT by default). Cosine on both blocks together, so a
    player with the same numbers from a neighbouring role can still rank, a
    step below one from the same role. Raises IncompleteDataError when the
    query player lacks a template stat; the caller checks has_role_mix."""
    if not template:
        raise NotEnoughStatsError()
    weight = ROLE_WEIGHT if role_weight is None else role_weight
    roles = list(metrics.ROLE_COLUMNS)
    pool_reset = with_per90(pool_df.reset_index(drop=True), template)
    query_mask = (pool_reset["Player"] == player) & (pool_reset["Season"] == season)
    query_row = pool_reset[query_mask].iloc[0]
    missing = [c for c in template if c not in pool_reset.columns or pd.isna(query_row[c])]
    if missing:
        raise IncompleteDataError(missing)
    complete = pool_reset[template + roles].notna().all(axis=1)
    scored = pool_reset[(complete & ~query_mask) | query_mask].reset_index(drop=True).copy()
    scored_query = (scored["Player"] == player) & (scored["Season"] == season)
    style = np.clip(_zscore_matrix(scored, template), -Z_CAP, Z_CAP)
    shares = scored[roles].astype(float).fillna(0.0)
    role_block = (shares - shares.mean()).values * weight
    X = np.hstack([style, role_block])
    scored["similarity_score"] = _cosine_similarity(X[scored.index[scored_query][0]], X)
    main = scored[roles].astype(float).fillna(0.0)
    scored["main_role"] = main.idxmax(axis=1).str.removeprefix("role_").str.upper()
    scored["main_role_share"] = main.max(axis=1)
    return (
        scored[~scored_query]
        .sort_values("similarity_score", ascending=False)
        .head(top_n)[RESULT_COLUMNS + ["main_role", "main_role_share", "similarity_score"]]
        .reset_index(drop=True)
    )


def similar_in_pool(
    pool_df: pd.DataFrame,
    player: str,
    season: str,
    mode: str = MODE_SHARED,
    top_n: int = 10,
) -> pd.DataFrame:
    """Similar players from an already fetched pool (the season, plus the
    paired season for the trial mode). FBref seasons: rank_similar on
    FEATURE_TEMPLATES, exactly as before. Newer seasons: rank_fixed on the
    mode's template and pool. Raises ValueError when the player is not in the
    pool or the mode is not offered for him."""
    query = (pool_df["Player"] == player) & (pool_df["Season"] == season)
    if not query.any():
        raise ValueError(f"'{player}' ({season}) is not in the {MIN_MINUTES}+ minute pool.")
    row = pool_df[query].iloc[0]
    family = metrics.family_of(row.get("Source"))
    group = row["position_group"]
    big_five = metrics.is_big_five(row.get("Source"), row.get("League"))
    if mode not in modes_for(family, group, big_five):
        raise ValueError(f"'{mode}' is not available for {player} ({season}).")
    if family == metrics.FBREF:
        same = (pool_df["Season"] == season) & (pool_df["position_group"] == group)
        return rank_similar(pool_df[same], player, season, FEATURE_TEMPLATES[group], top_n)
    role_aware = group != "GK" and has_role_mix(row)
    compared = mode_pool(pool_df, row, mode, same_group=not role_aware)
    if mode == MODE_TRIAL and not (compared["Season"] == paired_season(season)).any():
        raise NoPairedSeasonError()  # FBref rows never count: mode_pool keeps Europe and US
    template = mode_template(mode, family, group)
    if role_aware:
        return rank_role_aware(compared, player, season, template, top_n)
    return rank_fixed(compared, player, season, template, top_n)


def find_similar_players(
    player: str,
    season: str,
    top_n: int = 10,
    bq_client: bigquery.Client | None = None,
    mode: str = MODE_SHARED,
) -> pd.DataFrame:
    """
    Find the top_n most similar players to a given player in a given season.

    Parameters
    ----------
    player : str
        Exact player name as it appears in the database.
    season : str
        Season string, e.g. "2022-2023".
    top_n : int
        Number of similar players to return (default 10).
    bq_client : bigquery.Client, optional
        Reuse an existing client. If None, a new one is created.
    mode : str
        MODE_SHARED (default), MODE_BIG_FIVE or MODE_TRIAL (see modes_for).
        The trial mode also fetches the paired season.

    Returns
    -------
    pd.DataFrame
        Columns: Player, Squad, League, Season, position_group,
                 minutes, similarity_score (0–1, descending).

    Raises
    ------
    NotEnoughStatsError
        When fewer than MIN_FEATURES template stats are usable (see
        select_features). It is a ValueError.
    IncompleteDataError
        A newer-season player lacks a stat of the mode's fixed template.
        Also a ValueError.
    """
    if bq_client is None:
        bq_client = bigquery.Client(project=GCP_PROJECT)

    # ── 1. Find the player's position group ──────────────────────────────────
    lookup_sql = f"""
        SELECT position_group
        FROM {table_ref(OUTFIELD_TABLE)}
        WHERE Player = @player AND Season = @season
        UNION ALL
        SELECT position_group
        FROM {table_ref(GOALKEEPER_TABLE)}
        WHERE Player = @player AND Season = @season
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("player", "STRING", player),
            bigquery.ScalarQueryParameter("season", "STRING", season),
        ]
    )
    result = bq_client.query(lookup_sql, job_config=job_config).to_dataframe(create_bqstorage_client=False)
    if result.empty:
        raise ValueError(f"Player '{player}' not found for season '{season}'.")

    position_group = result.iloc[0]["position_group"]

    # ── 2. Fetch full pool for that position group and season(s) ──────────────
    pool_df = _fetch_features(bq_client, position_group, season, paired=mode == MODE_TRIAL)

    if pool_df.empty:
        raise ValueError(f"No data found for position group '{position_group}'.")

    # ── 3. Locate the query player in the pool ────────────────────────────────
    query_mask = (pool_df["Player"] == player) & (pool_df["Season"] == season)
    if not query_mask.any():
        raise ValueError(
            f"'{player}' ({season}) is in the database but below the "
            f"{MIN_MINUTES}-minute threshold and cannot be used as a query."
        )

    return similar_in_pool(pool_df, player, season, mode, top_n)


def get_player_stats(
    player: str,
    season: str,
    bq_client: bigquery.Client | None = None,
) -> pd.Series:
    """
    Return all feature columns for a single player-season.
    Used by the Streamlit app to display a player's stat card.
    """
    if bq_client is None:
        bq_client = bigquery.Client(project=GCP_PROJECT)

    for table in [OUTFIELD_TABLE, GOALKEEPER_TABLE]:
        sql = f"""
            SELECT *
            FROM {table_ref(table)}
            WHERE Player = @player AND Season = @season
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("player", "STRING", player),
                bigquery.ScalarQueryParameter("season", "STRING", season),
            ]
        )
        df = bq_client.query(sql, job_config=job_config).to_dataframe(create_bqstorage_client=False)
        if not df.empty:
            return df.iloc[0]

    raise ValueError(f"Player '{player}' not found for season '{season}'.")
