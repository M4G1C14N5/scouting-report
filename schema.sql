-- ============================================================
-- Scouting Report — BigQuery Schema
-- Dataset: scouting
-- ============================================================
-- Run this file once to set up all tables and views.
-- The loader script (gcp_loader.py) populates the raw tables.
-- ============================================================


-- ============================================================
-- LAYER 1: RAW TABLES
-- Direct loads from cleaned Excel files. Never transformed.
-- ============================================================

CREATE TABLE IF NOT EXISTS `scouting.standard` (
    Player              STRING,
    Nation              STRING,
    Position            STRING,
    Squad               STRING,
    League              STRING,
    Age                 INT64,
    Born                INT64,
    matches_played      INT64,
    starts              INT64,
    minutes             INT64,
    nineties_played     FLOAT64,
    goals               INT64,
    assists             INT64,
    goals_plus_assists  INT64,
    nonpen_goals        INT64,
    penalty_kicks_made  INT64,
    penalty_kicks_attempted INT64,
    yellow_cards        INT64,
    red_cards           INT64,
    xg                  FLOAT64,
    npxg                FLOAT64,
    xag                 FLOAT64,
    npxg_plus_xag       FLOAT64,
    progressive_carries INT64,
    progressive_passes  INT64,
    progressive_passes_received INT64,
    goals_per_90                    FLOAT64,
    assists_per_90                  FLOAT64,
    goals_plus_assists_per_90       FLOAT64,
    nonpen_goals_per_90             FLOAT64,
    goals_plus_assists_minus_pk_per_90 FLOAT64,
    xg_per_90                       FLOAT64,
    xag_per_90                      FLOAT64,
    xg_plus_xag_per_90              FLOAT64,
    npxg_per_90                     FLOAT64,
    npxg_plus_xag_per_90            FLOAT64,
    Season              STRING
);

CREATE TABLE IF NOT EXISTS `scouting.shooting` (
    Player              STRING,
    Nation              STRING,
    Position            STRING,
    Squad               STRING,
    League              STRING,
    Age                 INT64,
    Born                INT64,
    `90s`               FLOAT64,
    goals               INT64,
    shots_total         INT64,
    shots_on_target     INT64,
    shots_on_target_pct         FLOAT64,
    shots_total_per_90          FLOAT64,
    shots_on_target_per_90      FLOAT64,
    goals_per_shot              FLOAT64,
    goals_per_shot_on_target    FLOAT64,
    avg_shot_distance           FLOAT64,
    shots_from_free_kicks       INT64,
    penalty_kicks_made          INT64,
    penalty_kicks_attempted     INT64,
    xg                          FLOAT64,
    npxg                        FLOAT64,
    npxg_per_shot               FLOAT64,
    goals_minus_xg              FLOAT64,
    nonpen_goals_minus_npxg     FLOAT64,
    Season              STRING
);

CREATE TABLE IF NOT EXISTS `scouting.passing` (
    Player              STRING,
    Nation              STRING,
    Position            STRING,
    Squad               STRING,
    League              STRING,
    Age                 INT64,
    Born                INT64,
    `90s`               FLOAT64,
    passes_completed_total      INT64,
    passes_attempted_total      INT64,
    passes_completion_pct_total FLOAT64,
    total_passing_distance      INT64,
    progressive_passing_distance INT64,
    passes_completed_short      INT64,
    passes_attempted_short      INT64,
    passes_completion_pct_short FLOAT64,
    passes_completed_medium     INT64,
    passes_attempted_medium     INT64,
    passes_completion_pct_medium FLOAT64,
    passes_completed_long       INT64,
    passes_attempted_long       INT64,
    passes_completion_pct_long  FLOAT64,
    assists                     INT64,
    expected_assisted_goals     FLOAT64,
    expected_assists            FLOAT64,
    assists_minus_expected_assisted_goals FLOAT64,
    key_passes                  INT64,
    passes_into_final_third     INT64,
    passes_into_penalty_area    INT64,
    crosses_into_penalty_area   INT64,
    progressive_passes          INT64,
    Season              STRING
);

CREATE TABLE IF NOT EXISTS `scouting.defending` (
    Player              STRING,
    Nation              STRING,
    Position            STRING,
    Squad               STRING,
    League              STRING,
    Age                 INT64,
    Born                INT64,
    `90s`               FLOAT64,
    tackles_total               INT64,
    tackles_won                 INT64,
    tackles_def_3rd             INT64,
    tackles_mid_3rd             INT64,
    tackles_att_3rd             INT64,
    dribblers_tackled           INT64,
    dribbles_challenged         INT64,
    dribblers_tackled_pct       FLOAT64,
    challenges_lost             INT64,
    blocks_total                INT64,
    blocks_shots                INT64,
    blocks_passes               INT64,
    interceptions               INT64,
    tackles_plus_interceptions  INT64,
    clearances                  INT64,
    errors_leading_to_shot      INT64,
    Season              STRING
);

CREATE TABLE IF NOT EXISTS `scouting.goalkeeping` (
    Player              STRING,
    Nation              STRING,
    Position            STRING,
    Squad               STRING,
    League              STRING,
    Age                 INT64,
    Born                INT64,
    matches_played              INT64,
    starts                      INT64,
    minutes                     INT64,
    nineties_played             FLOAT64,
    goals_against               INT64,
    goals_against_per_90        FLOAT64,
    shots_on_target_against     INT64,
    saves                       INT64,
    save_pct                    FLOAT64,
    wins                        INT64,
    draws                       INT64,
    losses                      INT64,
    clean_sheets                INT64,
    clean_sheet_pct             FLOAT64,
    penalty_kicks_faced         INT64,
    penalty_kicks_allowed       INT64,
    penalty_kicks_saved         INT64,
    penalty_kicks_missed_by_kicker INT64,
    pk_save_pct                 FLOAT64,
    Season              STRING
);


-- ============================================================
-- LAYER 2: FEATURE TABLES
-- Wide joins for similarity model input.
-- Materialized as tables (not views) so the model can read
-- them fast without re-running the join every time.
-- Recreate these after any data reload.
-- ============================================================

CREATE OR REPLACE TABLE `scouting.outfield_features` AS
SELECT
    -- Identity
    s.Player,
    s.Nation,
    -- Take first listed position (e.g. "MF,FW" → "MF")
    SPLIT(s.Position, ',')[OFFSET(0)]   AS Position,
    s.Squad,
    s.League,
    s.Age,
    s.Born,
    s.Season,

    -- Pool assignment for similarity grouping
    CASE
        WHEN SPLIT(s.Position, ',')[OFFSET(0)] IN ('FW')        THEN 'ATT'
        WHEN SPLIT(s.Position, ',')[OFFSET(0)] IN ('MF')        THEN 'MID'
        WHEN SPLIT(s.Position, ',')[OFFSET(0)] IN ('DF')        THEN 'DEF'
        ELSE 'MID'   -- fallback for rare edge cases
    END AS position_group,

    -- Standard stats
    s.minutes,
    s.nineties_played,
    s.goals                             AS std_goals,
    s.assists                           AS std_assists,
    s.nonpen_goals,
    s.penalty_kicks_made,
    s.yellow_cards,
    s.red_cards,
    s.xg                                AS std_xg,
    s.npxg,
    s.xag,
    s.progressive_carries,
    s.progressive_passes                AS std_progressive_passes,
    s.progressive_passes_received,
    s.goals_per_90,
    s.assists_per_90,
    s.nonpen_goals_per_90,
    s.xg_per_90,
    s.xag_per_90,
    s.npxg_per_90,
    s.npxg_plus_xag_per_90,

    -- Shooting stats
    sh.shots_total,
    sh.shots_on_target,
    sh.shots_on_target_pct,
    sh.shots_total_per_90,
    sh.shots_on_target_per_90,
    sh.goals_per_shot,
    sh.goals_per_shot_on_target,
    sh.avg_shot_distance,
    sh.npxg_per_shot,
    sh.goals_minus_xg,
    sh.nonpen_goals_minus_npxg,

    -- Passing stats
    p.passes_completed_total,
    p.passes_attempted_total,
    p.passes_completion_pct_total,
    p.total_passing_distance,
    p.progressive_passing_distance,
    p.passes_completion_pct_short,
    p.passes_completion_pct_medium,
    p.passes_completion_pct_long,
    p.expected_assisted_goals,
    p.expected_assists,
    p.key_passes,
    p.passes_into_final_third,
    p.passes_into_penalty_area,
    p.crosses_into_penalty_area,
    p.progressive_passes                AS pass_progressive_passes,

    -- Defending stats
    d.tackles_total,
    d.tackles_won,
    d.tackles_def_3rd,
    d.tackles_mid_3rd,
    d.tackles_att_3rd,
    d.dribblers_tackled,
    d.dribblers_tackled_pct,
    d.blocks_total,
    d.interceptions,
    d.tackles_plus_interceptions,
    d.clearances,
    d.errors_leading_to_shot

FROM `scouting.standard` s
LEFT JOIN `scouting.shooting`  sh ON s.Player = sh.Player AND s.Season = sh.Season
LEFT JOIN `scouting.passing`   p  ON s.Player = p.Player  AND s.Season = p.Season
LEFT JOIN `scouting.defending` d  ON s.Player = d.Player  AND s.Season = d.Season
WHERE SPLIT(s.Position, ',')[OFFSET(0)] != 'GK'
  AND s.minutes >= 90;   -- drop players with almost no data


CREATE OR REPLACE TABLE `scouting.goalkeeper_features` AS
SELECT
    s.Player,
    s.Nation,
    s.Position,
    s.Squad,
    s.League,
    s.Age,
    s.Born,
    s.Season,
    'GK' AS position_group,

    -- Standard playing time
    s.minutes,
    s.nineties_played,

    -- GK-specific stats
    g.goals_against,
    g.goals_against_per_90,
    g.shots_on_target_against,
    g.saves,
    g.save_pct,
    g.wins,
    g.draws,
    g.losses,
    g.clean_sheets,
    g.clean_sheet_pct,
    g.penalty_kicks_faced,
    g.penalty_kicks_allowed,
    g.penalty_kicks_saved,
    g.pk_save_pct

FROM `scouting.standard` s
INNER JOIN `scouting.goalkeeping` g ON s.Player = g.Player AND s.Season = g.Season
WHERE SPLIT(s.Position, ',')[OFFSET(0)] = 'GK'
  AND s.minutes >= 90;


-- ============================================================
-- LAYER 3: PERCENTILE VIEWS
-- Computed on demand. PERCENT_RANK returns 0.0–1.0,
-- multiplied by 100 for display in the app.
-- Pool = all players in that position group across all seasons.
-- 450-minute threshold filters out bit-part players from pool.
-- ============================================================

CREATE OR REPLACE VIEW `scouting.attacker_percentiles` AS
SELECT
    Player, Squad, League, Season, minutes,
    goals_per_90,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY goals_per_90) * 100, 1)             AS goals_per_90_pct,
    assists_per_90,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY assists_per_90) * 100, 1)           AS assists_per_90_pct,
    nonpen_goals_per_90,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY nonpen_goals_per_90) * 100, 1)      AS nonpen_goals_per_90_pct,
    xg_per_90,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY xg_per_90) * 100, 1)               AS xg_per_90_pct,
    npxg_per_90,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY npxg_per_90) * 100, 1)             AS npxg_per_90_pct,
    shots_total_per_90,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY shots_total_per_90) * 100, 1)       AS shots_total_per_90_pct,
    shots_on_target_per_90,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY shots_on_target_per_90) * 100, 1)   AS shots_on_target_per_90_pct,
    goals_per_shot,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY goals_per_shot) * 100, 1)           AS goals_per_shot_pct,
    npxg_per_shot,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY npxg_per_shot) * 100, 1)            AS npxg_per_shot_pct,
    key_passes,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY key_passes) * 100, 1)              AS key_passes_pct,
    passes_into_penalty_area,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY passes_into_penalty_area) * 100, 1) AS passes_into_penalty_area_pct,
    progressive_carries,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY progressive_carries) * 100, 1)      AS progressive_carries_pct
FROM `scouting.outfield_features`
WHERE position_group = 'ATT'
  AND minutes >= 450;


CREATE OR REPLACE VIEW `scouting.midfielder_percentiles` AS
SELECT
    Player, Squad, League, Season, minutes,
    goals_per_90,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY goals_per_90) * 100, 1)             AS goals_per_90_pct,
    assists_per_90,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY assists_per_90) * 100, 1)           AS assists_per_90_pct,
    xag_per_90,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY xag_per_90) * 100, 1)              AS xag_per_90_pct,
    key_passes,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY key_passes) * 100, 1)              AS key_passes_pct,
    passes_completion_pct_total,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY passes_completion_pct_total) * 100, 1) AS passes_completion_pct_pct,
    progressive_passes                  AS pass_progressive_passes,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY progressive_passes) * 100, 1)       AS progressive_passes_pct,
    passes_into_final_third,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY passes_into_final_third) * 100, 1)  AS passes_into_final_third_pct,
    tackles_total,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY tackles_total) * 100, 1)            AS tackles_total_pct,
    interceptions,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY interceptions) * 100, 1)            AS interceptions_pct,
    tackles_plus_interceptions,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY tackles_plus_interceptions) * 100, 1) AS tackles_plus_interceptions_pct,
    progressive_carries,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY progressive_carries) * 100, 1)      AS progressive_carries_pct
FROM `scouting.outfield_features`
WHERE position_group = 'MID'
  AND minutes >= 450;


CREATE OR REPLACE VIEW `scouting.defender_percentiles` AS
SELECT
    Player, Squad, League, Season, minutes,
    tackles_total,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY tackles_total) * 100, 1)            AS tackles_total_pct,
    tackles_won,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY tackles_won) * 100, 1)             AS tackles_won_pct,
    dribblers_tackled_pct,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY dribblers_tackled_pct) * 100, 1)    AS dribblers_tackled_pct_pct,
    interceptions,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY interceptions) * 100, 1)            AS interceptions_pct,
    tackles_plus_interceptions,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY tackles_plus_interceptions) * 100, 1) AS tackles_plus_interceptions_pct,
    blocks_total,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY blocks_total) * 100, 1)             AS blocks_total_pct,
    clearances,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY clearances) * 100, 1)              AS clearances_pct,
    errors_leading_to_shot,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY errors_leading_to_shot) * 100, 1)   AS errors_leading_to_shot_pct,
    passes_completion_pct_total,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY passes_completion_pct_total) * 100, 1) AS passes_completion_pct_pct,
    progressive_passing_distance,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY progressive_passing_distance) * 100, 1) AS progressive_passing_distance_pct
FROM `scouting.outfield_features`
WHERE position_group = 'DEF'
  AND minutes >= 450;


CREATE OR REPLACE VIEW `scouting.goalkeeper_percentiles` AS
SELECT
    Player, Squad, League, Season, minutes,
    save_pct,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY save_pct) * 100, 1)                 AS save_pct_pct,
    goals_against_per_90,
    -- Lower is better for GA/90, so reverse the rank
    ROUND((1 - PERCENT_RANK() OVER (PARTITION BY Season ORDER BY goals_against_per_90)) * 100, 1) AS goals_against_per_90_pct,
    clean_sheet_pct,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY clean_sheet_pct) * 100, 1)          AS clean_sheet_pct_pct,
    clean_sheets,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY clean_sheets) * 100, 1)             AS clean_sheets_pct,
    pk_save_pct,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY pk_save_pct) * 100, 1)              AS pk_save_pct_pct,
    shots_on_target_against,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY shots_on_target_against) * 100, 1)  AS shots_on_target_against_pct,
    saves,
    ROUND(PERCENT_RANK() OVER (PARTITION BY Season ORDER BY saves) * 100, 1)                   AS saves_pct
FROM `scouting.goalkeeper_features`
WHERE minutes >= 450;
