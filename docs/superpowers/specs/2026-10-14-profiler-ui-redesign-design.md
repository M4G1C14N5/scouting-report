# Football Player Profiler: UI redesign

Date: 2026-10-14. Approved by Ajaiah. Delivery: straight to `main`.

## Goal

Make the Profiler look and work like a modern, dark, soccer-themed scouting app.
It should feel like a sibling of the Basketball Intelligence Platform (same
structure and card style) without copying its colours. It must also fix the
functional bugs that sit under the current UI. The data source and SQL stay
exactly as they are today (BigQuery `scouting-466001.scouting`, FBref-era
tables). A later project will swap in API-Football data, so per-position metric
lists must live in one config module.

## Non-goals

- No change to the BigQuery tables, `schema.sql`, `gcp_loader.py` or the cleaning pipeline.
- No change to the similarity algorithm in `src/similarity.py` beyond caching wrappers.
- No new pages beyond what is listed here (no league explorer, no player search across seasons).

## Bugs fixed as part of this work

1. The app crashes (`IndexError`) when the filters leave no players (`app.py` ~L345).
2. The season picker uses stateless `st.button`s, so any rerun snaps back to the oldest season.
3. The selectbox has no `index=None`, so the alphabetically first player is auto-selected.
4. Percentiles are not inverted for lower-is-better stats (cards, errors, goals against, losses, pens allowed, and so on).
5. The percentile denominator counts NaN rows, which pushes percentiles down.
6. Ordinal suffix is always "th" ("1th", "22th").
7. 4-5 uncached BigQuery queries run on every rerun. The position pool is fetched twice.
8. The CSS paints every `st.alert` pink, so error, warning and info look the same.
9. The deprecated `use_container_width` is still used.

## Visual system: "Floodlight"

A night-match look: deep pitch-black greens, chalk-white type, and a floodlight-lime
accent. Values were checked with the dataviz palette validator against the
dark surface.

| Token | Value | Use |
|---|---|---|
| `PAGE` | `#0a110d` | app background (with a very faint mowed-stripe gradient) |
| `SURFACE` | `#111a15` | cards, chart surface, sidebar |
| `SURFACE_2` | `#17231c` | hover, table header |
| `BORDER` | `#213028` | card and table borders |
| `INK` | `#ecf2ed` | primary text |
| `INK_2` | `#b3c1b8` | secondary text |
| `MUTED` | `#7f8f86` | labels, captions |
| `GRID` | `rgba(236,242,237,0.07)` | chart gridlines |
| `ACCENT` | `#b8f05a` | brand accent: primaryColor, h4 accent bar, active states, hero highlight |
| `PLAYER` | `#5aa82c` | selected player series in charts |
| `COMPARE` | `#7c7cf2` | comparison player series in charts |
| `PCT_RAMP` | `#3d5a26`, `#5c8a2e`, `#84c234`, `#b8f05a` | percentile bands: <25, 25-49, 50-74, 75+ (passes the ordinal check) |
| `RADIUS` | `2px` | all corners, matching the basketball app |

Validator results: `PLAYER`/`COMPARE` pass every check in dark mode (CVD ΔE 28.6,
normal ΔE 33.0, tritan 6.4, so the pair is always direct-labelled). `PCT_RAMP` passes
the ordinal checks (monotone lightness, single hue).

Rules:
- Percentile bars are coloured by `PCT_RAMP` band. The band is always also written
  as text ("Elite", "Above avg", "Below avg", "Poor") with the percentile value, so
  colour is never the only cue. There are no red/amber/green traffic-light grades;
  status colours stay reserved for alerts.
- Text never wears a series colour. Values and labels use the ink tokens.
- Fonts: Inter for body text. Headings, the hero and KPI values use a condensed sports
  face (Barlow Condensed via Google Fonts) to give the app its own soccer-kit voice
  and set it apart from the basketball app.
- `.streamlit/config.toml` mirrors the tokens (`base="dark"`, `primaryColor`,
  backgrounds, text, `baseRadius="2px"`, `dataframeHeaderBackgroundColor`,
  `dataframeBorderColor`). The `Dockerfile` must `COPY .streamlit/ ./.streamlit/`.

Cards use the basketball pattern (`lib/theme.py` `kpi()`/`card()`), with an
uppercase muted label, a large tabular-number value and a muted sub-line, restyled
with the tokens above. They are ported, not imported (separate repo). Avoid fragile
`data-testid` selectors. Where a bordered Streamlit container needs styling, use
`st.container(border=True, key="card_*")` and target `[class*="st-key-card"]`.

## Structure

```
app.py                  thin shell: page config, theme injection, sidebar, routing
src/ui/theme.py         tokens, CSS injection, kpi()/card()/chip()/hero() HTML helpers
src/ui/viz.py           Plotly layout + percentile_bars(), compare_dumbbell(), pizza (optional)
src/ui/metrics.py       per-position metric lists, labels, categories, lower-is-better set,
                        number formats (THE place to edit when the data source changes)
src/ui/data.py          cached wrappers around the existing queries (same SQL)
src/ui/stats.py         pure functions: percentile_of(), band(), ordinal(), formatting
src/ui/views/profile.py, stats_tab.py, similar.py
tests/                  pytest for stats.py + metrics consistency + an AppTest smoke test
```

`STAT_LABELS` and `STAT_CATEGORIES` move from `app.py` into `src/ui/metrics.py`
unchanged, as the starting point.

## Page, top to bottom

1. **Hero.** "Football Player Profiler", with the subtitle "Big 5 leagues · 2017-18 to
   2023-24 · 16.5k player-seasons" and a meta line ("FBref data, frozen snapshot ·
   BigQuery"). Page icon ⚽.
2. **Sidebar.** Brand block, then uppercase section labels:
   - Season: a pills/segmented control, "All" plus each season.
   - League: a selectbox.
   - Club: a selectbox, filtered by league.
   - A caption: "Showing N players".
3. **Player search.** One selectbox over (Player, Season) rows, `index=None`,
   `format_func` = "Name — Club, League". Until a player is chosen, show an onboarding
   empty state (3 short step cards, plus a "Try an example" button that picks a known
   player), then `st.stop()`.
4. **Season switcher.** If the player has more than one season within the filters,
   show `st.segmented_control` keyed in session state, defaulting to the latest season.
5. **Identity card.** Initials avatar, name, and chips: club, league, season, position
   in full (ATT → Forward, MID → Midfielder, DEF → Defender, GK → Goalkeeper), age,
   nation. Then a KPI row:
   - Outfield: Minutes (thousands separator), 90s, Goals, Assists, xG, xAG.
   - Goalkeeper: Minutes, 90s, Save %, Goals against / 90, Clean sheet %, Saves / 90
     (whichever exist in the GK table).
6. **Tabs.**
   - **Profile.** Percentile bars by category for the position's curated metrics (from
     `metrics.py`, seeded from `FEATURE_TEMPLATES` plus the SQL percentile views).
     Each bar shows the band colour, the raw value and the percentile label. A caption
     states the pool: "vs N {position}s in {season}, 450+ minutes". If the player is
     outside the pool, show a clear empty state.
   - **Stats.** Category cards (Shooting, Passing, Defending, Progression, Discipline,
     Playing time, Goalkeeping), stats ordered by meaning, with separate Total and
     Per-90 columns, formatted via `column_config.NumberColumn` (`%d`, `%.2f`,
     `%.1f%%`).
   - **Similar players.**
     - The top-N slider moves into this tab.
     - Table with readable headers (Player, Club, League, Season, Position, Minutes) and
       similarity as a `ProgressColumn` from 0 to 1.
     - Single-row selection shows a `compare_dumbbell` of the selected player against
       the comparison player on the curated percentiles, using `PLAYER` vs `COMPARE`
       with direct labels, plus a "View profile" button that switches to that player.
     - One line explains the method: cosine similarity on z-scored stats within the
       position pool.
7. **Footer.** Data source (FBref snapshot, 2017-18 to 2023-24), pipeline (FBref → GCS →
   BigQuery), and contributors: Thomas Tsangou and Ajaiah Darlington.

## Data and caching

- `src/ui/data.py` wraps `load_player_index`, `get_player_stats`, the position-pool
  query and `find_similar_players` in `@st.cache_data(ttl=3600, show_spinner=...)`,
  with the BigQuery client passed as `_bq`. Keys: (player, season, position_group).
- The position pool is fetched once and shared by the Profile and Similar tabs.
- The SQL text stays identical to today's. The remaining f-string queries keep their
  current form, since inputs come from controlled sets; parameterising them is optional.

## Charts (Plotly)

- Add `plotly` to `requirements.txt`.
- Transparent background, Inter font, small margins, modebar hidden, `hovertemplate`
  with `<extra></extra>`, gridlines in `GRID`, `fixedrange` so charts don't zoom.
- `percentile_bars`: horizontal 0-100 bars with a reference line at 50, ~38px per row,
  and a direct label "value · 87th".
- `compare_dumbbell`: one row per metric, two dots joined by a line, a legend plus
  direct labels on the first row.

## Errors and empty states

- Short, readable `st.error` messages, with the exception in an expander.
- Native alert colours are kept.
- Explicit empty states for: no players match the filters, no player selected, player
  below 450 minutes (Profile/Similar), and a BigQuery failure.

## Testing and verification

- `pytest` for:
  - `percentile_of` (higher-is-better / lower-is-better, NaN excluded, ties);
  - `band()`;
  - `ordinal()` (1st, 2nd, 3rd, 11th, 12th, 13th, 21st, 22nd, 111th);
  - formatting;
  - `metrics.py` consistency: every curated metric exists in the feature-table columns
    in `schema.sql`, and every lower-is-better key exists.
- An `streamlit.testing.v1.AppTest` smoke test with the data layer stubbed: the app
  renders with no selection, with a selected outfield player and with a goalkeeper,
  without exceptions.
- A manual visual check: run locally against real BigQuery (gcloud ADC works on this
  machine) and take Playwright screenshots of the empty state and each tab, for an
  outfield player and a goalkeeper, at 1200x750 @2x. Review them before pushing.

## Delivery

Commit to `main` of `M4G1C14N5/scouting-report` and push, as the user chose. The live
site (fae.camuedlabs.org, Coolify) is expected to redeploy from `main`. The README is
updated: the app description, screenshots, and removal of the "raw or per-90 toggle"
claim unless it is built.
