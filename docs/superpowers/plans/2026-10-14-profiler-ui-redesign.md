# Football Player Profiler UI Redesign: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the Streamlit Profiler (`app.py`) as a dark "Floodlight" soccer scouting app, a sibling of the Basketball Intelligence Platform, and fix the nine functional bugs listed in the spec. The BigQuery data and SQL stay exactly as they are.

**Architecture:** `app.py` becomes a thin shell. The UI moves into `src/ui/`. Pure helpers are unit-tested: `stats.py` (percentiles, bands, formatting), `filters.py` (player index filtering) and `build.py` (display rows). `metrics.py` holds all metric config: labels, categories, per-position lists and the lower-is-better set. `data.py` wraps today's queries in `st.cache_data`. `theme.py` and `viz.py` are restyled ports of the basketball helpers. `views/` holds one module per screen section. Tests stub `src.ui.data`, so nothing hits BigQuery.

**Tech Stack:** Python 3.14 locally (3.11 in Docker), Streamlit 1.56, Plotly, pandas, google-cloud-bigquery, pytest with `streamlit.testing.v1.AppTest`, and Playwright (system `C:\Python314\python`) for screenshots.

**Spec:** `docs/superpowers/specs/2026-10-14-profiler-ui-redesign-design.md`

## Global Constraints

- Repo: `C:\Users\haiaj\OneDrive\Desktop\Coding Projects\Portfolio\Sports\Scouting\og_scouting\scouting-report`. Run every command from this folder in PowerShell. Commit straight to `main` (the user approved this). **Never push** unless the user says so in this session.
- Every commit message ends with a blank line and then `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Use `git commit -m "<subject>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.
- No changes to the BigQuery tables, `schema.sql`, `gcp_loader.py`, `run_cleaning.py`, `src/fbref_utils.py` or the notebooks.
- `src/similarity.py` stays unchanged: the algorithm, its SQL and `FEATURE_TEMPLATES`. The UI only wraps it in caching.
- SQL text stays identical to today's `app.py` (compared with whitespace normalised). The two queries are `load_player_index` and the position-pool query from the old Percentile tab. Today's `load_percentiles` is never called, so it is dropped rather than ported.
- Floodlight tokens, verbatim: `PAGE #0a110d`, `SURFACE #111a15`, `SURFACE_2 #17231c`, `BORDER #213028`, `INK #ecf2ed`, `INK_2 #b3c1b8`, `MUTED #7f8f86`, `GRID rgba(236,242,237,0.07)`, `ACCENT #b8f05a`, `PLAYER #5aa82c`, `COMPARE #7c7cf2`, `PCT_RAMP #3d5a26, #5c8a2e, #84c234, #b8f05a` (bands <25, 25-49, 50-74, 75+), `RADIUS 2px`. Do not reuse basketball's blue.
- Fonts: Inter for body text. Barlow Condensed (Google Fonts) for headings, the hero and KPI values.
- Percentile bands always appear as text too ("Elite", "Above avg", "Below avg", "Poor"), next to the percentile. No red/amber/green grades. Text never uses the `PLAYER`/`COMPARE` series colours. Native `st.error`/`st.warning`/`st.info` colours are kept: no CSS touches alerts.
- No `data-testid` selectors in CSS. Bordered containers are styled with `st.container(border=True, key="card_*")` plus `.stApp [class*="st-key-card"]`. Equal-height KPI tiles use a CSS grid in one HTML block, not the basketball `stColumn` hack.
- No `use_container_width`. Use `width="stretch"`.
- Code must run on Docker `python:3.11-slim` as well as local 3.14. Start every module with `from __future__ import annotations` and avoid 3.12+ syntax (no `type` statements, no PEP 695 generics).
- `requirements.txt` gains `plotly>=6.0` and pins `streamlit>=1.56`. pytest goes in a new `requirements-dev.txt`. The `Dockerfile` must `COPY .streamlit/ ./.streamlit/`.
- The footer names the contributors: Thomas Tsangou and Ajaiah Darlington.
- Tests never touch BigQuery. Monkeypatch the `src.ui.data` functions with the fakes in `tests/fakes.py`. App and view code must call `data.<fn>(...)` through the module, never `from src.ui.data import <fn>`, so the monkeypatch reaches it.
- Every helper that returns HTML returns a single line. A blank or indented line inside raw HTML makes `st.markdown` fall back to Markdown.
- The pool threshold is `src.similarity.MIN_MINUTES` (450). Import it; don't hard-code it in UI code.

## Verified Streamlit 1.56 facts (checked against the installed package)

- `st.segmented_control` and `st.pills` exist. Both take `required: bool` and `format_func`. AppTest exposes both as `at.button_group(key=...)`.
- `st.dataframe(..., on_select="rerun", selection_mode="single-row", key=...)` returns a state object, read with `event.selection.rows`. Setting `st.session_state[key] = {"selection": {"rows": [0], "columns": []}}` selects a row programmatically. This was verified in AppTest, and it is how the tests drive the dumbbell.
- `height` accepts `"content"`. `row_height` exists.
- `st.column_config.ProgressColumn(label, *, width, help, pinned, format, min_value, max_value, step, color)`. `color` accepts a hex string.
- `NumberColumn.format` accepts printf strings or `"localized"`/`"plain"`/etc.
- `st.container(border=..., key=...)`: the key becomes the CSS class `st-key-<key>`.
- `st.tabs` and `st.selectbox(index=None, placeholder=..., format_func=...)` exist.
- `AppTest.from_function(fn, *, default_timeout, args, kwargs)`, `AppTest.from_file(path, *, default_timeout)`, `at.get("plotly_chart")`. Widgets can be looked up by key (`at.selectbox(key="...")`).
- `st.cache_data` works outside a runtime (bare mode), and `.clear()` resets it. `_`-prefixed args are not hashed.
- Theme options that exist: `base`, `primaryColor`, `backgroundColor`, `secondaryBackgroundColor`, `textColor`, `borderColor`, `linkColor`, `font`, `headingFont`, `fontFaces`, `baseRadius`, `buttonRadius`, `showSidebarBorder`, `dataframeHeaderBackgroundColor`, `dataframeBorderColor`, `[theme.sidebar] backgroundColor/secondaryBackgroundColor`.
- `font`/`headingFont` take `"<Family>:<url>"`, split on the first colon. The URL may hold only one `family=` or Streamlit raises. There is no room for a fallback list after the URL.
- plotly 7.1.0 and pytest 9.1.1 are on PyPI for Python 3.14. Neither is installed in `.venv` yet.

## File Structure

| Path | Status | Responsibility |
|---|---|---|
| `requirements.txt` | modify | add `plotly>=6.0`, pin `streamlit>=1.56` |
| `requirements-dev.txt` | create | `-r requirements.txt` + `pytest>=8.3` |
| `pytest.ini` | create | `testpaths = tests`, `pythonpath = .` |
| `.gitignore` | modify | ignore `.pytest_cache/` and editor `*.py.tmp.*` files |
| `.streamlit/config.toml` | create | Floodlight theme for native widgets |
| `Dockerfile` | modify | `COPY .streamlit/ ./.streamlit/` |
| `src/similarity.py.tmp.*` (2 files) | delete | editor junk |
| `src/ui/__init__.py` | create | package marker |
| `src/ui/stats.py` | create | pure numeric/text helpers |
| `src/ui/filters.py` | create | pure player-index filtering for the sidebar/search |
| `src/ui/metrics.py` | create | ALL metric config (the file to edit when the data source changes) |
| `src/ui/build.py` | create | pure display-row builders driven by `metrics.py` |
| `src/ui/data.py` | create | cached BigQuery wrappers, same SQL |
| `src/ui/theme.py` | create | tokens, CSS, HTML helpers |
| `src/ui/viz.py` | create | Plotly layout, `percentile_bars`, `compare_dumbbell` |
| `src/ui/state.py` | create | session-state keys + `goto()` callback |
| `src/ui/views/__init__.py` | create | package marker |
| `src/ui/views/header.py` | create | identity card + KPI row |
| `src/ui/views/profile.py` | create | Profile tab |
| `src/ui/views/stats_tab.py` | create | Stats tab |
| `src/ui/views/similar.py` | create | Similar players tab |
| `src/ui/views/picker.py` | create | sidebar filters, search, onboarding, season switcher |
| `app.py` | rewrite | thin shell |
| `tests/__init__.py`, `tests/schema_utils.py`, `tests/fakes.py` | create | test helpers (schema.sql parser, fake data) |
| `tests/test_*.py` | create | one file per module + AppTest smoke tests |
| `scripts/screenshot.py` | create | Playwright review screenshots |
| `docs/screenshots/*.png` | create | review images (4 committed) |
| `README.md` | modify | app description, screenshots, tests, drop the per-90 toggle claim |

---

### Task 1: Tooling, theme config, Docker and test scaffolding

**Files:**
- Modify: `requirements.txt`, `.gitignore`, `Dockerfile`
- Create: `requirements-dev.txt`, `pytest.ini`, `.streamlit/config.toml`, `src/ui/__init__.py`, `src/ui/views/__init__.py`, `tests/__init__.py`, `tests/schema_utils.py`
- Delete: `src/similarity.py.tmp.18048.4b0b2291fdd8`, `src/similarity.py.tmp.18048.ef927e30cd26`
- Test: `tests/test_config.py`, `tests/test_schema_utils.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `tests.schema_utils.IDENTITY_COLUMNS: frozenset[str]`: `{"Player","Nation","Position","Squad","League","Age","Born","Season","position_group"}`.
  - `tests.schema_utils.feature_columns(table: str) -> list[str]`: column names of `outfield_features` (68) or `goalkeeper_features` (25), parsed from `schema.sql`, in order.
  - `tests.schema_utils.numeric_columns(table: str) -> list[str]`: `feature_columns` minus identity columns.
  - Packages `src.ui`, `src.ui.views`, `tests`. pytest runs with the repo root on `sys.path`.

- [ ] **Step 1: Install the new packages into the venv**

Run: `.venv\Scripts\python -m pip install "plotly>=6.0" "pytest>=8.3"`
Expected: `Successfully installed ... plotly-7.x ... pytest-9.x`. Don't `pip install -r requirements.txt` here: `python-Levenshtein` may need a compiler on 3.14, and everything else is already installed.

- [ ] **Step 2: Create the pytest config and package markers**

`pytest.ini`:

```ini
[pytest]
testpaths = tests
pythonpath = .
```

`tests/__init__.py`:

```python
```

(empty file)

`src/ui/__init__.py`:

```python
"""Streamlit UI for the Football Player Profiler (theme, charts, metric config, cached queries, views)."""
```

`src/ui/views/__init__.py`:

```python
"""One module per screen section: header, profile, stats_tab, similar, picker."""
```

- [ ] **Step 3: Write the failing tests**

`tests/test_schema_utils.py`:

```python
from __future__ import annotations

from tests.schema_utils import IDENTITY_COLUMNS, feature_columns, numeric_columns


def test_outfield_columns_are_parsed_in_order():
    cols = feature_columns("outfield_features")
    assert len(cols) == 68
    assert cols[:9] == ["Player", "Nation", "Position", "Squad", "League", "Age", "Born",
                        "Season", "position_group"]
    assert "pass_progressive_passes" in cols  # an `AS` alias
    assert cols[-1] == "errors_leading_to_shot"


def test_goalkeeper_columns_are_parsed_in_order():
    cols = feature_columns("goalkeeper_features")
    assert len(cols) == 25
    assert "position_group" in cols  # 'GK' AS position_group
    assert cols[-1] == "pk_save_pct"


def test_numeric_columns_drop_identity():
    out = numeric_columns("outfield_features")
    gk = numeric_columns("goalkeeper_features")
    assert not set(out) & IDENTITY_COLUMNS
    assert len(out) == 59
    assert len(gk) == 16
    assert {"minutes", "nineties_played"} <= set(gk)
```

`tests/test_config.py`:

```python
"""Theme config, Docker image, requirements and repo hygiene."""

from __future__ import annotations

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _config() -> dict:
    return tomllib.loads((ROOT / ".streamlit" / "config.toml").read_text(encoding="utf-8"))


def test_theme_mirrors_floodlight_tokens():
    cfg = _config()
    theme = cfg["theme"]
    assert theme["base"] == "dark"
    assert theme["primaryColor"] == "#b8f05a"
    assert theme["backgroundColor"] == "#0a110d"
    assert theme["secondaryBackgroundColor"] == "#17231c"
    assert theme["textColor"] == "#ecf2ed"
    assert theme["borderColor"] == "#213028"
    assert theme["baseRadius"] == "2px"
    assert theme["buttonRadius"] == "2px"
    assert theme["dataframeHeaderBackgroundColor"] == "#17231c"
    assert theme["dataframeBorderColor"] == "#213028"
    assert theme["sidebar"]["backgroundColor"] == "#111a15"


def test_theme_fonts_are_inter_and_barlow_condensed():
    theme = _config()["theme"]
    for key, family in (("font", "Inter"), ("headingFont", "Barlow Condensed")):
        name, url = theme[key].split(":", 1)
        assert name == family
        assert url.startswith("https://fonts.googleapis.com/css2?family=")
        assert url.count("family=") == 1  # Streamlit rejects multi-font URLs


def test_dockerfile_ships_the_streamlit_config():
    assert "COPY .streamlit/ ./.streamlit/" in (ROOT / "Dockerfile").read_text(encoding="utf-8")


def test_requirements_add_plotly_and_dev_pytest():
    reqs = (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
    dev = (ROOT / "requirements-dev.txt").read_text(encoding="utf-8").splitlines()
    assert "plotly>=6.0" in reqs
    assert "streamlit>=1.56" in reqs
    assert "streamlit" not in reqs  # the unpinned line is replaced
    assert dev[0] == "-r requirements.txt"
    assert "pytest>=8.3" in dev


def test_editor_temp_files_are_gone():
    assert not list((ROOT / "src").glob("similarity.py.tmp.*"))
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `.venv\Scripts\python -m pytest tests/test_schema_utils.py tests/test_config.py -v`
Expected: FAIL. You should see `ModuleNotFoundError: No module named 'tests.schema_utils'` and `FileNotFoundError` for `.streamlit/config.toml`.

- [ ] **Step 5: Write `tests/schema_utils.py`**

```python
"""Parse the feature-table column lists out of schema.sql.

Used by the metrics consistency tests and by tests/fakes.py, so the fakes
always carry exactly the columns BigQuery returns.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

SCHEMA = Path(__file__).resolve().parents[1] / "schema.sql"

IDENTITY_COLUMNS = frozenset(
    {"Player", "Nation", "Position", "Squad", "League", "Age", "Born", "Season", "position_group"}
)


@lru_cache(maxsize=None)
def feature_columns(table: str) -> list[str]:
    """Column names of `scouting.<table>` (a CREATE OR REPLACE TABLE ... AS SELECT)."""
    text = SCHEMA.read_text(encoding="utf-8")
    match = re.search(
        r"CREATE OR REPLACE TABLE `scouting\." + re.escape(table) + r"` AS\s+SELECT(.*?)\nFROM ",
        text,
        re.S,
    )
    if not match:
        raise ValueError(f"{table} not found in {SCHEMA}")
    body = "\n".join(line.split("--", 1)[0] for line in match.group(1).splitlines())

    # Split on top-level commas only: SPLIT(s.Position, ',') has commas inside parens.
    items: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            items.append("".join(current))
            current = []
        else:
            current.append(ch)
    items.append("".join(current))

    columns: list[str] = []
    for item in items:
        item = item.strip()
        if not item:
            continue
        alias = re.search(r"\bAS\s+(\w+)\s*$", item, re.I)
        columns.append(alias.group(1) if alias else re.search(r"(\w+)\s*$", item).group(1))
    return columns


def numeric_columns(table: str) -> list[str]:
    return [c for c in feature_columns(table) if c not in IDENTITY_COLUMNS]
```

- [ ] **Step 6: Write the config, requirements and Docker changes**

`.streamlit/config.toml`:

```toml
# Floodlight theme. Mirrors the tokens in src/ui/theme.py - keep the two in
# sync (tests/test_theme.py checks it).
[theme]
base = "dark"
primaryColor = "#b8f05a"              # ACCENT
backgroundColor = "#0a110d"           # PAGE
secondaryBackgroundColor = "#17231c"  # SURFACE_2: inputs and widgets
textColor = "#ecf2ed"                 # INK
borderColor = "#213028"               # BORDER: bordered containers, widgets
linkColor = "#b8f05a"
# "<Family>:<Google Fonts URL>" - one family per URL, no fallback list allowed.
font = "Inter:https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"
headingFont = "Barlow Condensed:https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700;800&display=swap"
# Sharp corners on native widgets, matching theme.RADIUS on custom cards.
baseRadius = "2px"
buttonRadius = "2px"
showSidebarBorder = true
# The data grid draws into a <canvas>; these are the only hooks that reach it.
dataframeHeaderBackgroundColor = "#17231c"  # SURFACE_2
dataframeBorderColor = "#213028"            # BORDER

[theme.sidebar]
backgroundColor = "#111a15"           # SURFACE
secondaryBackgroundColor = "#17231c"  # SURFACE_2

[browser]
gatherUsageStats = false
```

`requirements.txt` (full new content):

```
requests
beautifulsoup4
pandas
fuzzywuzzy
python-Levenshtein
openpyxl
lxml
numpy
google-cloud-bigquery
google-cloud-storage
pyarrow
db-dtypes
streamlit>=1.56
plotly>=6.0
```

`requirements-dev.txt`:

```
-r requirements.txt
pytest>=8.3
```

`Dockerfile` (full new content):

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY .streamlit/ ./.streamlit/
COPY app.py .
COPY src/ ./src/

# Disable the Streamlit welcome page and run in server mode
ENV STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

EXPOSE 8501

CMD ["streamlit", "run", "app.py"]
```

Append to `.gitignore`:

```
# Tests
.pytest_cache/

# Editor temp files
*.py.tmp.*
```

- [ ] **Step 7: Remove the stray editor files from git**

Run: `git rm src/similarity.py.tmp.18048.4b0b2291fdd8 src/similarity.py.tmp.18048.ef927e30cd26`
Expected: two `rm 'src/similarity.py.tmp...'` lines.

- [ ] **Step 8: Run the tests to verify they pass**

Run: `.venv\Scripts\python -m pytest tests/test_schema_utils.py tests/test_config.py -v`
Expected: 8 passed.

- [ ] **Step 9: Check that Streamlit parses the theme**

Run: `.venv\Scripts\python -m streamlit config show | Select-String -Pattern 'primaryColor = |baseRadius = |headingFont = |dataframeBorderColor = '`
Expected: lines showing `primaryColor = "#b8f05a"`, `baseRadius = "2px"`, `headingFont = "Barlow Condensed:https://..."` and `dataframeBorderColor = "#213028"`.

- [ ] **Step 10: Commit**

```powershell
git add pytest.ini requirements.txt requirements-dev.txt .gitignore Dockerfile .streamlit/config.toml src/ui/__init__.py src/ui/views/__init__.py tests/__init__.py tests/schema_utils.py tests/test_schema_utils.py tests/test_config.py
git commit -m "ui: add Floodlight theme config, plotly, pytest scaffolding; drop editor temp files" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Pure helpers: `stats.py` and `filters.py` (TDD)

**Files:**
- Create: `src/ui/stats.py`, `src/ui/filters.py`
- Test: `tests/test_stats.py`, `tests/test_filters.py`

**Interfaces:**
- Consumes: nothing (pure pandas).
- Produces (`src.ui.stats`):
  - `MISSING = "—"`, `BAND_LABELS = ("Poor", "Below avg", "Above avg", "Elite")`
  - `class Band(NamedTuple): index: int; label: str`. `index` is 0-3 and indexes `theme.PCT_RAMP`.
  - `is_missing(value) -> bool`, `to_float(value) -> float | None`
  - `percentile_of(series, value, higher_is_better: bool = True) -> float | None`: mid-rank 0-100, NaN excluded.
  - `band(pct: float) -> Band`, `ordinal(n: int) -> str`, `pct_label(pct: float) -> str` (clamped "1st".."99th")
  - `fmt_number(value, fmt: str) -> str`, where `fmt` is one of `"int"`, `"dec1"`, `"dec2"`, `"pct"`.
  - `per90(total, nineties) -> float | None`, `short_season(season: str) -> str`, `initials(name: str) -> str`, `slug(text: str) -> str`
- Produces (`src.ui.filters`):
  - `seasons_desc(index) -> list[str]`, `leagues(index) -> list[str]`, `clubs(index, league: str | None) -> list[str]`
  - `filter_index(index, season=None, league=None, club=None) -> pd.DataFrame` (`None` = no filter)
  - `search_options(filtered) -> pd.DataFrame`: one row per player (their latest-season row), sorted case-insensitively by name.
  - `option_label(player: str, club: str, league: str) -> str` → `"Name — Club, League"`
  - `player_seasons(filtered, player: str) -> list[str]` (ascending)
  - `pick_example(filtered, preferred: Iterable[str]) -> str | None`

- [ ] **Step 1: Write the failing tests**

`tests/test_stats.py`:

```python
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.ui import stats


class TestPercentileOf:
    def test_higher_is_better_uses_mid_rank(self):
        assert stats.percentile_of(pd.Series([1, 2, 3, 4]), 4) == 87.5

    def test_lower_is_better_is_the_exact_mirror(self):
        assert stats.percentile_of(pd.Series([1, 2, 3, 4]), 4, higher_is_better=False) == 12.5
        assert stats.percentile_of(pd.Series([1, 2, 3, 4]), 1, higher_is_better=False) == 87.5

    def test_nan_rows_are_left_out_of_the_denominator(self):
        assert stats.percentile_of(pd.Series([1, 2, np.nan, 3, 4]), 4) == 87.5

    def test_ties_count_half(self):
        s = pd.Series([1, 2, 2, 3])
        assert stats.percentile_of(s, 2) == 50.0
        assert stats.percentile_of(s, 2, higher_is_better=False) == 50.0

    def test_all_tied_sits_in_the_middle(self):
        # most players have 0 red cards: 0 must not rank near the bottom
        assert stats.percentile_of(pd.Series([0, 0, 0, 0]), 0, higher_is_better=False) == 50.0

    def test_values_outside_the_pool(self):
        s = pd.Series([1, 2, 3, 4])
        assert stats.percentile_of(s, 10) == 100.0
        assert stats.percentile_of(s, 0) == 0.0

    @pytest.mark.parametrize("value", [None, np.nan, pd.NA])
    def test_missing_value_returns_none(self, value):
        assert stats.percentile_of(pd.Series([1, 2, 3]), value) is None

    def test_empty_or_all_nan_pool_returns_none(self):
        assert stats.percentile_of(pd.Series([], dtype=float), 1) is None
        assert stats.percentile_of(pd.Series([np.nan, np.nan]), 1) is None

    def test_nullable_int_column(self):
        s = pd.Series([1, 2, None, 4], dtype="Int64")
        assert stats.percentile_of(s, 4) == pytest.approx(250 / 3)

    def test_accepts_a_plain_list(self):
        assert stats.percentile_of([1, 2, 3, 4], 4) == 87.5


@pytest.mark.parametrize(
    "pct, index, label",
    [
        (0, 0, "Poor"), (24.9, 0, "Poor"),
        (25, 1, "Below avg"), (49.9, 1, "Below avg"),
        (50, 2, "Above avg"), (74.9, 2, "Above avg"),
        (75, 3, "Elite"), (100, 3, "Elite"),
    ],
)
def test_band(pct, index, label):
    assert stats.band(pct) == stats.Band(index, label)


@pytest.mark.parametrize(
    "n, text",
    [
        (1, "1st"), (2, "2nd"), (3, "3rd"), (4, "4th"), (11, "11th"), (12, "12th"),
        (13, "13th"), (21, "21st"), (22, "22nd"), (23, "23rd"), (101, "101st"),
        (111, "111th"), (112, "112th"), (0, "0th"),
    ],
)
def test_ordinal(n, text):
    assert stats.ordinal(n) == text


@pytest.mark.parametrize(
    "pct, text",
    [(87.4, "87th"), (87.5, "88th"), (21.5, "22nd"), (50, "50th"),
     (99.8, "99th"), (100, "99th"), (0.2, "1st"), (0, "1st")],
)
def test_pct_label_rounds_half_up_and_clamps(pct, text):
    assert stats.pct_label(pct) == text


class TestFmtNumber:
    def test_int_has_thousands_separator(self):
        assert stats.fmt_number(2536, "int") == "2,536"
        assert stats.fmt_number(2536.6, "int") == "2,537"

    def test_decimals(self):
        assert stats.fmt_number(19.24, "dec1") == "19.2"
        assert stats.fmt_number(1234.56, "dec1") == "1,234.6"
        assert stats.fmt_number(0.678, "dec2") == "0.68"

    def test_pct(self):
        assert stats.fmt_number(41.27, "pct") == "41.3%"

    @pytest.mark.parametrize("value", [None, np.nan, pd.NA])
    def test_missing_is_an_em_dash(self, value):
        assert stats.fmt_number(value, "int") == stats.MISSING == "—"

    def test_unknown_format_raises(self):
        with pytest.raises(ValueError):
            stats.fmt_number(1, "money")


@pytest.mark.parametrize(
    "total, nineties, expected",
    [(10, 20, 0.5), (10, 0, None), (None, 20, None), (10, None, None), (np.nan, 20, None)],
)
def test_per90(total, nineties, expected):
    assert stats.per90(total, nineties) == expected


@pytest.mark.parametrize(
    "season, short",
    [("2022-2023", "2022-23"), ("2019-2020", "2019-20"), ("2023", "2023"), ("All", "All")],
)
def test_short_season(season, short):
    assert stats.short_season(season) == short


@pytest.mark.parametrize(
    "name, text",
    [("Mohamed Salah", "MS"), ("Virgil van Dijk", "VD"), ("Alisson", "A"),
     ("N'Golo Kanté", "NK"), ("   ", "?")],
)
def test_initials(name, text):
    assert stats.initials(name) == text


@pytest.mark.parametrize("text, expected", [("Playing time", "playing_time"), ("Shooting", "shooting"),
                                            ("Passing & Assisting", "passing_assisting")])
def test_slug(text, expected):
    assert stats.slug(text) == expected


def test_is_missing_and_to_float():
    assert stats.is_missing(None) and stats.is_missing(np.nan) and stats.is_missing(pd.NA)
    assert not stats.is_missing(0) and not stats.is_missing("x") and not stats.is_missing([1, 2])
    assert stats.to_float("3.5") == 3.5
    assert stats.to_float("abc") is None
    assert stats.to_float(np.int64(4)) == 4.0
```

`tests/test_filters.py`:

```python
from __future__ import annotations

import pandas as pd

from src.ui import filters

COLUMNS = ["Player", "Season", "Squad", "League", "Position", "position_group", "minutes"]
INDEX = pd.DataFrame(
    [
        ("Mohamed Salah", "2023-2024", "Liverpool", "Premier League", "FW", "ATT", 2536),
        ("Mohamed Salah", "2022-2023", "Liverpool", "Premier League", "FW", "ATT", 3290),
        ("Harry Kane", "2023-2024", "Bayern Munich", "Bundesliga", "FW", "ATT", 2617),
        ("Harry Kane", "2022-2023", "Tottenham", "Premier League", "FW", "ATT", 3406),
        ("Alisson", "2023-2024", "Liverpool", "Premier League", "GK", "GK", 2520),
        ("ederson", "2023-2024", "Manchester City", "Premier League", "GK", "GK", 2970),
    ],
    columns=COLUMNS,
)


def test_seasons_desc_leagues_clubs():
    assert filters.seasons_desc(INDEX) == ["2023-2024", "2022-2023"]
    assert filters.leagues(INDEX) == ["Bundesliga", "Premier League"]
    assert filters.clubs(INDEX, None) == ["Bayern Munich", "Liverpool", "Manchester City", "Tottenham"]
    assert filters.clubs(INDEX, "Bundesliga") == ["Bayern Munich"]


def test_filter_index_combines_filters():
    assert len(filters.filter_index(INDEX)) == 6
    assert len(filters.filter_index(INDEX, season="2023-2024")) == 4
    out = filters.filter_index(INDEX, season="2022-2023", league="Premier League", club="Tottenham")
    assert out["Player"].tolist() == ["Harry Kane"]
    assert filters.filter_index(INDEX, season="2022-2023", league="Bundesliga").empty
    assert list(out.index) == [0]  # index reset


def test_search_options_one_row_per_player_latest_season_sorted_casefold():
    opts = filters.search_options(INDEX)
    assert opts["Player"].tolist() == ["Alisson", "ederson", "Harry Kane", "Mohamed Salah"]
    kane = opts[opts["Player"] == "Harry Kane"].iloc[0]
    assert kane["Season"] == "2023-2024" and kane["Squad"] == "Bayern Munich"


def test_search_options_of_empty_frame_is_empty():
    assert filters.search_options(INDEX.iloc[0:0]).empty


def test_option_label():
    assert filters.option_label("Mohamed Salah", "Liverpool", "Premier League") == \
        "Mohamed Salah — Liverpool, Premier League"


def test_player_seasons_ascending():
    assert filters.player_seasons(INDEX, "Mohamed Salah") == ["2022-2023", "2023-2024"]
    assert filters.player_seasons(INDEX, "Nobody") == []


def test_pick_example_prefers_listed_names_then_most_minutes():
    assert filters.pick_example(INDEX, ("Kevin De Bruyne", "Alisson")) == "Alisson"
    assert filters.pick_example(INDEX, ("Kevin De Bruyne",)) == "Harry Kane"  # 3406 minutes
    assert filters.pick_example(INDEX.iloc[0:0], ("Alisson",)) is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv\Scripts\python -m pytest tests/test_stats.py tests/test_filters.py -v`
Expected: FAIL with `ImportError: cannot import name 'stats' from 'src.ui'` (and the same for `filters`).

- [ ] **Step 3: Write `src/ui/stats.py`**

```python
"""Pure helpers: percentiles, percentile bands, ordinals and number formatting.

No Streamlit and no config imports, so everything here is unit-tested
directly (tests/test_stats.py). Fixes three old bugs: percentiles counted
NaN rows in the denominator, lower-is-better stats were never inverted, and
the suffix was always "th".
"""

from __future__ import annotations

import math
import re
from typing import Any, NamedTuple

import pandas as pd

BAND_LABELS = ("Poor", "Below avg", "Above avg", "Elite")
BAND_EDGES = (25.0, 50.0, 75.0)  # <25, 25-49, 50-74, 75+
MISSING = "—"
_SEASON = re.compile(r"^(\d{4})-(\d{4})$")


class Band(NamedTuple):
    """A percentile band. `index` (0-3) picks the colour from theme.PCT_RAMP."""

    index: int
    label: str


def is_missing(value: Any) -> bool:
    """True for None, NaN, pd.NA and NaT. Never raises."""
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):  # list-likes are not a single missing value
        return False


def to_float(value: Any) -> float | None:
    """`value` as a float, or None when it is missing or not numeric."""
    if is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def percentile_of(series: Any, value: Any, higher_is_better: bool = True) -> float | None:
    """Mid-rank percentile (0-100) of `value` within `series`.

    NaN rows are dropped before counting. Ties count half, which makes the
    two directions exact mirrors (lower = 100 - higher): a player with zero
    red cards lands mid-pool instead of near the bottom just because most
    of the pool also has zero.
    """
    v = to_float(value)
    if v is None:
        return None
    clean = pd.to_numeric(pd.Series(series), errors="coerce").dropna().astype(float)
    if clean.empty:
        return None
    below = int((clean < v).sum())
    equal = int((clean == v).sum())
    pct = (below + 0.5 * equal) / len(clean) * 100.0
    return pct if higher_is_better else 100.0 - pct


def band(pct: float) -> Band:
    for i, edge in enumerate(BAND_EDGES):
        if pct < edge:
            return Band(i, BAND_LABELS[i])
    return Band(len(BAND_EDGES), BAND_LABELS[-1])


def ordinal(n: int) -> str:
    """1 -> "1st", 2 -> "2nd", 11 -> "11th", 22 -> "22nd", 111 -> "111th"."""
    n = int(n)
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def pct_label(pct: float) -> str:
    """Percentile as an ordinal, rounded half-up and clamped to 1st-99th."""
    return ordinal(min(99, max(1, math.floor(pct + 0.5))))


def fmt_number(value: Any, fmt: str) -> str:
    """Format for display. fmt: "int" (1,234), "dec1" (12.3), "dec2" (0.45), "pct" (41.3%)."""
    v = to_float(value)
    if v is None:
        return MISSING
    if fmt == "int":
        return f"{v:,.0f}"
    if fmt == "dec1":
        return f"{v:,.1f}"
    if fmt == "dec2":
        return f"{v:,.2f}"
    if fmt == "pct":
        return f"{v:.1f}%"
    raise ValueError(f"unknown number format: {fmt!r}")


def per90(total: Any, nineties: Any) -> float | None:
    t, n = to_float(total), to_float(nineties)
    if t is None or n is None or n <= 0:
        return None
    return t / n


def short_season(season: str) -> str:
    """"2022-2023" -> "2022-23"; anything else is returned unchanged."""
    m = _SEASON.match(str(season))
    return f"{m.group(1)}-{m.group(2)[2:]}" if m else str(season)


def initials(name: str) -> str:
    words = str(name).split()
    if not words:
        return "?"
    if len(words) == 1:
        return words[0][0].upper()
    return (words[0][0] + words[-1][0]).upper()


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(text).lower()).strip("_")
```

- [ ] **Step 4: Write `src/ui/filters.py`**

```python
"""Pure filtering of the player index for the sidebar and the search box.

The index has one row per player-season with the columns
Player, Season, Squad, League, Position, position_group, minutes.
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd


def seasons_desc(index: pd.DataFrame) -> list[str]:
    return sorted(index["Season"].dropna().unique().tolist(), reverse=True)


def leagues(index: pd.DataFrame) -> list[str]:
    return sorted(index["League"].dropna().unique().tolist())


def clubs(index: pd.DataFrame, league: str | None) -> list[str]:
    rows = index if league is None else index[index["League"] == league]
    return sorted(rows["Squad"].dropna().unique().tolist())


def filter_index(index: pd.DataFrame, season: str | None = None, league: str | None = None,
                 club: str | None = None) -> pd.DataFrame:
    """Rows matching every filter that is not None."""
    mask = pd.Series(True, index=index.index)
    if season is not None:
        mask &= index["Season"] == season
    if league is not None:
        mask &= index["League"] == league
    if club is not None:
        mask &= index["Squad"] == club
    return index[mask].reset_index(drop=True)


def search_options(filtered: pd.DataFrame) -> pd.DataFrame:
    """One row per player: their latest season in view (most minutes on a tie)."""
    if filtered.empty:
        return filtered.copy()
    latest = (filtered.sort_values(["Season", "minutes"], ascending=[False, False])
              .drop_duplicates("Player", keep="first"))
    return latest.sort_values("Player", key=lambda s: s.str.casefold()).reset_index(drop=True)


def option_label(player: str, club: str, league: str) -> str:
    return f"{player} — {club}, {league}"


def player_seasons(filtered: pd.DataFrame, player: str) -> list[str]:
    return sorted(filtered.loc[filtered["Player"] == player, "Season"].unique().tolist())


def pick_example(filtered: pd.DataFrame, preferred: Iterable[str]) -> str | None:
    """First preferred name that is in view, else the player with the most minutes."""
    if filtered.empty:
        return None
    names = set(filtered["Player"])
    for name in preferred:
        if name in names:
            return name
    return str(filtered.sort_values("minutes", ascending=False).iloc[0]["Player"])
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv\Scripts\python -m pytest tests/test_stats.py tests/test_filters.py -v`
Expected: all passed (about 75 including parametrised cases), 0 failed.

- [ ] **Step 6: Commit**

```powershell
git add src/ui/stats.py src/ui/filters.py tests/test_stats.py tests/test_filters.py
git commit -m "ui: pure percentile, band, ordinal, formatting and index-filter helpers" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Metric config `metrics.py` and display builders `build.py`

`STAT_LABELS` and `STAT_CATEGORIES` move here from `app.py`. All 73 keys are kept and every label is unchanged except one: `std_progressive_passes` becomes "Progressive Passes (standard data)", because both it and `pass_progressive_passes` were labelled "Progressive Passes" and would show as two identical rows in one card. Category names are normalised to the spec's seven (Shooting, Passing, Defending, Progression, Discipline, Playing time, Goalkeeping). Within each category, keys are listed in order of meaning, and that order is the display order. `dribblers_tackled*` moves from "Dribbling & Possession" to Defending, where those FBref stats belong.

Curated per-position lists for the Profile tab: each is the union of `FEATURE_TEMPLATES[group]` (src/similarity.py) and the stats in that position's SQL percentile view (schema.sql L328-437), grouped by category:

| Group | Category: metrics | Count |
|---|---|---|
| ATT | Shooting: goals_per_90, nonpen_goals_per_90, xg_per_90, npxg_per_90, npxg_plus_xag_per_90, shots_total_per_90, shots_on_target_per_90, goals_per_shot, npxg_per_shot · Passing: assists_per_90, xag_per_90, key_passes, passes_into_penalty_area, crosses_into_penalty_area, passes_completion_pct_total · Progression: progressive_carries, progressive_passing_distance, passes_into_final_third | 18 |
| MID | Progression: progressive_carries, progressive_passing_distance, pass_progressive_passes, passes_into_final_third · Passing: assists_per_90, xag_per_90, key_passes, passes_into_penalty_area, crosses_into_penalty_area, passes_completion_pct_total · Shooting: goals_per_90, npxg_per_90, shots_total_per_90 · Defending: tackles_total, interceptions, tackles_plus_interceptions | 16 |
| DEF | Defending: tackles_total, tackles_won, tackles_def_3rd, tackles_mid_3rd, dribblers_tackled_pct, interceptions, tackles_plus_interceptions, blocks_total, clearances, errors_leading_to_shot · Passing: passes_completion_pct_total, assists_per_90 · Progression: progressive_passing_distance, passes_into_final_third · Shooting: goals_per_90 | 15 |
| GK | Goalkeeping: save_pct, goals_against_per_90, clean_sheet_pct, clean_sheets, pk_save_pct, saves, shots_on_target_against | 7 |

Lower-is-better keys (real column names): `errors_leading_to_shot`, `yellow_cards`, `red_cards`, `goals_against`, `goals_against_per_90`, `losses`, `penalty_kicks_allowed`. `shots_on_target_against` and `saves` stay higher-is-better. They measure workload, and the SQL `goalkeeper_percentiles` view ranks them ascending too. Only `goals_against_per_90` is inverted there, and it is in our set.

**Files:**
- Create: `src/ui/metrics.py`, `src/ui/build.py`
- Test: `tests/test_metrics.py`, `tests/test_build.py`

**Interfaces:**
- Consumes: `src.ui.stats` (Task 2): `to_float`, `percentile_of`, `fmt_number`, `per90`. Also `tests.schema_utils.numeric_columns` (Task 1) and `src.similarity.FEATURE_TEMPLATES`.
- Produces (`src.ui.metrics`):
  - `POSITION_NAMES: dict[str, str]` (ATT→Forward, MID→Midfielder, DEF→Defender, GK→Goalkeeper) and `POSITION_PLURALS` (forwards, midfielders, defenders, goalkeepers).
  - `CATEGORY_ORDER: tuple[str, ...]`, `STAT_LABELS: dict[str, str]`, `STAT_CATEGORIES: dict[str, str]`, `KIND: dict[str, str]`. Kinds: count, tally, total, per90, pct, ratio, distance, minutes, nineties.
  - `KIND_FORMAT: dict[str, str]` (kind → stats fmt), `TOTAL_DECIMALS: dict[str, int]`, `PER90_COMPUTED_KINDS: frozenset`, `PER90_PAIRS: dict[str, str]` (total → stored per-90 column).
  - `PROFILE_METRICS: dict[str, dict[str, tuple[str, ...]]]`, `LOWER_IS_BETTER: frozenset[str]`
  - `class KpiSpec(NamedTuple): label: str; key: str; per90: bool = False` and `KPI_TILES: dict[str, tuple[KpiSpec, ...]]` (keys `"outfield"`, `"GK"`).
  - `GLOSSARY: dict[str, str]`
  - `label(key) -> str`, `fmt_of(key) -> str`, `keys_in_category(category) -> list[str]`, `stats_categories(group) -> list[str]`, `profile_keys(group) -> list[str]`
- Produces (`src.ui.build`):
  - `class PctRow(NamedTuple): key: str; label: str; pct: float; value: float`
  - `STAT_TABLE_COLUMNS = ("Stat", "Total", "Per 90")`, `SIMILAR_COLUMNS = ("Player", "Club", "League", "Season", "Position", "Minutes", "Similarity")`
  - `fmt_value(key: str, value) -> str`
  - `profile_rows(pool: pd.DataFrame, player_row: Mapping, group: str) -> dict[str, list[PctRow]]`
  - `compare_lists(a_rows, b_rows) -> tuple[list[str], list[float], list[float]]`
  - `stat_table(row: Mapping, category: str) -> pd.DataFrame` with columns `STAT_TABLE_COLUMNS`, float64 `Total`/`Per 90`.
  - `kpi_tiles(row: Mapping, group: str) -> list[tuple[str, str, str]]`, giving (label, value text, help text).
  - `similar_table(similar: pd.DataFrame) -> pd.DataFrame` with columns `SIMILAR_COLUMNS`.

- [ ] **Step 1: Write the failing tests**

`tests/test_metrics.py`:

```python
"""metrics.py must agree with schema.sql: the guard for the future data-source swap."""

from __future__ import annotations

from src.similarity import FEATURE_TEMPLATES
from src.ui import metrics
from tests.schema_utils import numeric_columns

OUTFIELD = set(numeric_columns("outfield_features"))
GOALKEEPER = set(numeric_columns("goalkeeper_features"))


def _columns_for(group: str) -> set[str]:
    return GOALKEEPER if group == "GK" else OUTFIELD


def test_every_feature_column_has_a_label_category_and_kind():
    every = OUTFIELD | GOALKEEPER
    assert set(metrics.STAT_LABELS) == every
    assert set(metrics.STAT_CATEGORIES) == every
    assert set(metrics.KIND) == every


def test_categories_are_the_spec_seven_in_display_order():
    assert metrics.CATEGORY_ORDER == ("Shooting", "Passing", "Defending", "Progression",
                                      "Discipline", "Playing time", "Goalkeeping")
    assert set(metrics.STAT_CATEGORIES.values()) == set(metrics.CATEGORY_ORDER)


def test_kinds_are_known():
    assert set(metrics.KIND.values()) <= set(metrics.KIND_FORMAT)


def test_profile_metrics_exist_in_the_right_feature_table():
    for group, categories in metrics.PROFILE_METRICS.items():
        for category, keys in categories.items():
            missing = [k for k in keys if k not in _columns_for(group)]
            assert not missing, f"{group}/{category}: not in schema.sql: {missing}"


def test_profile_metrics_cover_the_similarity_templates():
    for group, template in FEATURE_TEMPLATES.items():
        assert set(template) <= set(metrics.profile_keys(group)), group


def test_profile_metric_counts_and_no_duplicates():
    counts = {g: len(metrics.profile_keys(g)) for g in metrics.PROFILE_METRICS}
    assert counts == {"ATT": 18, "MID": 16, "DEF": 15, "GK": 7}
    for group in metrics.PROFILE_METRICS:
        keys = metrics.profile_keys(group)
        assert len(keys) == len(set(keys)), group


def test_profile_categories_are_known_categories():
    for categories in metrics.PROFILE_METRICS.values():
        assert set(categories) <= set(metrics.CATEGORY_ORDER)


def test_lower_is_better_keys_are_real_columns():
    assert metrics.LOWER_IS_BETTER <= OUTFIELD | GOALKEEPER
    assert metrics.LOWER_IS_BETTER == {
        "errors_leading_to_shot", "yellow_cards", "red_cards", "goals_against",
        "goals_against_per_90", "losses", "penalty_kicks_allowed",
    }


def test_per90_pairs_point_at_per90_columns_in_the_same_category():
    for total, per90 in metrics.PER90_PAIRS.items():
        assert metrics.KIND[per90] == "per90"
        assert metrics.KIND[total] != "per90"
        assert metrics.STAT_CATEGORIES[total] == metrics.STAT_CATEGORIES[per90]


def test_kpi_and_glossary_keys_are_real_columns():
    for group, specs in metrics.KPI_TILES.items():
        cols = GOALKEEPER if group == "GK" else OUTFIELD
        assert [s.key for s in specs if s.key not in cols] == [], group
    assert set(metrics.GLOSSARY) <= set(metrics.STAT_LABELS)


def test_duplicate_label_is_resolved():
    labels = list(metrics.STAT_LABELS.values())
    assert len(labels) == len(set(labels))


def test_helpers():
    assert metrics.label("std_goals") == "Goals"
    assert metrics.label("unknown_col") == "unknown_col"
    assert metrics.fmt_of("minutes") == "int"
    assert metrics.fmt_of("std_xg") == "dec1"
    assert metrics.fmt_of("save_pct") == "pct"
    assert metrics.keys_in_category("Discipline") == ["yellow_cards", "red_cards"]
    assert metrics.stats_categories("GK") == ["Goalkeeping", "Playing time"]
    assert metrics.stats_categories("ATT") == ["Shooting", "Passing", "Defending", "Progression",
                                               "Discipline", "Playing time"]
```

`tests/test_build.py`:

```python
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.ui import build


def test_fmt_value_uses_the_metric_kind():
    assert build.fmt_value("minutes", 2536) == "2,536"
    assert build.fmt_value("std_xg", 19.24) == "19.2"
    assert build.fmt_value("goals_per_90", 0.678) == "0.68"
    assert build.fmt_value("save_pct", 71.24) == "71.2%"
    assert build.fmt_value("std_goals", None) == "—"


def _def_pool() -> pd.DataFrame:
    return pd.DataFrame({
        "Player": ["A", "B", "C", "D"],
        "Season": ["2023-2024"] * 4,
        "tackles_total": [10, 20, 30, 40],
        "errors_leading_to_shot": [0, 1, 2, 3],
        "tackles_won": [5, 6, np.nan, 8],
        "goals_per_90": [0.1, 0.2, 0.3, 0.4],
    })


def test_profile_rows_inverts_lower_is_better_stats():
    pool = _def_pool()
    rows = build.profile_rows(pool, pool.iloc[3], "DEF")
    defending = {r.key: r for r in rows["Defending"]}
    assert defending["tackles_total"].pct == 87.5
    assert defending["errors_leading_to_shot"].pct == 12.5  # most errors = worst
    assert defending["tackles_total"].label == "Tackles"
    assert defending["tackles_total"].value == 40.0


def test_profile_rows_skip_missing_values_columns_and_empty_categories():
    pool = _def_pool()
    rows = build.profile_rows(pool, pool.iloc[2], "DEF")  # C has NaN tackles_won
    keys = [r.key for r in rows["Defending"]]
    assert "tackles_won" not in keys
    assert "Passing" not in rows  # no passing columns in the pool
    assert [r.key for r in rows["Shooting"]] == ["goals_per_90"]


def test_profile_rows_follow_metric_order():
    pool = _def_pool()
    keys = [r.key for r in build.profile_rows(pool, pool.iloc[0], "DEF")["Defending"]]
    assert keys == ["tackles_total", "tackles_won", "errors_leading_to_shot"]


def test_profile_rows_unknown_group_is_empty():
    assert build.profile_rows(_def_pool(), _def_pool().iloc[0], "XYZ") == {}


def test_compare_lists_keeps_shared_metrics_in_a_order():
    a = {"Shooting": [build.PctRow("g", "Goals", 80.0, 1.0), build.PctRow("x", "xG", 60.0, 1.0)]}
    b = {"Shooting": [build.PctRow("x", "xG", 20.0, 1.0)]}
    assert build.compare_lists(a, b) == (["xG"], [60.0], [20.0])


def test_stat_table_pairs_totals_with_stored_and_computed_per90():
    row = pd.Series({
        "nineties_played": 20.0, "std_goals": 10, "goals_per_90": 0.5, "std_xg": 8.04,
        "xg_per_90": 0.4, "shots_total": 50, "shots_on_target_pct": 41.27,
        "npxg_plus_xag_per_90": 0.71, "penalty_kicks_made": 3,
    })
    table = build.stat_table(row, "Shooting")
    assert tuple(table.columns) == build.STAT_TABLE_COLUMNS
    by = table.set_index("Stat")
    assert by.loc["Goals", "Total"] == 10 and by.loc["Goals", "Per 90"] == 0.5
    assert by.loc["xG", "Total"] == 8.0 and by.loc["xG", "Per 90"] == 0.4
    assert by.loc["Shots", "Per 90"] == 2.5          # computed 50 / 20
    assert by.loc["Shot Accuracy %", "Total"] == 41.3
    assert pd.isna(by.loc["Shot Accuracy %", "Per 90"])  # rates have no per 90
    assert pd.isna(by.loc["Penalties Made", "Per 90"])   # tallies have no per 90
    assert pd.isna(by.loc["npxG + xAG / 90", "Total"])
    assert by.loc["npxG + xAG / 90", "Per 90"] == 0.71
    assert "Goals / 90" not in by.index  # merged into the Goals row
    assert table["Total"].dtype == "float64" and table["Per 90"].dtype == "float64"
    assert list(table["Stat"])[:2] == ["Goals", "Penalties Made"]  # metric order, missing rows dropped


def test_stat_table_for_a_category_the_row_lacks_is_empty():
    gk_row = pd.Series({"nineties_played": 30.0, "wins": 10, "saves": 90})
    empty = build.stat_table(gk_row, "Shooting")
    assert empty.empty and tuple(empty.columns) == build.STAT_TABLE_COLUMNS
    gk = build.stat_table(gk_row, "Goalkeeping").set_index("Stat")
    assert gk.loc["Saves", "Per 90"] == 3.0
    assert pd.isna(gk.loc["Wins", "Per 90"])


def test_kpi_tiles_outfield_and_goalkeeper():
    out = build.kpi_tiles(pd.Series({"minutes": 2536, "nineties_played": 28.18, "std_goals": 18,
                                     "std_assists": 10, "std_xg": 19.24, "xag": 9.95}), "ATT")
    assert [(lbl, txt) for lbl, txt, _ in out] == [
        ("Minutes", "2,536"), ("90s", "28.2"), ("Goals", "18"), ("Assists", "10"),
        ("xG", "19.2"), ("xAG", "9.9")]
    assert out[4][2].startswith("Expected goals")
    gk = build.kpi_tiles(pd.Series({"minutes": 2700, "nineties_played": 30.0, "save_pct": 71.24,
                                    "goals_against_per_90": 1.0333, "clean_sheet_pct": 33.3,
                                    "saves": 90}), "GK")
    assert [lbl for lbl, _, _ in gk] == ["Minutes", "90s", "Save %", "Goals against / 90",
                                         "Clean sheet %", "Saves / 90"]
    assert gk[2][1] == "71.2%" and gk[3][1] == "1.03" and gk[5][1] == "3.00"
    assert gk[5][2] == "Saves divided by 90s played."


def test_kpi_tiles_missing_values_show_a_dash():
    tiles = build.kpi_tiles(pd.Series({"minutes": 100}), "ATT")
    assert tiles[2][1] == "—"


def test_similar_table_renames_for_display():
    sim = pd.DataFrame({
        "Player": ["B"], "Squad": ["Club B"], "League": ["Serie A"], "Season": ["2023-2024"],
        "position_group": ["ATT"], "minutes": [2000], "similarity_score": [0.91234],
    })
    out = build.similar_table(sim)
    assert tuple(out.columns) == build.SIMILAR_COLUMNS
    assert out.iloc[0].tolist() == ["B", "Club B", "Serie A", "2023-2024", "Forward", 2000, 0.912]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv\Scripts\python -m pytest tests/test_metrics.py tests/test_build.py -v`
Expected: FAIL with `ImportError: cannot import name 'metrics' from 'src.ui'`.

- [ ] **Step 3: Write `src/ui/metrics.py`**

```python
"""Metric configuration: labels, categories, number formats, per-position
profile lists and the lower-is-better set.

THE place to edit when the data source changes (the planned API-Football
swap). Every key is a column of outfield_features / goalkeeper_features in
schema.sql; tests/test_metrics.py fails if one is not.

STAT_LABELS / STAT_CATEGORIES started as the dicts in the old app.py. All
73 keys and their labels are kept, except one: std_progressive_passes is
relabelled, because it shared "Progressive Passes" with
pass_progressive_passes. Categories were normalised to the seven shown in
the Stats tab.
"""

from __future__ import annotations

from typing import NamedTuple

POSITION_NAMES = {"ATT": "Forward", "MID": "Midfielder", "DEF": "Defender", "GK": "Goalkeeper"}
POSITION_PLURALS = {"ATT": "forwards", "MID": "midfielders", "DEF": "defenders", "GK": "goalkeepers"}

CATEGORY_ORDER = ("Shooting", "Passing", "Defending", "Progression", "Discipline",
                  "Playing time", "Goalkeeping")

# (column, label, category, kind) in display order. Kinds:
#   count    integer season total, per 90 computed when no stored per-90 column
#   tally    integer season total with no meaningful per 90 (wins, penalties)
#   total    decimal season total (xG family), per 90 computed
#   per90    stored per-90 rate (shown in the Per 90 column)
#   pct      percentage, a season rate
#   ratio    other season rate (per shot)
#   distance average distance (yards)
#   minutes / nineties  playing time
_METRICS: tuple[tuple[str, str, str, str], ...] = (
    # Shooting
    ("std_goals", "Goals", "Shooting", "count"),
    ("nonpen_goals", "Non-Pen Goals", "Shooting", "count"),
    ("penalty_kicks_made", "Penalties Made", "Shooting", "tally"),
    ("std_xg", "xG", "Shooting", "total"),
    ("npxg", "npxG", "Shooting", "total"),
    ("goals_minus_xg", "Goals - xG", "Shooting", "total"),
    ("nonpen_goals_minus_npxg", "Non-Pen Goals - npxG", "Shooting", "total"),
    ("shots_total", "Shots", "Shooting", "count"),
    ("shots_on_target", "Shots on Target", "Shooting", "count"),
    ("shots_on_target_pct", "Shot Accuracy %", "Shooting", "pct"),
    ("goals_per_shot", "Goals per Shot", "Shooting", "ratio"),
    ("goals_per_shot_on_target", "Goals per SoT", "Shooting", "ratio"),
    ("npxg_per_shot", "npxG per Shot", "Shooting", "ratio"),
    ("avg_shot_distance", "Avg Shot Distance", "Shooting", "distance"),
    ("goals_per_90", "Goals / 90", "Shooting", "per90"),
    ("nonpen_goals_per_90", "Non-Pen Goals / 90", "Shooting", "per90"),
    ("xg_per_90", "xG / 90", "Shooting", "per90"),
    ("npxg_per_90", "npxG / 90", "Shooting", "per90"),
    ("shots_total_per_90", "Shots / 90", "Shooting", "per90"),
    ("shots_on_target_per_90", "Shots on Target / 90", "Shooting", "per90"),
    ("npxg_plus_xag_per_90", "npxG + xAG / 90", "Shooting", "per90"),
    # Passing
    ("std_assists", "Assists", "Passing", "count"),
    ("xag", "xAG", "Passing", "total"),
    ("expected_assisted_goals", "Expected Assisted Goals", "Passing", "total"),
    ("expected_assists", "Expected Assists", "Passing", "total"),
    ("key_passes", "Key Passes", "Passing", "count"),
    ("passes_into_final_third", "Passes into Final Third", "Passing", "count"),
    ("passes_into_penalty_area", "Passes into Box", "Passing", "count"),
    ("crosses_into_penalty_area", "Crosses into Box", "Passing", "count"),
    ("passes_completed_total", "Passes Completed", "Passing", "count"),
    ("passes_attempted_total", "Passes Attempted", "Passing", "count"),
    ("passes_completion_pct_total", "Pass Completion %", "Passing", "pct"),
    ("passes_completion_pct_short", "Short Pass Completion %", "Passing", "pct"),
    ("passes_completion_pct_medium", "Medium Pass Completion %", "Passing", "pct"),
    ("passes_completion_pct_long", "Long Pass Completion %", "Passing", "pct"),
    ("total_passing_distance", "Total Pass Distance", "Passing", "count"),
    ("assists_per_90", "Assists / 90", "Passing", "per90"),
    ("xag_per_90", "xAG / 90", "Passing", "per90"),
    # Defending
    ("tackles_total", "Tackles", "Defending", "count"),
    ("tackles_won", "Tackles Won", "Defending", "count"),
    ("tackles_def_3rd", "Tackles (Def 3rd)", "Defending", "count"),
    ("tackles_mid_3rd", "Tackles (Mid 3rd)", "Defending", "count"),
    ("tackles_att_3rd", "Tackles (Att 3rd)", "Defending", "count"),
    ("dribblers_tackled", "Dribblers Tackled", "Defending", "count"),
    ("dribblers_tackled_pct", "Dribbler Tackle %", "Defending", "pct"),
    ("interceptions", "Interceptions", "Defending", "count"),
    ("tackles_plus_interceptions", "Tackles + Interceptions", "Defending", "count"),
    ("blocks_total", "Blocks", "Defending", "count"),
    ("clearances", "Clearances", "Defending", "count"),
    ("errors_leading_to_shot", "Errors Leading to Shot", "Defending", "count"),
    # Progression
    ("progressive_carries", "Progressive Carries", "Progression", "count"),
    ("std_progressive_passes", "Progressive Passes (standard data)", "Progression", "count"),
    ("pass_progressive_passes", "Progressive Passes", "Progression", "count"),
    ("progressive_passing_distance", "Progressive Pass Distance", "Progression", "count"),
    ("progressive_passes_received", "Progressive Passes Received", "Progression", "count"),
    # Discipline
    ("yellow_cards", "Yellow Cards", "Discipline", "count"),
    ("red_cards", "Red Cards", "Discipline", "count"),
    # Playing time
    ("minutes", "Minutes", "Playing time", "minutes"),
    ("nineties_played", "90s", "Playing time", "nineties"),
    # Goalkeeping
    ("goals_against", "Goals Against", "Goalkeeping", "count"),
    ("goals_against_per_90", "Goals Against / 90", "Goalkeeping", "per90"),
    ("shots_on_target_against", "Shots on Target Against", "Goalkeeping", "count"),
    ("saves", "Saves", "Goalkeeping", "count"),
    ("save_pct", "Save %", "Goalkeeping", "pct"),
    ("clean_sheets", "Clean Sheets", "Goalkeeping", "tally"),
    ("clean_sheet_pct", "Clean Sheet %", "Goalkeeping", "pct"),
    ("wins", "Wins", "Goalkeeping", "tally"),
    ("draws", "Draws", "Goalkeeping", "tally"),
    ("losses", "Losses", "Goalkeeping", "tally"),
    ("penalty_kicks_faced", "Penalties Faced", "Goalkeeping", "tally"),
    ("penalty_kicks_allowed", "Penalties Allowed", "Goalkeeping", "tally"),
    ("penalty_kicks_saved", "Penalties Saved", "Goalkeeping", "tally"),
    ("pk_save_pct", "Penalty Save %", "Goalkeeping", "pct"),
)

STAT_LABELS: dict[str, str] = {key: label for key, label, _, _ in _METRICS}
STAT_CATEGORIES: dict[str, str] = {key: category for key, _, category, _ in _METRICS}
KIND: dict[str, str] = {key: kind for key, _, _, kind in _METRICS}

KIND_FORMAT = {
    "count": "int", "tally": "int", "minutes": "int",
    "total": "dec1", "nineties": "dec1", "distance": "dec1",
    "per90": "dec2", "ratio": "dec2",
    "pct": "pct",
}
TOTAL_DECIMALS = {"int": 0, "dec1": 1, "dec2": 2, "pct": 1}
PER90_COMPUTED_KINDS = frozenset({"count", "total"})

# Season total -> the stored per-90 column shown beside it in the Stats tab.
PER90_PAIRS = {
    "std_goals": "goals_per_90",
    "nonpen_goals": "nonpen_goals_per_90",
    "std_xg": "xg_per_90",
    "npxg": "npxg_per_90",
    "shots_total": "shots_total_per_90",
    "shots_on_target": "shots_on_target_per_90",
    "std_assists": "assists_per_90",
    "xag": "xag_per_90",
    "goals_against": "goals_against_per_90",
}

# Profile tab: FEATURE_TEMPLATES (src/similarity.py) plus the SQL percentile
# views (schema.sql), grouped by category. Category order = card order.
PROFILE_METRICS: dict[str, dict[str, tuple[str, ...]]] = {
    "ATT": {
        "Shooting": ("goals_per_90", "nonpen_goals_per_90", "xg_per_90", "npxg_per_90",
                     "npxg_plus_xag_per_90", "shots_total_per_90", "shots_on_target_per_90",
                     "goals_per_shot", "npxg_per_shot"),
        "Passing": ("assists_per_90", "xag_per_90", "key_passes", "passes_into_penalty_area",
                    "crosses_into_penalty_area", "passes_completion_pct_total"),
        "Progression": ("progressive_carries", "progressive_passing_distance",
                        "passes_into_final_third"),
    },
    "MID": {
        "Progression": ("progressive_carries", "progressive_passing_distance",
                        "pass_progressive_passes", "passes_into_final_third"),
        "Passing": ("assists_per_90", "xag_per_90", "key_passes", "passes_into_penalty_area",
                    "crosses_into_penalty_area", "passes_completion_pct_total"),
        "Shooting": ("goals_per_90", "npxg_per_90", "shots_total_per_90"),
        "Defending": ("tackles_total", "interceptions", "tackles_plus_interceptions"),
    },
    "DEF": {
        "Defending": ("tackles_total", "tackles_won", "tackles_def_3rd", "tackles_mid_3rd",
                      "dribblers_tackled_pct", "interceptions", "tackles_plus_interceptions",
                      "blocks_total", "clearances", "errors_leading_to_shot"),
        "Passing": ("passes_completion_pct_total", "assists_per_90"),
        "Progression": ("progressive_passing_distance", "passes_into_final_third"),
        "Shooting": ("goals_per_90",),
    },
    "GK": {
        "Goalkeeping": ("save_pct", "goals_against_per_90", "clean_sheet_pct", "clean_sheets",
                        "pk_save_pct", "saves", "shots_on_target_against"),
    },
}

# Percentiles for these are inverted, so a longer bar always means better.
LOWER_IS_BETTER = frozenset({
    "errors_leading_to_shot", "yellow_cards", "red_cards",
    "goals_against", "goals_against_per_90", "losses", "penalty_kicks_allowed",
})


class KpiSpec(NamedTuple):
    label: str
    key: str
    per90: bool = False  # True: show key / nineties_played


KPI_TILES: dict[str, tuple[KpiSpec, ...]] = {
    "outfield": (
        KpiSpec("Minutes", "minutes"), KpiSpec("90s", "nineties_played"),
        KpiSpec("Goals", "std_goals"), KpiSpec("Assists", "std_assists"),
        KpiSpec("xG", "std_xg"), KpiSpec("xAG", "xag"),
    ),
    "GK": (
        KpiSpec("Minutes", "minutes"), KpiSpec("90s", "nineties_played"),
        KpiSpec("Save %", "save_pct"), KpiSpec("Goals against / 90", "goals_against_per_90"),
        KpiSpec("Clean sheet %", "clean_sheet_pct"), KpiSpec("Saves / 90", "saves", per90=True),
    ),
}

# Plain-English help for jargon (the basketball glossary.py pattern), shown as
# a hover/tap tip on KPI labels.
GLOSSARY = {
    "nineties_played": "Minutes played divided by 90: how many full matches his minutes add up to.",
    "std_xg": ("Expected goals: the summed chance quality of every shot he took. "
               "Goals above xG suggests strong finishing, or luck."),
    "xag": "Expected assisted goals: the xG of the shots his passes set up, scored or not.",
    "save_pct": "Share of shots on target faced that he saved.",
    "goals_against_per_90": "Goals conceded per 90 minutes he played. Lower is better.",
    "clean_sheet_pct": "Share of his matches in which his team kept a clean sheet.",
}


def label(key: str) -> str:
    return STAT_LABELS.get(key, key)


def fmt_of(key: str) -> str:
    return KIND_FORMAT[KIND.get(key, "ratio")]


def keys_in_category(category: str) -> list[str]:
    return [key for key, cat in STAT_CATEGORIES.items() if cat == category]


def stats_categories(group: str) -> list[str]:
    """Stats-tab cards for a position group, in display order."""
    if group == "GK":
        return ["Goalkeeping", "Playing time"]
    return [c for c in CATEGORY_ORDER if c != "Goalkeeping"]


def profile_keys(group: str) -> list[str]:
    return [key for keys in PROFILE_METRICS.get(group, {}).values() for key in keys]
```

- [ ] **Step 4: Write `src/ui/build.py`**

```python
"""Display rows for the views, driven by metrics.py. Pure: no Streamlit."""

from __future__ import annotations

from typing import Any, Mapping, NamedTuple

import pandas as pd

from src.ui import metrics, stats

STAT_TABLE_COLUMNS = ("Stat", "Total", "Per 90")
SIMILAR_COLUMNS = ("Player", "Club", "League", "Season", "Position", "Minutes", "Similarity")


class PctRow(NamedTuple):
    key: str
    label: str
    pct: float
    value: float


def fmt_value(key: str, value: Any) -> str:
    return stats.fmt_number(value, metrics.fmt_of(key))


def profile_rows(pool: pd.DataFrame, player_row: Mapping[str, Any],
                 group: str) -> dict[str, list[PctRow]]:
    """Percentile rows per category for the group's curated metrics.

    Metrics missing from the pool or blank for the player are skipped, and
    so are categories left empty."""
    out: dict[str, list[PctRow]] = {}
    for category, keys in metrics.PROFILE_METRICS.get(group, {}).items():
        rows: list[PctRow] = []
        for key in keys:
            if key not in pool.columns:
                continue
            value = stats.to_float(player_row.get(key))
            if value is None:
                continue
            pct = stats.percentile_of(pool[key], value,
                                      higher_is_better=key not in metrics.LOWER_IS_BETTER)
            if pct is None:
                continue
            rows.append(PctRow(key, metrics.label(key), pct, value))
        if rows:
            out[category] = rows
    return out


def compare_lists(a_rows: Mapping[str, list[PctRow]],
                  b_rows: Mapping[str, list[PctRow]]) -> tuple[list[str], list[float], list[float]]:
    """Labels and both players' percentiles for the metrics both have, in A's order."""
    b_lookup = {row.key: row for rows in b_rows.values() for row in rows}
    labels: list[str] = []
    a_vals: list[float] = []
    b_vals: list[float] = []
    for rows in a_rows.values():
        for row in rows:
            other = b_lookup.get(row.key)
            if other is None:
                continue
            labels.append(row.label)
            a_vals.append(row.pct)
            b_vals.append(other.pct)
    return labels, a_vals, b_vals


def stat_table(row: Mapping[str, Any], category: str) -> pd.DataFrame:
    """Stat / Total / Per 90 rows for one Stats-tab card.

    A stored per-90 column is shown beside its total rather than on its own
    row. Counting stats without one get a computed per 90. Rates and tallies
    show their value under Total and leave Per 90 blank."""
    paired = set(metrics.PER90_PAIRS.values())
    nineties = stats.to_float(row.get("nineties_played"))
    records = []
    for key in metrics.keys_in_category(category):
        kind = metrics.KIND[key]
        if kind == "per90" and key in paired:
            continue
        if kind == "per90":
            total, p90 = None, stats.to_float(row.get(key))
        else:
            total = stats.to_float(row.get(key))
            if key in metrics.PER90_PAIRS:
                p90 = stats.to_float(row.get(metrics.PER90_PAIRS[key]))
            elif kind in metrics.PER90_COMPUTED_KINDS:
                p90 = stats.per90(total, nineties)
            else:
                p90 = None
        if total is None and p90 is None:
            continue
        decimals = metrics.TOTAL_DECIMALS[metrics.KIND_FORMAT[kind]]
        records.append({
            "Stat": metrics.label(key),
            "Total": None if total is None else round(total, decimals),
            "Per 90": None if p90 is None else round(p90, 2),
        })
    table = pd.DataFrame(records, columns=list(STAT_TABLE_COLUMNS))
    return table.astype({"Total": "float64", "Per 90": "float64"})


def kpi_tiles(row: Mapping[str, Any], group: str) -> list[tuple[str, str, str]]:
    """(label, value text, help text) for the identity card's KPI row."""
    specs = metrics.KPI_TILES["GK" if group == "GK" else "outfield"]
    tiles = []
    for spec in specs:
        if spec.per90:
            value = stats.per90(row.get(spec.key), row.get("nineties_played"))
            text = stats.fmt_number(value, "dec2")
            help_text = f"{metrics.label(spec.key)} divided by 90s played."
        else:
            text = fmt_value(spec.key, row.get(spec.key))
            help_text = metrics.GLOSSARY.get(spec.key, "")
        tiles.append((spec.label, text, help_text))
    return tiles


def similar_table(similar: pd.DataFrame) -> pd.DataFrame:
    """find_similar_players output with readable column names."""
    out = pd.DataFrame({
        "Player": similar["Player"].astype(str),
        "Club": similar["Squad"],
        "League": similar["League"],
        "Season": similar["Season"],
        "Position": similar["position_group"].map(lambda g: metrics.POSITION_NAMES.get(g, g)),
        "Minutes": pd.to_numeric(similar["minutes"], errors="coerce"),
        "Similarity": pd.to_numeric(similar["similarity_score"], errors="coerce").round(3),
    })
    return out.reset_index(drop=True)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv\Scripts\python -m pytest tests/test_metrics.py tests/test_build.py -v`
Expected: all passed. If `test_every_feature_column_has_a_label_category_and_kind` fails, the error lists the column you mistyped. Fix it in `_METRICS` and never in schema.sql.

- [ ] **Step 6: Run the whole suite**

Run: `.venv\Scripts\python -m pytest`
Expected: all passed.

- [ ] **Step 7: Commit**

```powershell
git add src/ui/metrics.py src/ui/build.py tests/test_metrics.py tests/test_build.py
git commit -m "ui: metric config (labels, categories, per-position lists, lower-is-better) and display builders" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Cached data layer `data.py`

**Files:**
- Create: `src/ui/data.py`
- Test: `tests/test_data.py`

**Interfaces:**
- Consumes: `src.similarity.find_similar_players`, `get_player_stats`, `GCP_PROJECT`, `BQ_DATASET` (unchanged).
- Produces (`src.ui.data`). App and view code call these through the module (`data.position_pool(...)`):
  - `SIMILAR_MAX = 20`, `TTL = 3600`
  - `get_bq_client() -> bigquery.Client` (`st.cache_resource`)
  - `load_player_index(_bq) -> pd.DataFrame`, with columns Player, Season, Squad, League, Position, position_group, minutes.
  - `player_stats(_bq, player: str, season: str) -> pd.Series`, one feature-table row. Raises `ValueError` if not found.
  - `position_pool(_bq, position_group: str, season: str) -> pd.DataFrame`, all feature columns, minutes >= 450.
  - `similar_players(_bq, player: str, season: str, position_group: str) -> pd.DataFrame`, the top `SIMILAR_MAX` rows with columns Player, Squad, League, Season, position_group, minutes, similarity_score. Views slice `.head(top_n)`, so moving the slider never re-queries.

- [ ] **Step 1: Write the failing tests**

`tests/test_data.py`:

```python
"""data.py: same SQL as the old app.py, cached, never hits BigQuery in tests."""

from __future__ import annotations

import pandas as pd
import pytest
import streamlit as st

from src.ui import data


class FakeJob:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def to_dataframe(self) -> pd.DataFrame:
        return self._df


class FakeBQ:
    """Records every SQL string; returns a fixed frame."""

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df
        self.calls: list[str] = []

    def query(self, sql: str, job_config=None) -> FakeJob:
        self.calls.append(sql)
        return FakeJob(self.df)


@pytest.fixture(autouse=True)
def clear_caches():
    st.cache_data.clear()
    yield
    st.cache_data.clear()


def norm(sql: str) -> str:
    return " ".join(sql.split())


def test_load_player_index_runs_todays_sql():
    bq = FakeBQ(pd.DataFrame({"Player": ["A"]}))
    out = data.load_player_index(bq)
    expected = (
        f"SELECT Player, Season, Squad, League, Position, position_group, minutes "
        f"FROM `{data.GCP_PROJECT}.{data.BQ_DATASET}.outfield_features` "
        f"UNION ALL "
        f"SELECT Player, Season, Squad, League, Position, position_group, minutes "
        f"FROM `{data.GCP_PROJECT}.{data.BQ_DATASET}.goalkeeper_features` "
        f"ORDER BY Player, Season"
    )
    assert norm(bq.calls[0]) == expected
    assert out["Player"].tolist() == ["A"]


def test_load_player_index_is_cached():
    bq = FakeBQ(pd.DataFrame({"Player": ["A"]}))
    data.load_player_index(bq)
    data.load_player_index(bq)
    assert len(bq.calls) == 1


@pytest.mark.parametrize("group, table", [("GK", "goalkeeper_features"), ("ATT", "outfield_features"),
                                          ("MID", "outfield_features"), ("DEF", "outfield_features")])
def test_position_pool_runs_todays_sql(group, table):
    bq = FakeBQ(pd.DataFrame({"Player": ["A"]}))
    data.position_pool(bq, group, "2023-2024")
    expected = (
        f"SELECT * FROM `{data.GCP_PROJECT}.{data.BQ_DATASET}.{table}` "
        f"WHERE position_group = '{group}' AND Season = '2023-2024' AND minutes >= 450"
    )
    assert norm(bq.calls[0]) == expected


def test_position_pool_cache_key_is_group_and_season():
    bq = FakeBQ(pd.DataFrame({"Player": ["A"]}))
    data.position_pool(bq, "ATT", "2023-2024")
    data.position_pool(bq, "ATT", "2023-2024")
    data.position_pool(bq, "MID", "2023-2024")
    data.position_pool(bq, "ATT", "2022-2023")
    assert len(bq.calls) == 3


def test_player_stats_wraps_get_player_stats(monkeypatch):
    seen = []

    def fake(player, season, bq_client=None):
        seen.append((player, season, bq_client))
        return pd.Series({"Player": player, "Season": season})

    monkeypatch.setattr(data, "get_player_stats", fake)
    bq = object()
    row = data.player_stats(bq, "A", "2023-2024")
    data.player_stats(bq, "A", "2023-2024")
    assert row["Player"] == "A"
    assert seen == [("A", "2023-2024", bq)]  # second call served from cache


def test_similar_players_asks_for_the_max_once(monkeypatch):
    seen = []

    def fake(**kwargs):
        seen.append(kwargs)
        return pd.DataFrame({"Player": ["B"], "similarity_score": [0.9]})

    monkeypatch.setattr(data, "find_similar_players", fake)
    bq = object()
    data.similar_players(bq, "A", "2023-2024", "ATT")
    data.similar_players(bq, "A", "2023-2024", "ATT")
    assert seen == [{"player": "A", "season": "2023-2024", "top_n": data.SIMILAR_MAX, "bq_client": bq}]


def test_errors_are_not_cached(monkeypatch):
    calls = []

    def boom(**kwargs):
        calls.append(1)
        raise RuntimeError("bigquery down")

    monkeypatch.setattr(data, "find_similar_players", boom)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            data.similar_players(object(), "A", "2023-2024", "ATT")
    assert len(calls) == 2
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv\Scripts\python -m pytest tests/test_data.py -v`
Expected: FAIL with `ImportError: cannot import name 'data' from 'src.ui'`.

- [ ] **Step 3: Write `src/ui/data.py`**

```python
"""Cached BigQuery access for the UI.

The SQL is the same text as the pre-redesign app.py (load_player_index and
the Percentile tab's position-pool query). Every query is cached for an
hour, which fixes the old app re-running 4-5 queries on every widget
click. The BigQuery client is passed as `_bq`: the leading underscore
tells st.cache_data not to hash it.

Call these through the module (`data.position_pool(...)`) so the tests can
monkeypatch them.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
from google.cloud import bigquery

from src.similarity import BQ_DATASET, GCP_PROJECT, find_similar_players, get_player_stats

TTL = 3600
SIMILAR_MAX = 20  # the slider's max; views slice .head(top_n)


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT)


@st.cache_data(ttl=TTL, show_spinner="Loading players…")
def load_player_index(_bq) -> pd.DataFrame:
    sql = f"""
        SELECT Player, Season, Squad, League, Position, position_group, minutes
        FROM `{GCP_PROJECT}.{BQ_DATASET}.outfield_features`
        UNION ALL
        SELECT Player, Season, Squad, League, Position, position_group, minutes
        FROM `{GCP_PROJECT}.{BQ_DATASET}.goalkeeper_features`
        ORDER BY Player, Season
    """
    return _bq.query(sql).to_dataframe()


@st.cache_data(ttl=TTL, show_spinner="Loading player stats…")
def player_stats(_bq, player: str, season: str) -> pd.Series:
    return get_player_stats(player, season, _bq)


@st.cache_data(ttl=TTL, show_spinner="Loading the position pool…")
def position_pool(_bq, position_group: str, season: str) -> pd.DataFrame:
    """Everyone in the group and season with 450+ minutes, fetched once and
    shared by the Profile and Similar tabs."""
    if position_group == "GK":
        table = f"`{GCP_PROJECT}.{BQ_DATASET}.goalkeeper_features`"
    else:
        table = f"`{GCP_PROJECT}.{BQ_DATASET}.outfield_features`"

    pool_sql = f"""
        SELECT *
        FROM {table}
        WHERE position_group = '{position_group}'
          AND Season = '{season}'
          AND minutes >= 450
    """
    return _bq.query(pool_sql).to_dataframe()


@st.cache_data(ttl=TTL, show_spinner="Finding similar players…")
def similar_players(_bq, player: str, season: str, position_group: str) -> pd.DataFrame:
    """Top SIMILAR_MAX matches. position_group is part of the cache key only."""
    return find_similar_players(player=player, season=season, top_n=SIMILAR_MAX, bq_client=_bq)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv\Scripts\python -m pytest tests/test_data.py -v`
Expected: 10 passed. Warnings like `missing ScriptRunContext` are expected in bare mode.

- [ ] **Step 5: Commit**

```powershell
git add src/ui/data.py tests/test_data.py
git commit -m "ui: cached BigQuery wrappers with today's SQL" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Floodlight theme `theme.py`

Ported from basketball `dashboard/lib/theme.py` (`kpi()`, `card()`/`card_html()`, `chip()`, `tip()`), plus the `avatar_html()` initials fallback from `lib/media.py`, and restyled. The class prefix is `fl-`.

**Files:**
- Create: `src/ui/theme.py`
- Test: `tests/test_theme.py`

**Interfaces:**
- Consumes: `src.ui.stats.initials` (Task 2).
- Produces (`src.ui.theme`):
  - Tokens: `PAGE, SURFACE, SURFACE_2, BORDER, INK, INK_2, MUTED, GRID, ACCENT, PLAYER, COMPARE: str`, `PCT_RAMP: tuple[str, str, str, str]`, `RADIUS = "2px"`, `FONT_BODY`, `FONT_DISPLAY`, `FONTS_URL`, `CSS`.
  - `inject(st) -> None`, `esc(text) -> str`, `tip(text) -> str`
  - `card_title_html(title, help="") -> str`, `card_html(title, body_html, help="") -> str`, `card(st, title, body_html, help="") -> None`
  - `kpi(label, value, sub="", help="") -> str`, `kpi_row_html(tiles: Iterable[str]) -> str`
  - `chip(text, accent=False) -> str`, `avatar_html(name, size=72) -> str`, `identity_html(name, chips: Sequence[str]) -> str`
  - `hero_html(title, highlight, sub, meta) -> str`, `brand_html() -> str`, `section_label_html(text) -> str`
  - `steps_html(steps: Sequence[tuple[str, str]]) -> str`, `empty_state_html(title, body) -> str`, `footer_html(parts: Sequence[tuple[str, str]]) -> str`
  - `error_box(st, message: str, exc: BaseException) -> None`: `st.error(message)`, with the exception text in an "Error details" expander.

- [ ] **Step 1: Write the failing tests**

`tests/test_theme.py`:

```python
from __future__ import annotations

import tomllib
from pathlib import Path

from src.ui import theme

ROOT = Path(__file__).resolve().parents[1]


def test_tokens_are_floodlight_verbatim():
    assert (theme.PAGE, theme.SURFACE, theme.SURFACE_2, theme.BORDER) == \
        ("#0a110d", "#111a15", "#17231c", "#213028")
    assert (theme.INK, theme.INK_2, theme.MUTED) == ("#ecf2ed", "#b3c1b8", "#7f8f86")
    assert theme.GRID == "rgba(236,242,237,0.07)"
    assert (theme.ACCENT, theme.PLAYER, theme.COMPARE) == ("#b8f05a", "#5aa82c", "#7c7cf2")
    assert theme.PCT_RAMP == ("#3d5a26", "#5c8a2e", "#84c234", "#b8f05a")
    assert theme.RADIUS == "2px"


def test_streamlit_config_mirrors_the_tokens():
    cfg = tomllib.loads((ROOT / ".streamlit" / "config.toml").read_text(encoding="utf-8"))["theme"]
    assert cfg["primaryColor"] == theme.ACCENT
    assert cfg["backgroundColor"] == theme.PAGE
    assert cfg["secondaryBackgroundColor"] == theme.SURFACE_2
    assert cfg["textColor"] == theme.INK
    assert cfg["borderColor"] == theme.BORDER
    assert cfg["baseRadius"] == theme.RADIUS
    assert cfg["sidebar"]["backgroundColor"] == theme.SURFACE


def test_css_avoids_testids_and_leaves_alerts_native():
    assert "data-testid" not in theme.CSS
    assert "stAlert" not in theme.CSS
    assert "progressbar" not in theme.CSS


def test_css_loads_fonts_and_styles_keyed_cards():
    assert f"@import url('{theme.FONTS_URL}')" in theme.CSS
    assert "Barlow+Condensed" in theme.FONTS_URL and "family=Inter" in theme.FONTS_URL
    assert '.stApp [class*="st-key-card"]' in theme.CSS
    assert "Barlow Condensed" in theme.FONT_DISPLAY and "Inter" in theme.FONT_BODY


def test_kpi_markup():
    html = theme.kpi("Goals", "19", sub="in 31 games")
    assert '<div class="fl-kpi-label">Goals</div>' in html
    assert '<div class="fl-kpi-value">19</div>' in html
    assert '<div class="fl-kpi-sub">in 31 games</div>' in html
    assert "fl-tip" not in html
    assert 'class="fl-tip"' in theme.kpi("xG", "1.0", help="Expected goals")


def test_kpi_row_is_one_grid():
    row = theme.kpi_row_html([theme.kpi("A", "1"), theme.kpi("B", "2")])
    assert row.startswith('<div class="fl-kpi-row">') and row.count('class="fl-kpi"') == 2


def test_text_is_escaped():
    assert "&lt;b&gt;" in theme.chip("<b>")
    assert "A &amp; B" in theme.card_title_html("A & B")
    assert "O&#x27;Brien" in theme.identity_html("O'Brien", [])
    assert 'data-tip="a &quot;b&quot;"' in theme.tip('a "b"')


def test_chip_variants():
    assert theme.chip("Liverpool") == '<span class="fl-chip">Liverpool</span>'
    assert theme.chip("Forward", accent=True) == '<span class="fl-chip fl-chip-accent">Forward</span>'


def test_avatar_identity_and_hero():
    assert ">MS</span>" in theme.avatar_html("Mohamed Salah")
    ident = theme.identity_html("Mohamed Salah", [theme.chip("Liverpool")])
    assert "fl-name" in ident and "Mohamed Salah" in ident and "Liverpool" in ident
    hero = theme.hero_html("Football Player", "Profiler", "sub line", "meta line")
    assert "Football Player <em>Profiler</em>" in hero and "sub line" in hero and "meta line" in hero


def test_steps_empty_state_footer_and_labels():
    steps = theme.steps_html([("One", "a"), ("Two", "b"), ("Three", "c")])
    assert ">01<" in steps and ">03<" in steps and "Three" in steps
    empty = theme.empty_state_html("Nothing here", "Try again")
    assert "fl-empty-title" in empty and "Try again" in empty
    footer = theme.footer_html([("Data", "FBref"), ("Built by", "Thomas Tsangou and Ajaiah Darlington")])
    assert "<strong>Built by</strong> Thomas Tsangou and Ajaiah Darlington" in footer
    assert "fl-label" in theme.section_label_html("Season")
    assert "Profiler" in theme.brand_html()


def test_every_html_helper_is_a_single_line():
    outputs = [
        theme.kpi("A", "1", "s", "h"), theme.kpi_row_html([theme.kpi("A", "1")]),
        theme.card_html("T", "<p>x</p>", "h"), theme.identity_html("A B", [theme.chip("c")]),
        theme.hero_html("a", "b", "c", "d"), theme.brand_html(), theme.section_label_html("x"),
        theme.steps_html([("a", "b")]), theme.empty_state_html("a", "b"),
        theme.footer_html([("a", "b")]), theme.avatar_html("A B"),
    ]
    for html in outputs:
        assert "\n" not in html
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv\Scripts\python -m pytest tests/test_theme.py -v`
Expected: FAIL with `ImportError: cannot import name 'theme' from 'src.ui'`.

- [ ] **Step 3: Write `src/ui/theme.py`**

```python
"""Floodlight design tokens, global CSS and small HTML helpers.

Ported from the Basketball Intelligence Platform (dashboard/lib/theme.py:
kpi(), card(), chip(), tip(); lib/media.py: avatar initials) and restyled
as a night match: deep pitch greens, chalk-white type, floodlight-lime
accent. .streamlit/config.toml mirrors the tokens for native widgets
(tests/test_theme.py keeps the two in sync).

Every helper returns ONE line of HTML: st.markdown treats a blank or
indented line inside raw HTML as Markdown and breaks the block. No CSS
targets data-testid; bordered st.container(key="card_*") blocks are styled
through the st-key-card* class Streamlit adds from the key.
"""

from __future__ import annotations

import html
from typing import Iterable, Sequence

from src.ui import stats

PAGE = "#0a110d"        # app background
SURFACE = "#111a15"     # cards, chart surface, sidebar
SURFACE_2 = "#17231c"   # hover, table header, inputs
BORDER = "#213028"      # card and table borders
INK = "#ecf2ed"         # primary text
INK_2 = "#b3c1b8"       # secondary text
MUTED = "#7f8f86"       # labels, captions
GRID = "rgba(236,242,237,0.07)"  # chart gridlines
ACCENT = "#b8f05a"      # brand accent: primaryColor, title bars, active states, hero highlight
PLAYER = "#5aa82c"      # selected player series in charts
COMPARE = "#7c7cf2"     # comparison player series in charts
PCT_RAMP = ("#3d5a26", "#5c8a2e", "#84c234", "#b8f05a")  # bands <25, 25-49, 50-74, 75+
RADIUS = "2px"

FONT_BODY = '"Inter", system-ui, -apple-system, "Segoe UI", sans-serif'
FONT_DISPLAY = '"Barlow Condensed", "Arial Narrow", "Inter", sans-serif'
FONTS_URL = ("https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700;800"
             "&family=Inter:wght@400;500;600;700&display=swap")

CSS = f"""
<style>
@import url('{FONTS_URL}');

/* ---- page: night pitch with very faint mowed stripes ---- */
.stApp {{
  background:
    repeating-linear-gradient(90deg, rgba(236,242,237,0.012) 0px, rgba(236,242,237,0.012) 96px,
                              transparent 96px, transparent 192px),
    radial-gradient(1200px 520px at 50% -18%, rgba(184,240,90,0.06), transparent 62%),
    {PAGE};
}}

/* ---- hero ---- */
.fl-hero {{
  position: relative; overflow: hidden;
  background: linear-gradient(120deg, rgba(184,240,90,0.10) 0%, rgba(17,26,21,0) 55%), {SURFACE};
  border: 1px solid {BORDER}; border-left: 3px solid {ACCENT}; border-radius: {RADIUS};
  padding: 1.3rem 1.6rem; margin-bottom: 1rem;
}}
.fl-hero-title {{ font-family: {FONT_DISPLAY}; font-weight: 800; font-size: 2.6rem; line-height: 1;
                  letter-spacing: .01em; text-transform: uppercase; color: {INK}; }}
.fl-hero-title em {{ font-style: normal; color: {ACCENT}; }}
.fl-hero-sub {{ margin-top: .45rem; font-size: .92rem; color: {INK_2}; }}
.fl-hero-meta {{ margin-top: .3rem; font-size: .7rem; letter-spacing: .1em; text-transform: uppercase;
                 color: {MUTED}; font-weight: 600; }}

/* ---- cards ---- */
.fl-card {{ background: {SURFACE}; border: 1px solid {BORDER}; border-radius: {RADIUS};
            padding: 1rem 1.2rem; margin-bottom: .75rem; }}
.fl-card-title {{ display: flex; align-items: center; margin: 0 0 .6rem 0; font-size: .74rem;
                  font-weight: 700; text-transform: uppercase; letter-spacing: .1em; color: {MUTED}; }}
.fl-card-title::before {{ content: ""; display: inline-block; width: 3px; height: .95em;
                          background: {ACCENT}; border-radius: {RADIUS}; margin-right: .55rem; }}
.stApp [class*="st-key-card"] {{ background: {SURFACE}; border-color: {BORDER}; border-radius: {RADIUS}; }}

/* ---- identity card + KPI tiles ---- */
.fl-identity {{ display: flex; align-items: center; gap: 1rem; }}
.fl-avatar {{ display: inline-flex; align-items: center; justify-content: center; flex: none;
              border-radius: 50%; border: 2px solid {ACCENT}; background: rgba(184,240,90,0.10);
              color: {INK}; font-family: {FONT_DISPLAY}; font-weight: 700; }}
.fl-name {{ font-family: {FONT_DISPLAY}; font-weight: 700; font-size: 2.1rem; line-height: 1.05;
            letter-spacing: .01em; color: {INK}; }}
.fl-chips {{ display: flex; flex-wrap: wrap; gap: .35rem; margin-top: .45rem; }}
.fl-chip {{ display: inline-block; padding: 2px 10px; border-radius: {RADIUS}; background: {SURFACE_2};
            border: 1px solid {BORDER}; color: {INK_2}; font-size: .75rem; font-weight: 600;
            letter-spacing: .02em; }}
.fl-chip-accent {{ color: {ACCENT}; border-color: rgba(184,240,90,0.35); background: rgba(184,240,90,0.08); }}
.fl-kpi-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: .6rem;
               margin-bottom: 1rem; }}
.fl-kpi {{ background: {SURFACE}; border: 1px solid {BORDER}; border-left: 3px solid {ACCENT};
           border-radius: {RADIUS}; padding: .7rem .9rem; }}
.fl-kpi-label {{ display: flex; align-items: center; font-size: .7rem; text-transform: uppercase;
                 letter-spacing: .09em; color: {MUTED}; font-weight: 600; }}
.fl-kpi-value {{ font-family: {FONT_DISPLAY}; font-size: 2rem; font-weight: 700; line-height: 1.1;
                 color: {INK}; font-variant-numeric: tabular-nums; }}
.fl-kpi-sub {{ font-size: .74rem; color: {INK_2}; }}

/* ---- explanation tip (ported bip-tip; opens on hover or tap/focus) ---- */
.fl-tip {{ position: relative; display: inline-flex; align-items: center; justify-content: center;
           width: 13px; height: 13px; margin-left: 6px; border: 1px solid {MUTED}; border-radius: 50%;
           color: {MUTED}; font-size: 9px; font-weight: 700; font-style: normal; line-height: 1;
           text-transform: none; letter-spacing: 0; cursor: help; outline: none; }}
.fl-tip:hover, .fl-tip:focus {{ color: {INK}; border-color: {INK}; }}
.fl-tip::after {{ content: attr(data-tip); position: absolute; z-index: 1000; left: -8px;
                  top: calc(100% + 8px); width: max-content; max-width: 260px; padding: .5rem .65rem;
                  background: {SURFACE_2}; color: {INK}; border: 1px solid {BORDER};
                  border-radius: {RADIUS}; box-shadow: 0 6px 24px rgba(0,0,0,.45);
                  font-family: {FONT_BODY}; font-size: .78rem; font-weight: 400; line-height: 1.4;
                  text-align: left; white-space: normal; opacity: 0; pointer-events: none;
                  transition: opacity .12s ease; }}
.fl-tip:hover::after, .fl-tip:focus::after {{ opacity: 1; }}

/* ---- onboarding steps + empty states ---- */
.fl-steps {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: .75rem;
             margin: .5rem 0 1rem; }}
.fl-steps .fl-card {{ margin-bottom: 0; }}
.fl-step-num {{ font-family: {FONT_DISPLAY}; font-weight: 800; font-size: 2rem; line-height: 1; color: {ACCENT}; }}
.fl-step-title {{ font-weight: 700; color: {INK}; margin: .35rem 0 .2rem; }}
.fl-step-body {{ font-size: .85rem; color: {INK_2}; }}
.fl-empty {{ background: {SURFACE}; border: 1px dashed {BORDER}; border-radius: {RADIUS};
             padding: 1.4rem 1.6rem; margin: .5rem 0 1rem; }}
.fl-empty-title {{ font-family: {FONT_DISPLAY}; font-weight: 700; font-size: 1.35rem; color: {INK};
                   letter-spacing: .01em; }}
.fl-empty-body {{ font-size: .88rem; color: {INK_2}; margin-top: .25rem; }}

/* ---- sidebar ---- */
.fl-brand {{ font-family: {FONT_DISPLAY}; font-weight: 800; font-size: 1.35rem; text-transform: uppercase;
             letter-spacing: .02em; line-height: 1.1; color: {INK}; }}
.fl-brand em {{ font-style: normal; color: {ACCENT}; }}
.fl-brand-sub {{ font-size: .72rem; color: {MUTED}; margin: .2rem 0 .6rem; }}
.fl-label {{ font-size: .68rem; font-weight: 700; text-transform: uppercase; letter-spacing: .12em;
             color: {MUTED}; margin: .9rem 0 .15rem; }}

/* ---- footer ---- */
.fl-footer {{ margin-top: 2.5rem; padding-top: .9rem; border-top: 1px solid {BORDER}; font-size: .76rem;
              color: {MUTED}; line-height: 1.7; }}
.fl-footer strong {{ color: {INK_2}; font-weight: 600; }}

/* ---- phones (<= 640px, Streamlit's column-stacking breakpoint) ---- */
@media (max-width: 640px) {{
  .fl-hero {{ padding: 1rem 1.1rem; }}
  .fl-hero-title {{ font-size: 1.9rem; }}
  .fl-name {{ font-size: 1.6rem; }}
  .fl-kpi-row {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
  .fl-kpi-value {{ font-size: 1.5rem; }}
}}
</style>
"""


def inject(st) -> None:
    st.markdown(CSS, unsafe_allow_html=True)


def esc(text: object) -> str:
    return html.escape(str(text), quote=True)


def tip(text: str) -> str:
    """A small "i" that shows `text` on hover, or on tap/keyboard focus via tabindex."""
    safe = esc(text)
    return (f'<span class="fl-tip" tabindex="0" role="note" aria-label="{safe}" '
            f'data-tip="{safe}">i</span>')


def card_title_html(title: str, help: str = "") -> str:
    return f'<div class="fl-card-title">{esc(title)}{tip(help) if help else ""}</div>'


def card_html(title: str, body_html: str, help: str = "") -> str:
    return f'<div class="fl-card">{card_title_html(title, help)}{body_html}</div>'


def card(st, title: str, body_html: str, help: str = "") -> None:
    st.markdown(card_html(title, body_html, help), unsafe_allow_html=True)


def kpi(label: str, value: str, sub: str = "", help: str = "") -> str:
    tip_html = tip(help) if help else ""
    sub_html = f'<div class="fl-kpi-sub">{esc(sub)}</div>' if sub else ""
    return (f'<div class="fl-kpi"><div class="fl-kpi-label">{esc(label)}{tip_html}</div>'
            f'<div class="fl-kpi-value">{esc(value)}</div>{sub_html}</div>')


def kpi_row_html(tiles: Iterable[str]) -> str:
    """One CSS-grid row: equal-height tiles without any Streamlit column hacks."""
    return f'<div class="fl-kpi-row">{"".join(tiles)}</div>'


def chip(text: str, accent: bool = False) -> str:
    cls = "fl-chip fl-chip-accent" if accent else "fl-chip"
    return f'<span class="{cls}">{esc(text)}</span>'


def avatar_html(name: str, size: int = 72) -> str:
    """Initials disc (no headshots in this dataset)."""
    return (f'<span class="fl-avatar" style="width:{size}px;height:{size}px;'
            f'font-size:{size * 0.36:.0f}px">{esc(stats.initials(name))}</span>')


def identity_html(name: str, chips: Sequence[str]) -> str:
    """Avatar, name and chips. `chips` are chip() HTML strings (already escaped)."""
    return (f'<div class="fl-card"><div class="fl-identity">{avatar_html(name)}'
            f'<div><div class="fl-name">{esc(name)}</div>'
            f'<div class="fl-chips">{"".join(chips)}</div></div></div></div>')


def hero_html(title: str, highlight: str, sub: str, meta: str) -> str:
    return (f'<div class="fl-hero"><div class="fl-hero-title">{esc(title)} <em>{esc(highlight)}</em></div>'
            f'<div class="fl-hero-sub">{esc(sub)}</div><div class="fl-hero-meta">{esc(meta)}</div></div>')


def brand_html() -> str:
    return ('<div class="fl-brand">⚽ Player <em>Profiler</em></div>'
            '<div class="fl-brand-sub">Big 5 leagues · FBref 2017-18 to 2023-24</div>')


def section_label_html(text: str) -> str:
    return f'<div class="fl-label">{esc(text)}</div>'


def steps_html(steps: Sequence[tuple[str, str]]) -> str:
    cards = "".join(
        f'<div class="fl-card"><div class="fl-step-num">{i:02d}</div>'
        f'<div class="fl-step-title">{esc(title)}</div><div class="fl-step-body">{esc(body)}</div></div>'
        for i, (title, body) in enumerate(steps, start=1)
    )
    return f'<div class="fl-steps">{cards}</div>'


def empty_state_html(title: str, body: str) -> str:
    return (f'<div class="fl-empty"><div class="fl-empty-title">{esc(title)}</div>'
            f'<div class="fl-empty-body">{esc(body)}</div></div>')


def footer_html(parts: Sequence[tuple[str, str]]) -> str:
    rows = "".join(f'<div><strong>{esc(label)}</strong> {esc(text)}</div>' for label, text in parts)
    return f'<div class="fl-footer">{rows}</div>'


def error_box(st, message: str, exc: BaseException) -> None:
    """Short readable error; the exception text sits in an expander."""
    st.error(message)
    with st.expander("Error details"):
        st.code(f"{type(exc).__name__}: {exc}", language=None)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv\Scripts\python -m pytest tests/test_theme.py -v`
Expected: 11 passed.

- [ ] **Step 5: Commit**

```powershell
git add src/ui/theme.py tests/test_theme.py
git commit -m "ui: Floodlight theme tokens, CSS and HTML helpers (ported from basketball, restyled)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Plotly charts `viz.py`

Ported from basketball `dashboard/lib/viz.py`: `_layout()`, `PLOTLY_CONFIG`, `percentile_bars()` (~L396-428), `compare_dumbbell()` (~L431-465) and `_empty()`. Restyled for Floodlight: bars coloured by percentile band, `PLAYER`/`COMPARE` dots, and ink-coloured direct labels.

**Files:**
- Create: `src/ui/viz.py`
- Test: `tests/test_viz.py`

**Interfaces:**
- Consumes: `src.ui.stats.band`, `pct_label` (Task 2) and the `src.ui.theme` tokens (Task 5).
- Produces (`src.ui.viz`):
  - `PLOTLY_CONFIG = {"displayModeBar": False}`, `ROW_PX = 38`, `DUMBBELL_ROW_PX = 34`
  - `_layout(**overrides) -> dict`, `_empty(message: str) -> go.Figure`
  - `percentile_bars(rows: list[tuple[str, float, str]]) -> go.Figure`, where rows are (label, percentile 0-100, preformatted raw value).
  - `compare_dumbbell(metrics: list[str], a_vals: list[float], b_vals: list[float], a_name: str, b_name: str) -> go.Figure`
  - Views render with `st.plotly_chart(fig, config=viz.PLOTLY_CONFIG, theme=None, width="stretch", key=...)`. Pass `theme=None` so Streamlit's chart theme doesn't override the layout.

- [ ] **Step 1: Write the failing tests**

`tests/test_viz.py`:

```python
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
    fig = viz.compare_dumbbell(["Goals / 90", "xG / 90"], [80.0, 40.0], [30.0, 70.0], "Salah", "Kane")
    labels = {a.text: a for a in fig.layout.annotations}
    assert set(labels) == {"Salah", "Kane"}
    assert all(a.y == "Goals / 90" for a in labels.values())
    assert all(a.font.color == theme.INK_2 for a in labels.values())
    assert labels["Kane"].xanchor == "right" and labels["Salah"].xanchor == "left"  # Kane is lower


def test_dumbbell_empty_state():
    fig = viz.compare_dumbbell([], [], [], "A", "B")
    assert fig.layout.annotations[0].text == "No shared metrics to compare"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv\Scripts\python -m pytest tests/test_viz.py -v`
Expected: FAIL with `ImportError: cannot import name 'viz' from 'src.ui'`.

- [ ] **Step 3: Write `src/ui/viz.py`**

```python
"""Plotly figures in the Floodlight style.

Ported from the basketball dashboard (lib/viz.py: _layout, PLOTLY_CONFIG,
percentile_bars, compare_dumbbell, _empty). Chart rules: transparent
background, Inter, small margins, no modebar, every axis fixedrange (with
no modebar a zoom could not be undone), GRID hairlines, hovertemplates
ending in <extra></extra>. Text uses the ink tokens, never a series colour.
"""

from __future__ import annotations

import plotly.graph_objects as go

from src.ui import stats
from src.ui import theme as T

PLOTLY_CONFIG = {"displayModeBar": False}
ROW_PX = 38            # percentile bar row height
DUMBBELL_ROW_PX = 34
LINK = "rgba(236,242,237,0.22)"  # dumbbell connector


def _axis(**overrides) -> dict:
    # automargin reserves room for tick labels; without it long metric
    # names get clipped by the small fixed margins.
    base = dict(gridcolor=T.GRID, zeroline=False, linecolor=T.BORDER,
                tickfont=dict(color=T.MUTED, size=11), fixedrange=True, automargin=True)
    base.update(overrides)
    return base


def _layout(**overrides) -> dict:
    base = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=T.FONT_BODY, color=T.INK_2, size=12),
        margin=dict(l=8, r=8, t=8, b=8),
        dragmode=False,
        xaxis=_axis(),
        yaxis=_axis(),
        hoverlabel=dict(bgcolor=T.SURFACE_2, bordercolor=T.BORDER,
                        font=dict(family=T.FONT_BODY, color=T.INK)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(color=T.INK_2)),
        showlegend=False,
        height=320,
    )
    base.update(overrides)
    return base


def _empty(message: str) -> go.Figure:
    """A sentence instead of an empty axis box."""
    fig = go.Figure()
    fig.add_annotation(text=message, showarrow=False, font=dict(color=T.MUTED, size=13),
                       xref="paper", yref="paper", x=0.5, y=0.5)
    fig.update_layout(**_layout(height=160, xaxis=dict(visible=False, fixedrange=True),
                                yaxis=dict(visible=False, fixedrange=True)))
    return fig


def percentile_bars(rows: list[tuple[str, float, str]]) -> go.Figure:
    """Horizontal 0-100 percentile bars, one per metric, coloured by band.

    The label reads "value · 87th · Elite": the raw value because a
    percentile alone hides what it means, and the band in words so colour
    is never the only cue."""
    if not rows:
        return _empty("Not enough players in the pool to rank against")
    labels = [r[0] for r in rows]
    pcts = [float(r[1]) for r in rows]
    values = [r[2] for r in rows]
    bands = [stats.band(p) for p in pcts]
    ords = [stats.pct_label(p) for p in pcts]

    fig = go.Figure()
    fig.add_bar(
        y=labels, x=pcts, orientation="h",
        marker=dict(color=[T.PCT_RAMP[b.index] for b in bands], cornerradius=2),
        text=[f"{v} · {o} · {b.label}" for v, o, b in zip(values, ords, bands)],
        textposition="outside", cliponaxis=False,
        textfont=dict(color=T.INK_2, size=11, family=T.FONT_BODY),
        customdata=[[v, o, b.label] for v, o, b in zip(values, ords, bands)],
        hovertemplate="%{y}: %{customdata[0]}<br>%{customdata[1]} percentile · %{customdata[2]}<extra></extra>",
        showlegend=False,
    )
    fig.add_vline(x=50, line_color=T.MUTED, line_width=1, line_dash="dot")
    fig.update_layout(**_layout(
        height=ROW_PX * len(rows) + 60,
        bargap=0.34,
        # headroom past 100 so the outside labels are never clipped
        xaxis=_axis(range=[0, 165], tickvals=[0, 25, 50, 75, 100],
                    title=dict(text="Percentile in position pool", font=dict(color=T.MUTED, size=11))),
        yaxis=_axis(autorange="reversed", showgrid=False, tickfont=dict(color=T.INK_2, size=12)),
    ))
    return fig


def compare_dumbbell(metrics: list[str], a_vals: list[float], b_vals: list[float],
                     a_name: str, b_name: str) -> go.Figure:
    """Two players' percentiles on the same metrics: dots joined by a line, so
    the gap is the first thing you read."""
    if not metrics:
        return _empty("No shared metrics to compare")
    fig = go.Figure()
    for metric, a, b in zip(metrics, a_vals, b_vals):
        fig.add_scatter(x=[a, b], y=[metric, metric], mode="lines",
                        line=dict(color=LINK, width=2), hoverinfo="skip", showlegend=False)
    for vals, name, color in ((a_vals, a_name, T.PLAYER), (b_vals, b_name, T.COMPARE)):
        fig.add_scatter(
            x=list(vals), y=list(metrics), mode="markers", name=name,
            marker=dict(color=color, size=12, line=dict(color=T.SURFACE, width=2)),
            customdata=[stats.pct_label(v) for v in vals],
            hovertemplate=name + "<br>%{y}: %{customdata} percentile<extra></extra>",
        )
    # Direct labels on the first row, in ink, each on the outer side of its dot.
    a0, b0 = a_vals[0], b_vals[0]
    a_left = a0 <= b0
    for value, name, is_left in ((a0, a_name, a_left), (b0, b_name, not a_left)):
        fig.add_annotation(x=value, y=metrics[0], text=name, showarrow=False, yshift=16,
                           xanchor="right" if is_left else "left",
                           font=dict(color=T.INK_2, size=11))
    fig.add_vline(x=50, line_color=T.MUTED, line_width=1, line_dash="dot")
    fig.update_layout(**_layout(
        height=max(220, DUMBBELL_ROW_PX * len(metrics) + 90),
        showlegend=True,
        margin=dict(l=8, r=8, t=40, b=8),
        xaxis=_axis(range=[0, 100], tickvals=[0, 25, 50, 75, 100],
                    title=dict(text="Percentile in position pool", font=dict(color=T.MUTED, size=11))),
        yaxis=_axis(autorange="reversed", showgrid=False, tickfont=dict(color=T.INK_2, size=11)),
    ))
    return fig
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv\Scripts\python -m pytest tests/test_viz.py -v`
Expected: 8 passed.

- [ ] **Step 5: Commit**

```powershell
git add src/ui/viz.py tests/test_viz.py
git commit -m "ui: Floodlight Plotly charts - banded percentile bars and comparison dumbbell" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Views: header, Profile, Stats and Similar players

**Files:**
- Create: `src/ui/state.py`, `src/ui/views/header.py`, `src/ui/views/profile.py`, `src/ui/views/stats_tab.py`, `src/ui/views/similar.py`, `tests/fakes.py`
- Test: `tests/test_views.py`

**Interfaces:**
- Consumes: `build` (Task 3), `data` (Task 4; only `data.similar_players` and `data.SIMILAR_MAX` here), `theme` (Task 5), `viz` (Task 6), `metrics`, `stats`, `src.similarity.MIN_MINUTES`, `tests.schema_utils.feature_columns`.
- Produces:
  - `src.ui.state`: constants `ALL_SEASONS = "All"`, `ALL_LEAGUES = "All leagues"`, `ALL_CLUBS = "All clubs"`; keys `SEASON = "f_season"`, `LEAGUE = "f_league"`, `CLUB = "f_club"`, `PLAYER = "player_pick"`, `SWITCH = "season_switch"`, `SWITCH_FOR = "_switch_for"`, `SIMILAR_TABLE = "similar_table"`; and `goto(player: str, season: str) -> None`, an `on_click` callback that points the filters and search at that player-season.
  - `src.ui.views.header.render(row: pd.Series) -> None` and `chips_for(row) -> list[str]`
  - `src.ui.views.profile.render(pool: pd.DataFrame | None, player: str, season: str, group: str) -> None`
  - `src.ui.views.stats_tab.render(row: pd.Series, group: str) -> None`
  - `src.ui.views.similar.render(bq, pool: pd.DataFrame | None, row: pd.Series) -> None`
  - `tests.fakes`: `LATEST = "2023-2024"`, `PREVIOUS = "2022-2023"`, `INDEX_ROWS`, `player_index()`, `outfield_frame()`, `goalkeeper_frame()`, `player_stats(player, season) -> pd.Series`, `position_pool(group, season)`, `similar_players(player, season, group)`

- [ ] **Step 1: Write the test fakes `tests/fakes.py`**

```python
"""Small fake BigQuery results for tests. Columns come from schema.sql."""

from __future__ import annotations

import numpy as np
import pandas as pd

from tests.schema_utils import IDENTITY_COLUMNS, feature_columns

LATEST = "2023-2024"
PREVIOUS = "2022-2023"
INDEX_COLUMNS = ["Player", "Season", "Squad", "League", "Position", "position_group", "minutes"]

# Player, Season, Squad, League, Position, position_group, minutes, Nation, Age
INDEX_ROWS = [
    ("Mohamed Salah", LATEST, "Liverpool", "Premier League", "FW", "ATT", 2536, "EGY", 31),
    ("Mohamed Salah", PREVIOUS, "Liverpool", "Premier League", "FW", "ATT", 3290, "EGY", 30),
    ("Erling Haaland", LATEST, "Manchester City", "Premier League", "FW", "ATT", 2552, "NOR", 23),
    ("Harry Kane", LATEST, "Bayern Munich", "Bundesliga", "FW", "ATT", 2617, "ENG", 30),
    ("Victor Osimhen", LATEST, "Napoli", "Serie A", "FW", "ATT", 2002, "NGA", 24),
    ("Kylian Mbappé", LATEST, "Paris S-G", "Ligue 1", "FW", "ATT", 2621, "FRA", 24),
    ("Youssef En-Nesyri", LATEST, "Sevilla", "La Liga", "FW", "ATT", 1900, "MAR", 26),
    ("Bench Striker", LATEST, "Sevilla", "La Liga", "FW", "ATT", 120, "ESP", 19),
    ("Alisson", LATEST, "Liverpool", "Premier League", "GK", "GK", 2520, "BRA", 30),
    ("Ederson", LATEST, "Manchester City", "Premier League", "GK", "GK", 2970, "BRA", 29),
    ("Jan Oblak", LATEST, "Atlético Madrid", "La Liga", "GK", "GK", 3150, "SVN", 30),
    ("Gregor Kobel", LATEST, "Dortmund", "Bundesliga", "GK", "GK", 2700, "SUI", 25),
]


def player_index() -> pd.DataFrame:
    return pd.DataFrame([r[:7] for r in INDEX_ROWS], columns=INDEX_COLUMNS)


def _feature_frame(table: str, rows: list[tuple]) -> pd.DataFrame:
    cols = feature_columns(table)
    rng = np.random.default_rng(7 if table == "outfield_features" else 11)
    records = []
    for player, season, squad, league, pos, group, minutes, nation, age in rows:
        rec = {c: float(rng.uniform(0.05, 5.0)) for c in cols if c not in IDENTITY_COLUMNS}
        rec.update(Player=player, Nation=nation, Position=pos, Squad=squad, League=league,
                   Age=age, Born=2023 - age, Season=season, position_group=group,
                   minutes=minutes, nineties_played=round(minutes / 90, 1))
        records.append(rec)
    return pd.DataFrame(records, columns=cols)


def outfield_frame() -> pd.DataFrame:
    return _feature_frame("outfield_features", [r for r in INDEX_ROWS if r[5] != "GK"])


def goalkeeper_frame() -> pd.DataFrame:
    return _feature_frame("goalkeeper_features", [r for r in INDEX_ROWS if r[5] == "GK"])


def player_stats(player: str, season: str) -> pd.Series:
    for frame in (outfield_frame(), goalkeeper_frame()):
        hit = frame[(frame["Player"] == player) & (frame["Season"] == season)]
        if not hit.empty:
            return hit.iloc[0]
    raise ValueError(f"Player '{player}' not found for season '{season}'.")


def position_pool(group: str, season: str) -> pd.DataFrame:
    frame = goalkeeper_frame() if group == "GK" else outfield_frame()
    pool = frame[(frame["position_group"] == group) & (frame["Season"] == season)
                 & (frame["minutes"] >= 450)]
    return pool.reset_index(drop=True)


def similar_players(player: str, season: str, group: str) -> pd.DataFrame:
    pool = position_pool(group, season)
    others = pool[pool["Player"] != player].reset_index(drop=True)
    out = others[["Player", "Squad", "League", "Season", "position_group", "minutes"]].copy()
    out["similarity_score"] = np.linspace(0.95, 0.5, len(out)) if len(out) else []
    return out
```

- [ ] **Step 2: Write the failing view tests `tests/test_views.py`**

```python
"""Each view rendered on its own with fake data (AppTest.from_function; no BigQuery)."""

from __future__ import annotations

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
    for text in ("Goalkeeper", "Save %", "Goals against / 90", "Clean sheet %", "Saves / 90"):
        assert text in md, text


# --- profile -----------------------------------------------------------------

def test_profile_outfield_cards_and_pool_caption():
    at = _run(profile_script, "Mohamed Salah", fakes.LATEST, "ATT", False)
    assert "vs 6 forwards in 2023-24, 450+ minutes" in _captions(at)
    md = _markdown(at)
    assert "Shooting" in md and "Passing" in md and "Progression" in md
    assert len(at.get("plotly_chart")) == 3
    assert "Elite 75th+" in _captions(at)


def test_profile_goalkeeper():
    at = _run(profile_script, "Alisson", fakes.LATEST, "GK", False)
    assert "vs 4 goalkeepers in 2023-24, 450+ minutes" in _captions(at)
    assert len(at.get("plotly_chart")) == 1


def test_profile_player_below_threshold_gets_empty_state():
    at = _run(profile_script, "Bench Striker", fakes.LATEST, "ATT", False)
    assert "Bench Striker isn't in the percentile pool" in _markdown(at)
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


def test_similar_selection_shows_dumbbell_and_view_profile_jumps(stub_similar):
    at = _run(similar_script, "Mohamed Salah", fakes.LATEST)
    at.session_state["similar_table"] = {"selection": {"rows": [0], "columns": []}}
    at.run()
    assert not at.exception
    assert len(at.get("plotly_chart")) == 1
    assert "Mohamed Salah vs Erling Haaland" in _markdown(at)
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv\Scripts\python -m pytest tests/test_views.py -v`
Expected: FAIL. Every test errors with `ModuleNotFoundError: No module named 'src.ui.views.header'`, or the matching module for profile, stats_tab or similar, raised inside AppTest.

- [ ] **Step 4: Write `src/ui/state.py`**

```python
"""Session-state keys shared by the sidebar, the search box and the Similar tab."""

from __future__ import annotations

import streamlit as st

ALL_SEASONS = "All"
ALL_LEAGUES = "All leagues"
ALL_CLUBS = "All clubs"

SEASON = "f_season"          # sidebar season pills
LEAGUE = "f_league"          # sidebar league selectbox
CLUB = "f_club"              # sidebar club selectbox
PLAYER = "player_pick"       # player search selectbox
SWITCH = "season_switch"     # per-player season switcher
SWITCH_FOR = "_switch_for"   # which player SWITCH was last set for
SIMILAR_TABLE = "similar_table"


def goto(player: str, season: str) -> None:
    """on_click callback: open `player` in `season`.

    Filters are widened to that season with all leagues and clubs, so the
    player is always in the search options when the page reruns."""
    st.session_state[SEASON] = season
    st.session_state[LEAGUE] = ALL_LEAGUES
    st.session_state[CLUB] = ALL_CLUBS
    st.session_state[PLAYER] = player
    st.session_state[SWITCH] = season
    st.session_state[SWITCH_FOR] = player
```

- [ ] **Step 5: Write `src/ui/views/header.py`**

```python
"""Identity card (initials avatar, name, chips) and the KPI row."""

from __future__ import annotations

from typing import Any, Mapping

import streamlit as st

from src.ui import build, metrics, stats, theme


def chips_for(row: Mapping[str, Any]) -> list[str]:
    group = row.get("position_group")
    chips = [theme.chip(metrics.POSITION_NAMES.get(group, str(group)), accent=True)]
    for key in ("Squad", "League"):
        if not stats.is_missing(row.get(key)):
            chips.append(theme.chip(str(row.get(key))))
    chips.append(theme.chip(stats.short_season(row.get("Season"))))
    age = stats.to_float(row.get("Age"))
    if age is not None:
        chips.append(theme.chip(f"Age {int(age)}"))
    nation = row.get("Nation")
    if not stats.is_missing(nation) and str(nation).strip():
        chips.append(theme.chip(str(nation).strip()))
    return chips


def render(row) -> None:
    tiles = [theme.kpi(label, text, help=help_text)
             for label, text, help_text in build.kpi_tiles(row, row["position_group"])]
    st.markdown(theme.identity_html(str(row["Player"]), chips_for(row)) + theme.kpi_row_html(tiles),
                unsafe_allow_html=True)
```

- [ ] **Step 6: Write `src/ui/views/profile.py`**

```python
"""Profile tab: percentile bars by category for the position's curated metrics."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.similarity import MIN_MINUTES
from src.ui import build, metrics, stats, theme, viz

BAND_KEY = ("Bands: Elite 75th+ · Above avg 50th-74th · Below avg 25th-49th · Poor under 25th. "
            "Stats where lower is better (errors, cards, goals against) are inverted, "
            "so a longer bar is always better.")


def render(pool: pd.DataFrame | None, player: str, season: str, group: str) -> None:
    plural = metrics.POSITION_PLURALS.get(group, "players")
    short = stats.short_season(season)
    if pool is None or pool.empty:
        st.markdown(theme.empty_state_html(
            "No comparison pool",
            f"No {plural} reached {MIN_MINUTES} minutes in {short}, so there is nothing to rank against."),
            unsafe_allow_html=True)
        return
    me = pool[(pool["Player"] == player) & (pool["Season"] == season)]
    if me.empty:
        st.markdown(theme.empty_state_html(
            f"{player} isn't in the percentile pool",
            f"Percentiles compare {plural} with {MIN_MINUTES}+ minutes in {short}. "
            f"{player} played fewer minutes than that."),
            unsafe_allow_html=True)
        return

    by_category = build.profile_rows(pool, me.iloc[0], group)
    st.caption(f"vs {len(pool):,} {plural} in {short}, {MIN_MINUTES}+ minutes")
    cards = list(by_category.items())
    for start in range(0, len(cards), 2):
        cols = st.columns(2, gap="medium")
        for col, (category, rows) in zip(cols, cards[start:start + 2]):
            with col, st.container(border=True, key=f"card_profile_{stats.slug(category)}"):
                st.markdown(theme.card_title_html(category), unsafe_allow_html=True)
                bars = [(r.label, r.pct, build.fmt_value(r.key, r.value)) for r in rows]
                st.plotly_chart(viz.percentile_bars(bars), config=viz.PLOTLY_CONFIG, theme=None,
                                width="stretch", key=f"pct_{stats.slug(category)}")
    st.caption(BAND_KEY)
```

- [ ] **Step 7: Write `src/ui/views/stats_tab.py`**

```python
"""Stats tab: every stat by category, the season total next to per 90."""

from __future__ import annotations

import streamlit as st

from src.ui import build, metrics, stats, theme

# One format per column: the grid can't format per row. Total is
# "localized", so 1,234 / 12.3 / 41.3 read naturally; values are rounded per
# metric kind in build.stat_table. Per 90 is always two decimals.
COLUMN_CONFIG = {
    "Stat": st.column_config.TextColumn("Stat", width="medium"),
    "Total": st.column_config.NumberColumn(
        "Total", format="localized",
        help="Season total. For % and per-shot stats this is the season rate itself."),
    "Per 90": st.column_config.NumberColumn("Per 90", format="%.2f", help="Per 90 minutes played."),
}
NOTE = ("Per 90 = per 90 minutes played. Percentages and per-shot stats are season rates, "
        "so they have no per-90 value.")


def render(row, group: str) -> None:
    tables = [(category, build.stat_table(row, category)) for category in metrics.stats_categories(group)]
    tables = [(category, table) for category, table in tables if not table.empty]
    if not tables:
        st.markdown(theme.empty_state_html("No stats recorded",
                                           "This player-season has no stat columns filled in."),
                    unsafe_allow_html=True)
        return
    for start in range(0, len(tables), 2):
        cols = st.columns(2, gap="medium")
        for col, (category, table) in zip(cols, tables[start:start + 2]):
            with col, st.container(border=True, key=f"card_stats_{stats.slug(category)}"):
                st.markdown(theme.card_title_html(category), unsafe_allow_html=True)
                st.dataframe(table, hide_index=True, width="stretch", height="content",
                             row_height=34, column_config=COLUMN_CONFIG)
    st.caption(NOTE)
```

- [ ] **Step 8: Write `src/ui/views/similar.py`**

```python
"""Similar players tab: similarity table, a head-to-head dumbbell for the
selected row, and a button that opens that player's profile."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.similarity import MIN_MINUTES
from src.ui import build, data, metrics, state, stats, theme, viz

METHOD = ("Method: cosine similarity on z-scored stats within the position pool "
          f"(same position and season, {MIN_MINUTES}+ minutes).")

COLUMN_CONFIG = {
    "Player": st.column_config.TextColumn("Player", width="medium"),
    "Club": st.column_config.TextColumn("Club"),
    "League": st.column_config.TextColumn("League"),
    "Season": st.column_config.TextColumn("Season"),
    "Position": st.column_config.TextColumn("Position"),
    "Minutes": st.column_config.NumberColumn("Minutes", format="localized"),
    "Similarity": st.column_config.ProgressColumn(
        "Similarity", min_value=0.0, max_value=1.0, format="%.2f", color=theme.ACCENT,
        help="1.00 = an identical statistical profile"),
}


def render(bq, pool: pd.DataFrame | None, row) -> None:
    player, season, group = str(row["Player"]), str(row["Season"]), str(row["position_group"])
    st.caption(METHOD)
    minutes = stats.to_float(row.get("minutes")) or 0.0
    if minutes < MIN_MINUTES:
        st.markdown(theme.empty_state_html(
            f"{player} is below the {MIN_MINUTES}-minute threshold",
            f"Similarity search needs at least {MIN_MINUTES} minutes in a season; "
            f"{player} played {minutes:,.0f}."),
            unsafe_allow_html=True)
        return

    top_n = st.slider("Players to show", min_value=5, max_value=data.SIMILAR_MAX, value=10, key="top_n")
    try:
        matches = data.similar_players(bq, player, season, group)
    except Exception as exc:  # noqa: BLE001 - BigQuery, auth and "not in pool" all end here
        theme.error_box(st, "Couldn't compute similar players.", exc)
        return
    if matches.empty:
        plural = metrics.POSITION_PLURALS.get(group, "players")
        st.markdown(theme.empty_state_html(
            "No similar players",
            f"No other {plural} reached {MIN_MINUTES} minutes in {stats.short_season(season)}."),
            unsafe_allow_html=True)
        return

    table = build.similar_table(matches.head(top_n))
    event = st.dataframe(table, hide_index=True, width="stretch", on_select="rerun",
                         selection_mode="single-row", key=state.SIMILAR_TABLE,
                         column_config=COLUMN_CONFIG)
    rows = list(event.selection.rows) if event is not None else []
    picked = [i for i in rows if i < len(table)]
    if not picked:
        st.caption(f"Select a row to compare that player with {player}.")
        return
    other = table.iloc[picked[0]]
    _compare(pool, row, other, group)
    st.button(f"View {other['Player']}'s profile", key="view_profile", type="primary",
              on_click=state.goto, args=(str(other["Player"]), str(other["Season"])))


def _compare(pool: pd.DataFrame | None, row, other: pd.Series, group: str) -> None:
    a_name, b_name = str(row["Player"]), str(other["Player"])
    if pool is None or pool.empty:
        st.caption("Comparison unavailable: the position pool didn't load.")
        return
    a = pool[(pool["Player"] == a_name) & (pool["Season"] == row["Season"])]
    b = pool[(pool["Player"] == b_name) & (pool["Season"] == other["Season"])]
    if a.empty or b.empty:
        st.caption("Comparison unavailable: one of the players isn't in the position pool.")
        return
    labels, a_vals, b_vals = build.compare_lists(build.profile_rows(pool, a.iloc[0], group),
                                                 build.profile_rows(pool, b.iloc[0], group))
    with st.container(border=True, key="card_compare"):
        st.markdown(theme.card_title_html(f"{a_name} vs {b_name} · percentiles"), unsafe_allow_html=True)
        st.plotly_chart(viz.compare_dumbbell(labels, a_vals, b_vals, a_name, b_name),
                        config=viz.PLOTLY_CONFIG, theme=None, width="stretch", key="compare_chart")
```

- [ ] **Step 9: Run the view tests to verify they pass**

Run: `.venv\Scripts\python -m pytest tests/test_views.py -v`
Expected: 12 passed.

- [ ] **Step 10: Run the whole suite**

Run: `.venv\Scripts\python -m pytest`
Expected: all passed.

- [ ] **Step 11: Commit**

```powershell
git add src/ui/state.py src/ui/views/header.py src/ui/views/profile.py src/ui/views/stats_tab.py src/ui/views/similar.py tests/fakes.py tests/test_views.py
git commit -m "ui: identity card, Profile, Stats and Similar players views" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: App shell: sidebar, search, onboarding, season switcher, footer and AppTest smoke tests

The old `app.py` (508 lines) is replaced by a thin shell. `main()` returns early instead of calling `st.stop()`, so the footer renders on every path: empty filters, no selection and BigQuery failure.

**Files:**
- Create: `src/ui/views/picker.py`
- Rewrite: `app.py`
- Test: `tests/test_app.py`

**Interfaces:**
- Consumes: `data.get_bq_client`, `data.load_player_index`, `data.player_stats`, `data.position_pool` (Task 4); `filters` (Task 2); `state`, `header`, `profile`, `stats_tab`, `similar` (Task 7); `theme` (Task 5); `tests.fakes` (Task 7).
- Produces (`src.ui.views.picker`):
  - `EXAMPLE_PLAYERS: tuple[str, ...]`, `SEARCH_PLACEHOLDER = "Search a player, e.g. Salah"`, `STEPS`
  - `sidebar(index: pd.DataFrame) -> pd.DataFrame`, which renders the brand, season pills, league and club and returns the filtered index.
  - `search(filtered: pd.DataFrame) -> str | None`, the player selectbox (`index=None`).
  - `onboarding(filtered: pd.DataFrame) -> None`, the 3 step cards plus the "Try an example" button (key `try_example`).
  - `season_switch(filtered: pd.DataFrame, player: str) -> str | None`, a segmented control (key `season_switch`) when the player has 2+ seasons in view.
- `app.py`: `HERO_*` constants, `FOOTER_PARTS`, `main() -> None`.

- [ ] **Step 1: Write the failing smoke tests `tests/test_app.py`**

```python
"""Smoke tests: the whole app renders with the data layer stubbed (no BigQuery)."""

from __future__ import annotations

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
    assert "isn't in the percentile pool" in md
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv\Scripts\python -m pytest tests/test_app.py -v`
Expected: FAIL. The old `app.py` calls the real BigQuery client and has no `player_pick` widget, so you'll see an AppTest exception or `KeyError` on `at.selectbox(key="player_pick")`. `test_no_deprecated_use_container_width` fails on the old `app.py` L393.

- [ ] **Step 3: Write `src/ui/views/picker.py`**

```python
"""Sidebar filters, the player search, onboarding and the season switcher.

Fixes the old app's stateless season buttons (every rerun snapped back to
the oldest season) and its selectbox that auto-picked the first player.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.ui import filters, state, stats, theme

EXAMPLE_PLAYERS = ("Mohamed Salah", "Kevin De Bruyne", "Erling Haaland", "Virgil van Dijk")
SEARCH_PLACEHOLDER = "Search a player, e.g. Salah"
STEPS = (
    ("Set the scope", "Pick a season, league or club in the sidebar to narrow the list."),
    ("Find a player", "Type a name in the search box above."),
    ("Read the profile", "Percentiles against his position, every stat, and his most similar players."),
)


def _init(key: str, value: object) -> None:
    if key not in st.session_state:
        st.session_state[key] = value


def _season_label(value: str) -> str:
    return value if value == state.ALL_SEASONS else stats.short_season(value)


def sidebar(index: pd.DataFrame) -> pd.DataFrame:
    seasons = filters.seasons_desc(index)
    _init(state.SEASON, seasons[0] if seasons else state.ALL_SEASONS)
    _init(state.LEAGUE, state.ALL_LEAGUES)
    _init(state.CLUB, state.ALL_CLUBS)
    with st.sidebar:
        st.markdown(theme.brand_html(), unsafe_allow_html=True)

        st.markdown(theme.section_label_html("Season"), unsafe_allow_html=True)
        season = st.pills("Season", [state.ALL_SEASONS, *seasons], key=state.SEASON,
                          required=True, format_func=_season_label, label_visibility="collapsed")

        st.markdown(theme.section_label_html("League"), unsafe_allow_html=True)
        league = st.selectbox("League", [state.ALL_LEAGUES, *filters.leagues(index)],
                              key=state.LEAGUE, label_visibility="collapsed")
        league_filter = None if league == state.ALL_LEAGUES else league

        clubs = [state.ALL_CLUBS, *filters.clubs(index, league_filter)]
        if st.session_state.get(state.CLUB) not in clubs:  # league changed under the club
            st.session_state[state.CLUB] = state.ALL_CLUBS
        st.markdown(theme.section_label_html("Club"), unsafe_allow_html=True)
        club = st.selectbox("Club", clubs, key=state.CLUB, label_visibility="collapsed")

        filtered = filters.filter_index(
            index,
            season=None if season == state.ALL_SEASONS else season,
            league=league_filter,
            club=None if club == state.ALL_CLUBS else club,
        )
        st.caption(f"Showing {filtered['Player'].nunique():,} players")
    return filtered


def search(filtered: pd.DataFrame) -> str | None:
    options = filters.search_options(filtered)
    labels = {player: filters.option_label(player, club, league)
              for player, club, league in zip(options["Player"], options["Squad"], options["League"])}
    if st.session_state.get(state.PLAYER) not in labels:  # filters moved the player out of view
        st.session_state[state.PLAYER] = None
    return st.selectbox("Search player", list(labels), index=None, key=state.PLAYER,
                        format_func=labels.__getitem__, placeholder=SEARCH_PLACEHOLDER,
                        label_visibility="collapsed")


def _choose(player: str) -> None:
    st.session_state[state.PLAYER] = player


def onboarding(filtered: pd.DataFrame) -> None:
    st.markdown(theme.steps_html(STEPS), unsafe_allow_html=True)
    example = filters.pick_example(filtered, EXAMPLE_PLAYERS)
    if example:
        st.button(f"Try an example: {example}", key="try_example", type="primary",
                  on_click=_choose, args=(example,))


def season_switch(filtered: pd.DataFrame, player: str) -> str | None:
    """The season to show. Defaults to the player's latest season in view and
    stays put across reruns, until a different player is picked."""
    seasons = filters.player_seasons(filtered, player)
    if not seasons:
        return None
    if (st.session_state.get(state.SWITCH_FOR) != player
            or st.session_state.get(state.SWITCH) not in seasons):
        st.session_state[state.SWITCH] = seasons[-1]
        st.session_state[state.SWITCH_FOR] = player
    if len(seasons) == 1:
        return seasons[0]
    st.markdown(theme.section_label_html("Season"), unsafe_allow_html=True)
    return st.segmented_control("Season", seasons, key=state.SWITCH, required=True,
                                format_func=stats.short_season, label_visibility="collapsed")
```

- [ ] **Step 4: Rewrite `app.py`**

```python
"""
app.py
======
Football Player Profiler - Streamlit entry point.

Thin shell: page config, theme, hero, sidebar filters, player search and
the three tabs. The rest lives in src/ui/ (metric config in
src/ui/metrics.py, cached BigQuery queries in src/ui/data.py).

Run locally:
    .venv/Scripts/python -m streamlit run app.py
"""

from __future__ import annotations

import streamlit as st

from src.ui import data, theme
from src.ui.views import header, picker, profile, similar, stats_tab

st.set_page_config(page_title="Football Player Profiler", page_icon="⚽", layout="wide")
theme.inject(st)

HERO_TITLE = "Football Player"
HERO_HIGHLIGHT = "Profiler"
HERO_SUB = "Big 5 leagues · 2017-18 to 2023-24 · 16.5k player-seasons"
HERO_META = "FBref data, frozen snapshot · BigQuery"
FOOTER_PARTS = (
    ("Data", "FBref snapshot, Big 5 leagues, 2017-18 to 2023-24"),
    ("Pipeline", "FBref → GCS → BigQuery"),
    ("Built by", "Thomas Tsangou and Ajaiah Darlington"),
)


def main() -> None:
    try:
        bq = data.get_bq_client()
        index = data.load_player_index(bq)
    except Exception as exc:  # noqa: BLE001 - any auth/BigQuery failure gets the same message
        theme.error_box(st, "Couldn't load the player list from BigQuery. "
                            "Check the Google Cloud credentials and try again.", exc)
        return

    filtered = picker.sidebar(index)
    if filtered.empty:
        st.markdown(theme.empty_state_html(
            "No players match these filters",
            "Try another season, or set League and Club back to All in the sidebar."),
            unsafe_allow_html=True)
        return

    player = picker.search(filtered)
    if player is None:
        picker.onboarding(filtered)
        return

    season = picker.season_switch(filtered, player)
    try:
        row = data.player_stats(bq, player, season)
    except Exception as exc:  # noqa: BLE001
        theme.error_box(st, f"Couldn't load stats for {player} ({season}).", exc)
        return
    group = row["position_group"]
    header.render(row)

    pool, pool_error = None, None
    try:
        pool = data.position_pool(bq, group, season)  # fetched once, shared by two tabs
    except Exception as exc:  # noqa: BLE001
        pool_error = exc

    tab_profile, tab_stats, tab_similar = st.tabs(["Profile", "Stats", "Similar players"])
    with tab_profile:
        if pool_error is not None:
            theme.error_box(st, "Couldn't load the position pool for percentiles.", pool_error)
        else:
            profile.render(pool, player, season, group)
    with tab_stats:
        stats_tab.render(row, group)
    with tab_similar:
        similar.render(bq, pool, row)


st.markdown(theme.hero_html(HERO_TITLE, HERO_HIGHLIGHT, HERO_SUB, HERO_META), unsafe_allow_html=True)
main()
st.markdown(theme.footer_html(FOOTER_PARTS), unsafe_allow_html=True)
```

- [ ] **Step 5: Run the smoke tests to verify they pass**

Run: `.venv\Scripts\python -m pytest tests/test_app.py -v`
Expected: 11 passed.

- [ ] **Step 6: Run the whole suite**

Run: `.venv\Scripts\python -m pytest`
Expected: all passed, 0 failed.

- [ ] **Step 7: Boot check against real BigQuery (gcloud ADC works on this machine)**

Run in a second terminal: `.venv\Scripts\python -m streamlit run app.py --server.headless true --server.port 8599`
Then: `Invoke-WebRequest -UseBasicParsing http://localhost:8599/_stcore/health | Select-Object -ExpandProperty Content`
Expected: `ok`. Stop the server with Ctrl+C. Task 9 does the full visual check.

- [ ] **Step 8: Commit**

```powershell
git add app.py src/ui/views/picker.py tests/test_app.py
git commit -m "ui: thin app shell with sidebar filters, search, onboarding, season switcher and footer" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Visual verification, README and final check

**Files:**
- Create: `scripts/screenshot.py`, `docs/screenshots/01-empty.png`, `03-outfield-profile.png`, `05-outfield-similar.png`, `07-gk-profile.png` (the script writes all 8; commit these 4)
- Modify: `README.md`

**Interfaces:**
- Consumes: the running app (Tasks 1-8), CSS classes `.fl-hero-title` and `.stApp [class*="st-key-card"]` (Task 5), placeholder `"Search a player, e.g. Salah"` (Task 8), tab names "Profile"/"Stats"/"Similar players", button text "Try an example".
- Produces: review screenshots and an updated README.

- [ ] **Step 1: Write `scripts/screenshot.py`**

```python
"""Review screenshots of the running Profiler (spec: 1200x750 at 2x).

1. Start the app against real BigQuery (gcloud ADC must be set up):
       .venv\\Scripts\\python -m streamlit run app.py --server.headless true --server.port 8599
2. In a second terminal, with an interpreter that has Playwright + Chromium:
       C:\\Python314\\python scripts\\screenshot.py
   Options: --url, --outfield "Mohamed Salah", --goalkeeper "Alisson", --out docs/screenshots

It also prints style checks (fonts loaded, card surface colour) and exits 1
when one fails, so a broken theme is caught before anyone looks.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
LOAD_MS = 180_000    # the first BigQuery load can be slow
SETTLE_MS = 2_500    # let Plotly and the data grid finish drawing
SEARCH_PLACEHOLDER = "Search a player, e.g. Salah"
SURFACE_RGB = "rgb(17, 26, 21)"  # theme.SURFACE #111a15


def settle(page: Page) -> None:
    page.wait_for_timeout(SETTLE_MS)


def open_app(page: Page, url: str) -> None:
    page.goto(url, wait_until="domcontentloaded")
    page.get_by_text("Try an example", exact=False).first.wait_for(timeout=LOAD_MS)
    settle(page)


def choose_player(page: Page, name: str) -> None:
    box = page.get_by_role("combobox", name=re.compile("Search player"))
    if box.count() == 0:
        box = page.get_by_text(SEARCH_PLACEHOLDER)
    box.first.click()
    page.keyboard.type(name, delay=40)
    page.get_by_role("option", name=re.compile(re.escape(name))).first.click()
    page.get_by_role("tab", name="Profile").wait_for(timeout=LOAD_MS)
    settle(page)


def scroll_to_top(page: Page) -> None:
    page.locator(".fl-hero").first.evaluate("el => el.scrollIntoView({block: 'start'})")
    page.wait_for_timeout(400)


def show_tab(page: Page, name: str) -> None:
    page.get_by_role("tab", name=name).click()
    page.get_by_role("tablist").first.evaluate("el => el.scrollIntoView({block: 'start'})")
    settle(page)


def style_checks(page: Page) -> list[str]:
    report = page.evaluate(
        """() => {
            const loaded = [...document.fonts].filter(f => f.status === 'loaded')
                .map(f => f.family.replace(/["']/g, ''));
            const card = document.querySelector('.stApp [class*="st-key-card"]');
            const hero = document.querySelector('.fl-hero-title');
            return {
                fonts: [...new Set(loaded)],
                heroFont: hero ? getComputedStyle(hero).fontFamily : null,
                cardBackground: card ? getComputedStyle(card).backgroundColor : null,
            };
        }"""
    )
    print("style report:", report)
    failures = []
    if "Barlow Condensed" not in report["fonts"]:
        failures.append("Barlow Condensed did not load")
    if "Inter" not in report["fonts"]:
        failures.append("Inter did not load")
    if report["cardBackground"] != SURFACE_RGB:
        failures.append(f"card background is {report['cardBackground']}, expected {SURFACE_RGB}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default="http://localhost:8599")
    parser.add_argument("--outfield", default="Mohamed Salah")
    parser.add_argument("--goalkeeper", default="Alisson")
    parser.add_argument("--out", default="docs/screenshots")
    args = parser.parse_args()

    out = ROOT / args.out
    out.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(viewport={"width": 1200, "height": 750}, device_scale_factor=2)
        page = context.new_page()

        open_app(page, args.url)
        page.screenshot(path=str(out / "01-empty.png"))

        choose_player(page, args.outfield)
        scroll_to_top(page)
        page.screenshot(path=str(out / "02-outfield-identity.png"))
        show_tab(page, "Profile")
        page.screenshot(path=str(out / "03-outfield-profile.png"))
        failures += style_checks(page)
        show_tab(page, "Stats")
        page.screenshot(path=str(out / "04-outfield-stats.png"))
        show_tab(page, "Similar players")
        page.screenshot(path=str(out / "05-outfield-similar.png"))

        open_app(page, args.url)  # fresh session for the goalkeeper
        choose_player(page, args.goalkeeper)
        scroll_to_top(page)
        page.screenshot(path=str(out / "06-gk-identity.png"))
        show_tab(page, "Profile")
        page.screenshot(path=str(out / "07-gk-profile.png"))
        show_tab(page, "Stats")
        page.screenshot(path=str(out / "08-gk-stats.png"))

        browser.close()

    for failure in failures:
        print("FAIL:", failure)
    print(f"saved 8 screenshots to {out}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Start the app against real BigQuery**

Run in a second terminal (leave it running): `.venv\Scripts\python -m streamlit run app.py --server.headless true --server.port 8599`
Expected: `You can now view your Streamlit app in your browser.` and `URL: http://localhost:8599`.

- [ ] **Step 3: Take the screenshots**

Run: `C:\Python314\python scripts\screenshot.py`
Expected: a `style report: {...}` line, no `FAIL:` lines, and `saved 8 screenshots to ...\docs\screenshots`. Exit code 0.

If a selector times out, Streamlit's DOM differs from what the script expects. Add `page.pause()` before the failing call, run again to open the Playwright inspector, and adjust only that locator.

If `card background` fails, the `st-key-card*` class sits on a different element than the bordered box. Inspect it and change only the selector in `theme.CSS` (still no `data-testid`). Rerun the tests and the script.

- [ ] **Step 4: Review every screenshot against this checklist (open each PNG)**

Check each item and fix anything that fails in the module that owns it. Rerun `.venv\Scripts\python -m pytest`, then Step 3.
- Hero title in condensed uppercase (Barlow Condensed), "Profiler" in lime `#b8f05a`, faint vertical stripe texture on the page.
- Sidebar is `#111a15` with uppercase muted section labels. The season pills show "All" plus short seasons ("2023-24"). The caption reads "Showing N players".
- Empty state: three numbered step cards of equal height and the lime "Try an example" button. No player is auto-selected.
- Identity card: initials disc, condensed name, chips (position in lime, then club, league, season, age, nation), and 6 equal-height KPI tiles in one row with a lime left edge.
- Profile: bars in the four green band shades, never red/amber. Each bar is labelled "value · 87th · Elite". There is a dotted line at 50 and the caption "vs N forwards in 2023-24, 450+ minutes". Cards have a dark surface, a 1px border and sharp 2px corners.
- Stats: category cards with the Stat / Total / Per 90 columns. Headers on `#17231c`, numbers right-aligned.
- Similar: readable headers, Similarity progress bars in lime, and the method caption.
- Goalkeeper: Save % / Goals against / 90 / Clean sheet % / Saves / 90 tiles, and one Goalkeeping card.
- No pink anywhere. Alerts (if any) use Streamlit's native colours.

- [ ] **Step 5: Update `README.md`**

Make five edits.

(a) Replace the whole `## App` section (from `## App` down to the `---` before `## Setup`) with:

~~~markdown
## App

`app.py` runs the **Football Player Profiler**, a Streamlit app that queries BigQuery live (results are cached for an hour).

- Sidebar filters: season (or all seasons), league and club
- Search for one player. Players with several seasons in view get a season switcher.
- Identity card with headline numbers: goals, assists, xG and xAG, or save %, goals against per 90 and clean-sheet % for keepers
- **Profile**: percentile bars against the same position and season (450+ minutes) on a curated metric list per position. Lower-is-better stats (errors, cards, goals against) are inverted.
- **Stats**: every stat by category, with the season total next to per 90
- **Similar players**: the similarity model's top matches. Select one to compare percentiles side by side and jump to their profile.

UI code lives in `src/ui/`: theme, charts, cached queries and views. Per-position metric lists, labels and number formats are all in `src/ui/metrics.py`, the one file to edit when the data source changes.

### Screenshots

![Empty state](docs/screenshots/01-empty.png)
![Forward profile](docs/screenshots/03-outfield-profile.png)
![Similar players](docs/screenshots/05-outfield-similar.png)
![Goalkeeper profile](docs/screenshots/07-gk-profile.png)

---
~~~

(b) After the `**Start the app**` code block in `## Setup`, add:

~~~markdown
**Run the tests** (no BigQuery needed: the data layer is stubbed)

```bash
.venv/Scripts/python -m pip install -r requirements-dev.txt
.venv/Scripts/python -m pytest
```

**Refresh the README screenshots** (needs the app running on port 8599 and Playwright)

```bash
.venv/Scripts/python -m streamlit run app.py --server.headless true --server.port 8599
C:/Python314/python scripts/screenshot.py
```
~~~

(c) In `## Project structure`, replace the code block with:

~~~markdown
```
scouting-report/
├── run_cleaning.py          # Pipeline entry point — cleans all datasets
├── gcp_loader.py            # Uploads to GCS, loads into BigQuery, rebuilds feature tables
├── app.py                   # Streamlit app shell (Football Player Profiler)
├── schema.sql               # Full BigQuery DDL (raw tables + feature tables + percentile views)
├── requirements.txt
├── requirements-dev.txt     # + pytest
├── .streamlit/config.toml   # Floodlight theme for native widgets
├── src/
│   ├── fbref_utils.py       # All cleaning, normalisation, and aggregation logic
│   ├── similarity.py        # Similarity model and BigQuery query functions
│   └── ui/                  # theme, charts, metric config, cached queries, views
├── tests/                   # pytest + Streamlit AppTest (BigQuery stubbed)
├── scripts/screenshot.py    # Playwright review screenshots
├── docs/screenshots/        # README images
├── cleaned_data/            # Output of run_cleaning.py (gitignored)
├── uncleaned_data_csv/      # Raw FBref CSV exports (gitignored)
└── data_html/               # Raw FBref HTML files for goalkeeping (gitignored)
```
~~~

(d) In the `## Stack` table, replace the row `| App | Streamlit |` with:

```markdown
| App | Streamlit |
| Charts | Plotly |
| Tests | pytest, Streamlit AppTest |
```

(e) Append at the end of the file:

```markdown

---

## Contributors

Thomas Tsangou and Ajaiah Darlington.
```

Then check that the old claim is gone:
Run: `Select-String -Path README.md -Pattern "raw or per-90 toggle"`
Expected: no output.

- [ ] **Step 6: Final verification**

Run: `.venv\Scripts\python -m pytest`
Expected: all passed, 0 failed.

Run: `git status --short`
Expected: only `README.md`, `scripts/screenshot.py` and `docs/screenshots/*.png` are listed. No changes to `schema.sql`, `gcp_loader.py` or `src/similarity.py`.

Run: `git diff --stat ca07b73 -- schema.sql gcp_loader.py src/similarity.py run_cleaning.py src/fbref_utils.py`
Expected: no output (those files are untouched since the spec commit).

- [ ] **Step 7: Commit (the 4 README images only)**

```powershell
git add scripts/screenshot.py README.md docs/screenshots/01-empty.png docs/screenshots/03-outfield-profile.png docs/screenshots/05-outfield-similar.png docs/screenshots/07-gk-profile.png
git commit -m "ui: Playwright review screenshots and README for the Profiler redesign" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Leave the other four PNGs uncommitted (or delete them). They are review-only.

- [ ] **Step 8: Hand back for push approval**

Don't push. Report the commit list (`git log --oneline ca07b73..HEAD`) and the screenshot paths to the controller. The spec's delivery step (push to `origin/main`, Coolify redeploys fae.camuedlabs.org) happens only after the user has reviewed the screenshots and says to push.

---

## Spec coverage map

| Spec item | Task |
|---|---|
| Bug 1: crash on empty filters | 8 (`main` empty state, `test_filters_with_no_players_show_an_empty_state`) |
| Bug 2: stateless season buttons | 8 (`picker.season_switch`, `test_season_switcher_defaults_to_latest_and_sticks`) |
| Bug 3: auto-selected first player | 8 (`index=None`, asserted in `test_no_selection_...`) |
| Bug 4: no lower-is-better inversion | 2 (`percentile_of`), 3 (`LOWER_IS_BETTER`, `profile_rows`) |
| Bug 5: NaN in denominator | 2 (`test_nan_rows_are_left_out_of_the_denominator`) |
| Bug 6: "1th" | 2 (`ordinal`, `pct_label`) |
| Bug 7: uncached queries, pool fetched twice | 4 (`data.py`), 8 (pool fetched once in `main`) |
| Bug 8: pink alerts | 5 (no alert CSS, tested) |
| Bug 9: `use_container_width` | 8 (`test_no_deprecated_use_container_width`) |
| Floodlight tokens, fonts, radius, config.toml, Dockerfile | 1, 5 |
| Cards via `st.container(border=True, key="card_*")` | 5 (CSS), 7 (views) |
| Structure: theme/viz/metrics/data/stats/views | 2-8 |
| Hero, sidebar, search, onboarding, switcher | 8 |
| Identity card + KPI rows (outfield/GK) | 3 (`kpi_tiles`), 7 (`header`) |
| Profile tab with pool caption and empty state | 7 |
| Stats tab (category cards, Total / Per 90) | 3 (`stat_table`), 7 |
| Similar tab (slider, ProgressColumn, single-row select, dumbbell, View profile, method line) | 7 |
| Footer with contributors | 8 |
| Data caching keys, same SQL | 4 |
| Charts (transparent, Inter, modebar off, `<extra></extra>`, GRID, fixedrange, 38px rows, line at 50) | 6 |
| Errors in expanders; empty states (filters, no selection, <450, BigQuery) | 5 (`error_box`), 7, 8 |
| pytest: percentile_of, band, ordinal, formatting, metrics vs schema.sql | 2, 3 |
| AppTest smoke: no selection / outfield / GK | 8 |
| Playwright screenshots 1200x750 @2x, empty state + each tab, outfield + GK | 9 |
| README: description, screenshots, toggle claim removed | 9 |

## Deviations from the spec (decided while planning)

1. **Stats tab number formats.** `st.column_config.NumberColumn` applies one format per column, and a category mixes counts (`%d`), xG decimals and percentages. So `Total` uses `format="localized"` on values pre-rounded per metric kind (`build.stat_table`), and `Per 90` uses `"%.2f"`. Percentage stats show "41.3" under Total, with the "%" carried by the label ("Shot Accuracy %"). Rates and tallies leave Per 90 blank. A column help tooltip explains this.
2. **`st.stop()` → early `return` from `main()`.** This lets the footer render on every path. Behaviour is otherwise the same.
3. **Search options are one row per player** (their latest season within the filters), labelled "Name — Club, League". A player-season row per option would repeat the same label for multi-season players under "All". The season switcher then picks among that player's seasons.
4. **Percentile method is mid-rank** (ties count half). It is NaN-safe and makes lower-is-better an exact mirror. Strict "share below" (the basketball helper) put zero-card players near the bottom.
5. **The optional pizza chart is not built** (YAGNI). The old unused `load_percentiles` (SQL views) is not ported.
6. **`std_progressive_passes` relabelled** "Progressive Passes (standard data)". It duplicated `pass_progressive_passes`'s label.
