"""Load the soccer-data warehouse into the Scouting Hub's BigQuery v2 tables.

    .venv\\Scripts\\python scripts\\load_soccer_data.py --dry-run   # read and count only
    .venv\\Scripts\\python scripts\\load_soccer_data.py             # write v2 tables and views

Needs duckdb (requirements-dev.txt) and, for the real load, gcloud ADC.
See src/soccer_data_loader.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.soccer_data_loader import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
