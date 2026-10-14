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
