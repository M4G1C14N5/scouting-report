from __future__ import annotations

import contextlib
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


class _FakeSt:
    """Records the calls error_box makes on streamlit."""

    def __init__(self):
        self.calls = []

    def error(self, message):
        self.calls.append(("error", message))

    def expander(self, label):
        self.calls.append(("expander", label))
        return contextlib.nullcontext()

    def code(self, text, language=None):
        self.calls.append(("code", text))


def test_error_box_hides_exception_text_by_default(monkeypatch):
    monkeypatch.delenv("PROFILER_DEBUG", raising=False)
    st = _FakeSt()
    theme.error_box(st, "Couldn't load.", RuntimeError("SELECT secret FROM sa@proj.iam"))
    assert st.calls == [("error", "Couldn't load.")]


def test_error_box_shows_details_only_when_debugging(monkeypatch):
    monkeypatch.setenv("PROFILER_DEBUG", "1")
    st = _FakeSt()
    theme.error_box(st, "Couldn't load.", RuntimeError("boom"))
    assert ("expander", "Error details") in st.calls and ("code", "RuntimeError: boom") in st.calls
    monkeypatch.setenv("PROFILER_DEBUG", "0")
    off = _FakeSt()
    theme.error_box(off, "Couldn't load.", RuntimeError("boom"))
    assert off.calls == [("error", "Couldn't load.")]
