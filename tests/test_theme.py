from __future__ import annotations

import contextlib
import re
import tomllib
from pathlib import Path

from src.ui import stats, theme

ROOT = Path(__file__).resolve().parents[1]


def test_tokens_are_charcoal_verbatim():
    assert (theme.PAGE, theme.SURFACE, theme.SURFACE_2, theme.BORDER) ==         ("#0e1012", "#16191c", "#1d2125", "#2a2f34")
    assert (theme.INK, theme.INK_2, theme.MUTED) == ("#eef0f2", "#b6bcc2", "#858c93")
    assert theme.GRID == "rgba(238,240,242,0.07)"
    assert (theme.ACCENT, theme.RULE, theme.PLAYER, theme.COMPARE) ==         ("#b8f05a", "#c9cfd4", "#5aa82c", "#7c7cf2")
    assert theme.PCT_RAMP == ("#4a4f54", "#767c82", "#7fa36a", "#b8f05a")
    assert theme.RADIUS == "2px"


def _rule(selector: str) -> str:
    """The body of the first CSS rule that starts with `selector`."""
    start = theme.CSS.index(selector + " {")
    return theme.CSS[start:theme.CSS.index("}", start)]


def test_accent_is_not_used_on_structural_chrome():
    # chalk (RULE) carries the structure: tile borders, section bars, hero rule,
    # avatar ring, position chip, step numbers
    assert theme.RULE in _rule(".fl-kpi") and theme.ACCENT not in _rule(".fl-kpi")
    assert theme.RULE in _rule(".fl-card-title::before") and theme.ACCENT not in _rule(".fl-card-title::before")
    assert theme.RULE in _rule(".fl-hero") and theme.ACCENT not in _rule(".fl-hero")
    assert theme.RULE in _rule(".fl-avatar") and theme.ACCENT not in _rule(".fl-avatar")
    assert theme.RULE in _rule(".fl-chip-accent") and theme.ACCENT not in _rule(".fl-chip-accent")
    assert theme.ACCENT not in _rule(".fl-step-num")


def test_accent_appears_only_on_the_brand_words():
    # lime in the injected CSS: the hero word and the sidebar brand word ("Hub"), nothing else
    lines = [ln for ln in theme.CSS.splitlines() if theme.ACCENT in ln]
    assert len(lines) == 2
    assert any(ln.startswith(".fl-hero-title em") for ln in lines)
    assert any(ln.startswith(".fl-brand em") for ln in lines)


def test_no_green_tint_left_in_the_css():
    assert "184,240,90" not in theme.CSS  # the old lime rgba tints
    assert "236,242,237" not in theme.CSS and "17,26,21" not in theme.CSS
    assert "repeating-linear-gradient" not in theme.CSS and "radial-gradient" not in theme.CSS


def test_streamlit_config_mirrors_the_tokens():
    cfg = tomllib.loads((ROOT / ".streamlit" / "config.toml").read_text(encoding="utf-8"))["theme"]
    assert cfg["primaryColor"] == theme.ACCENT
    assert cfg["linkColor"] == theme.RULE
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
    hero = theme.hero_html("Scouting", "Hub", "sub line", "meta line")
    assert "Scouting <em>Hub</em>" in hero and hero.count("<em>") == 1 and "sub line" in hero and "meta line" in hero


def test_steps_empty_state_footer_and_labels():
    steps = theme.steps_html([("One", "a"), ("Two", "b"), ("Three", "c")])
    assert ">01<" in steps and ">03<" in steps and "Three" in steps
    empty = theme.empty_state_html("Nothing here", "Try again")
    assert "fl-empty-title" in empty and "Try again" in empty
    footer = theme.footer_html([("Data", "FBref"), ("Built by", "Thomas Tsangou and Ajaiah Darlington")])
    assert "<strong>Built by</strong> Thomas Tsangou and Ajaiah Darlington" in footer
    assert "fl-label" in theme.section_label_html("Season")
    brand = theme.brand_html()
    assert "Scouting <em>Hub</em>" in brand and brand.count("<em>") == 1


def test_every_html_helper_is_a_single_line():
    outputs = [
        theme.kpi("A", "1", "s", "h"), theme.kpi_row_html([theme.kpi("A", "1")]),
        theme.card_html("T", "<p>x</p>", "h"), theme.identity_html("A B", [theme.chip("c")]),
        theme.hero_html("a", "b", "c", "d"), theme.brand_html(), theme.section_label_html("x"),
        theme.steps_html([("a", "b")]), theme.empty_state_html("a", "b"),
        theme.footer_html([("a", "b")]), theme.avatar_html("A B"), theme.band_key_html("n"),
    ]
    for html in outputs:
        assert "\n" not in html


def test_band_key_stacks_four_bands_best_first_with_bar_colours():
    key = theme.band_key_html("Counting stats are ranked per 90 minutes.")
    assert "\n" not in key and key.startswith('<div class="fl-band-key">')
    rows = re.findall(r'<div class="fl-band-row"><span class="fl-band-swatch" style="background:(#[0-9a-f]{6})">'
                      r'</span><span class="fl-band-label">([^<]+)</span>'
                      r'<span class="fl-band-range">([^<]+)</span></div>', key)
    assert rows == [
        (theme.PCT_RAMP[3], "Elite", "75th+"),
        (theme.PCT_RAMP[2], "Above avg", "50th-74th"),
        (theme.PCT_RAMP[1], "Below avg", "25th-49th"),
        (theme.PCT_RAMP[0], "Poor", "under 25th"),
    ]
    assert [label for _, label, _ in rows] == list(reversed(stats.BAND_LABELS))
    assert key.endswith('<div class="fl-band-note">Counting stats are ranked per 90 minutes.</div></div>')
    assert "fl-band-note" not in theme.band_key_html()
    assert "a &amp; b" in theme.band_key_html("a & b")


def test_band_key_css_is_caption_sized_and_muted():
    assert theme.MUTED in _rule(".fl-band-key") and "font-size: .78rem" in _rule(".fl-band-key")
    assert theme.INK_2 in _rule(".fl-band-label")
    assert theme.MUTED in _rule(".fl-band-note")
    assert f"border-radius: {theme.RADIUS}" in _rule(".fl-band-swatch")


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
