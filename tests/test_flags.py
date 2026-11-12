from __future__ import annotations

from src import soccer_data_loader as loader
from src.ui import flags, theme


def test_every_loader_nation_code_has_a_flag():
    missing = sorted(set(loader.NATION_CODES.values()) - set(flags.FIFA_TO_FLAG))
    assert missing == []


def test_flag_url_uses_flagcdn_and_the_uk_nations():
    assert flags.flag_url("ENG") == "https://flagcdn.com/w40/gb-eng.png"
    assert flags.flag_url("SCO") == "https://flagcdn.com/w40/gb-sct.png"
    assert flags.flag_url("KVX") == "https://flagcdn.com/w40/xk.png"
    assert flags.flag_url(" usa ") == "https://flagcdn.com/w40/us.png"
    assert flags.flag_url("Bonaire") is None and flags.flag_url(None) is None


def test_flag_chip_shows_flag_then_code():
    chip = theme.flag_chip("ENG")
    assert chip.startswith('<span class="fl-chip fl-chip-nation"><img class="fl-flag" '
                           'src="https://flagcdn.com/w40/gb-eng.png"')
    assert chip.endswith(">ENG</span>")
    assert theme.flag_chip("Bonaire") == '<span class="fl-chip fl-chip-nation">Bonaire</span>'


def test_avatar_puts_the_photo_over_the_initials():
    html = theme.avatar_html("Bukayo Saka", photo_url="https://media.api-sports.io/football/players/1460.png")
    assert ">BS<img " in html and 'data-initials="BS"' in html and 'alt=""' in html
    assert 'src="https://media.api-sports.io/football/players/1460.png"' in html
    assert "<img" not in theme.avatar_html("Bukayo Saka")
    assert "<img" not in theme.avatar_html("Bukayo Saka", photo_url="javascript:alert(1)")  # https only
    assert "\n" not in html


def test_broken_photo_falls_back_to_initials_in_css():
    assert ".fl-avatar img::before" in theme.CSS and "attr(data-initials)" in theme.CSS
