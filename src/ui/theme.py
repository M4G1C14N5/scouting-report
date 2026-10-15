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
import os
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
.fl-kpi-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(92px, 1fr)); gap: .6rem;
               margin-bottom: 1rem; }}
.fl-kpi {{ background: {SURFACE}; border: 1px solid {BORDER}; border-left: 3px solid {ACCENT};
           border-radius: {RADIUS}; padding: .65rem .6rem; }}
.fl-kpi-label {{ display: flex; align-items: center; font-size: .7rem; text-transform: uppercase;
                 letter-spacing: .09em; color: {MUTED}; font-weight: 600; }}
.fl-kpi-value {{ font-family: {FONT_DISPLAY}; font-size: 1.75rem; font-weight: 700; line-height: 1.1;
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
    """Short readable error. The exception text (BigQuery errors can carry
    query text and service-account names) is shown only when PROFILER_DEBUG=1."""
    st.error(message)
    if os.environ.get("PROFILER_DEBUG") == "1":
        with st.expander("Error details"):
            st.code(f"{type(exc).__name__}: {exc}", language=None)
