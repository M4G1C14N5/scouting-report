"""Review screenshots of the running Scouting Hub app (spec: 1200x750 at 2x).

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
SURFACE_RGB = "rgb(22, 25, 28)"  # theme.SURFACE #16191c


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
    page.locator(".fl-hero").first.evaluate(
        "el => { for (let e = el; e; e = e.parentElement) e.scrollTop = 0; }")
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
