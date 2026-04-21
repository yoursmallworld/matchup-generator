"""
One-time helper: export your X session cookies so the GitHub Actions sweep
can scrape @ContraCostaFire and @CHP_ContraCosta on a daily cron.

Usage (run on your Mac, not in Actions):

    cd ~/Desktop/matchup-generator/streamlit-matchup
    pip install playwright
    playwright install chromium
    python tools/export_x_cookies.py

A real Chromium window will open. Log in to X if you aren't already. When
the home timeline is fully loaded, come back to this terminal and press
Enter. The script writes `x_storage.json` in the current directory.

Then:
    1. Open https://github.com/<you>/<repo>/settings/secrets/actions
    2. Create (or update) a secret named: X_COOKIES_JSON
    3. Paste the full contents of x_storage.json into the secret value.
    4. Delete x_storage.json locally — the secret is now the source of truth.

Cookies typically last ~30 days before X invalidates them. If the daily
workflow fails with "login_wall", re-run this script and update the secret.
"""

from __future__ import annotations

from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    raise SystemExit(
        "playwright is not installed. Run:\n"
        "  pip install playwright\n"
        "  playwright install chromium"
    )

OUTPUT = Path(__file__).resolve().parent.parent / "x_storage.json"


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.goto("https://x.com/home")

        print()
        print("A Chromium window is open.")
        print("  1. Log in to X if the page shows a login form.")
        print("  2. Wait until your home timeline loads (a few seconds).")
        print("  3. Then come back here and press Enter.")
        input("Press Enter to capture cookies... ")

        context.storage_state(path=str(OUTPUT))
        browser.close()

    print()
    print(f"Wrote session state to: {OUTPUT}")
    print()
    print("Next steps:")
    print("  1. Open the file and copy the entire JSON contents.")
    print("  2. In your GitHub repo, go to:")
    print("       Settings → Secrets and variables → Actions → New repository secret")
    print("  3. Name: X_COOKIES_JSON")
    print("     Value: paste the JSON")
    print("  4. Delete x_storage.json locally once the secret is saved.")


if __name__ == "__main__":
    main()
