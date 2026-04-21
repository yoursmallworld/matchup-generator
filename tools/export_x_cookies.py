"""
One-time helper: export your X session cookies from your *real* browser
(Chrome, Arc, Brave, Edge, Firefox, or Safari) into the Playwright
storage-state format that tools/x_sweep.py consumes.

Why not log in through Playwright? X's anti-bot silently rejects
Playwright-launched browsers at login — no error, the page just loops
back. Reading cookies from the browser you're already logged into
avoids that entirely.

Usage (run on your Mac from inside the venv):

    source .venv/bin/activate
    pip install browser-cookie3
    python tools/export_x_cookies.py

Preconditions:
    1. You are already logged into X in your normal browser (Chrome,
       Arc, Brave, Firefox, Safari — anything browser_cookie3 supports).
    2. Ideally quit that browser first to avoid the cookie DB being
       locked; browser_cookie3 makes a copy so running is *usually*
       fine too.

After it runs, `x_storage.json` will be written next to this script's
project root. Paste its contents into the GitHub Secret
`X_COOKIES_JSON` and then delete the local file.

If no logged-in browser is found, the script lists what it tried so
you can switch browsers or log in and re-run.
"""

from __future__ import annotations

import json
import sys
from http.cookiejar import Cookie
from pathlib import Path
from typing import Iterable, List, Tuple

try:
    import browser_cookie3 as bc3
except ImportError:
    raise SystemExit(
        "browser-cookie3 is not installed. Inside your venv:\n"
        "  pip install browser-cookie3"
    )

OUTPUT = Path(__file__).resolve().parent.parent / "x_storage.json"

# Ordered list of loaders to try. First one with an auth_token wins.
LOADERS: List[Tuple[str, callable]] = [
    ("Chrome", bc3.chrome),
    ("Arc", getattr(bc3, "arc", None)),
    ("Brave", bc3.brave),
    ("Edge", bc3.edge),
    ("Firefox", bc3.firefox),
    ("Safari", bc3.safari),
    ("Chromium", bc3.chromium),
    ("Opera", bc3.opera),
]


def _load_domain(loader, domain: str):
    try:
        return loader(domain_name=domain)
    except Exception as exc:  # noqa: BLE001 — surface per-browser issues but keep trying others
        return exc


def _to_playwright_cookie(c: Cookie) -> dict:
    # SameSite isn't exposed directly on http.cookiejar.Cookie; peek into _rest.
    same_site = "Lax"
    try:
        raw = c._rest.get("SameSite") or c._rest.get("samesite")
        if raw:
            same_site = str(raw).capitalize()
            if same_site not in {"Strict", "Lax", "None"}:
                same_site = "Lax"
    except AttributeError:
        pass

    domain = c.domain or ""
    # Playwright wants a leading dot for domain cookies. Chrome stores them that
    # way already; Firefox does too. Normalize just in case.
    if c.domain_specified and not domain.startswith("."):
        domain = "." + domain

    expires = -1
    if c.expires:
        try:
            expires = int(c.expires)
        except (TypeError, ValueError):
            expires = -1

    return {
        "name": c.name,
        "value": c.value or "",
        "domain": domain,
        "path": c.path or "/",
        "expires": expires,
        "httpOnly": bool(c._rest.get("HttpOnly", False) if getattr(c, "_rest", None) else False),
        "secure": bool(c.secure),
        "sameSite": same_site,
    }


ALLOWED_DOMAIN_SUFFIXES = (".x.com", "x.com", ".twitter.com", "twitter.com")


def _is_x_domain(domain: str) -> bool:
    """True only if the cookie actually belongs to x.com or twitter.com.

    browser_cookie3's domain_name filter does a substring match, which
    wrongly grabs cookies for sites like netflix.com (ends in ...x.com)
    or adgrx.com. Filter strictly by domain component.
    """
    d = (domain or "").lower().lstrip(".")
    return d == "x.com" or d == "twitter.com" or d.endswith(".x.com") or d.endswith(".twitter.com")


def _gather_cookies(domains: Iterable[str]):
    tried: List[str] = []
    for name, loader in LOADERS:
        if loader is None:
            continue
        all_cookies: List[Cookie] = []
        for d in domains:
            result = _load_domain(loader, d)
            if isinstance(result, Exception):
                continue
            all_cookies.extend(list(result))
        # Strict domain filter — drop cross-site noise that substring-matched.
        all_cookies = [c for c in all_cookies if _is_x_domain(c.domain or "")]
        tried.append(f"{name} ({len(all_cookies)} cookies)")
        # Only accept a browser that has a valid auth_token.
        if any(c.name == "auth_token" and c.value for c in all_cookies):
            return name, all_cookies, tried
    return None, [], tried


def main() -> int:
    print("Looking for a browser where you're logged into X...")
    browser, cookies, tried = _gather_cookies([".x.com", "x.com", ".twitter.com", "twitter.com"])

    if not browser:
        print()
        print("Couldn't find a logged-in session in any supported browser.")
        print("Tried:")
        for line in tried:
            print("  -", line)
        print()
        print("Checklist:")
        print("  • Log into https://x.com/home in your regular browser.")
        print("  • On macOS, Chrome/Arc/Brave need Keychain access — macOS may")
        print("    prompt you to allow 'python' to access 'Chrome Safe Storage'.")
        print("    Click Always Allow. Then re-run this script.")
        print("  • If you use Firefox, make sure it's not currently running so the")
        print("    cookie DB isn't locked.")
        return 1

    # Dedupe by (name, domain, path) — keep the last occurrence.
    unique: dict = {}
    for c in cookies:
        key = (c.name, c.domain, c.path)
        unique[key] = c
    pw_cookies = [_to_playwright_cookie(c) for c in unique.values()]

    storage = {"cookies": pw_cookies, "origins": []}
    OUTPUT.write_text(json.dumps(storage, indent=2), encoding="utf-8")

    print(f"Found logged-in X session in: {browser}")
    print(f"Wrote {len(pw_cookies)} cookies to: {OUTPUT}")
    print()
    print("Next steps:")
    print("  1. Open the JSON file, copy its entire contents.")
    print("  2. In your GitHub repo: Settings → Secrets and variables → Actions")
    print("     → New repository secret → name: X_COOKIES_JSON → paste.")
    print("  3. Delete x_storage.json locally once the secret is saved.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
