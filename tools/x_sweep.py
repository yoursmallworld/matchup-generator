"""
Headless X (Twitter) sweep for the Concord News tab.

Runs from GitHub Actions on a daily cron so the pipeline stays autonomous —
no Mac, no Chrome, no Claude desktop required.

Accounts scraped
----------------
- @ContraCostaFire   (Con Fire PIO)           → relevance-filtered
- @CHP_ContraCosta   (CHP Contra Costa)       → relevance-filtered
- @CHPAlerts         (CHP statewide alerts)   → relevance-filtered

Why a relevance filter
----------------------
All three accounts cover a wider area than Concord (the first two are
county-wide, @CHPAlerts is statewide), so unfiltered they'd flood the
UI with far-side incidents. The filter keeps only tweets whose text
mentions one of our KEYWORDS (case-insensitive). "contra costa" catches
county-wide alerts that don't name Concord specifically — important
for statewide-issued stuff like Amber/Ebony alerts where our region is
named at the county level.

Auth
----
X profile timelines are login-walled to logged-out viewers. We reuse a
logged-in session by loading a Playwright storage_state JSON written by
`tools/export_x_cookies.py`. The path to that file is read from
$X_STORAGE_STATE_PATH at runtime. In Actions the storage state comes from
the `X_COOKIES_JSON` repo secret, written to a temp file by the workflow.

If cookies are stale, we detect a login redirect, mark the source with
error="login_wall", and exit non-zero so the workflow surfaces a failure
email to Seth and he can refresh cookies.

Output
------
Writes `cache/concord_news_x.json` with the same payload shape the
Streamlit tab already reads (fetched_at, sources, findings[]).
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:  # pragma: no cover
    print(
        "playwright is not installed. Run: pip install playwright && playwright install chromium",
        file=sys.stderr,
    )
    sys.exit(2)

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE = REPO_ROOT / "cache" / "concord_news_x.json"
TMP = CACHE.with_suffix(".json.tmp")

# (source_key, source_name, profile_url, apply_relevance_filter)
ACCOUNTS: List[Tuple[str, str, str, bool]] = [
    ("contra_costa_fire_x", "Con Fire PIO (X)", "https://x.com/ContraCostaFire", True),
    ("chp_contra_costa_x", "CHP Contra Costa (X)", "https://x.com/CHP_ContraCosta", True),
    ("chp_alerts_x", "CHP Alerts (X)", "https://x.com/CHPAlerts", True),
]

KEYWORDS = {"concord", "contra costa"}
MAX_FINDINGS = 400
NAV_TIMEOUT_MS = 30_000
TWEET_WAIT_MS = 15_000
SETTLE_MS = 2_500


def _finding_id(source_key: str, url: str) -> str:
    key = f"{source_key}|{url}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def _split_title_summary(body: str) -> Tuple[str, str]:
    body = (body or "").strip()
    # Sentence break in the first ~140 chars → clean title/summary split.
    head = body[:140]
    dot = head.find(".")
    if dot != -1:
        title = body[: dot + 1].strip()
        summary = body[dot + 1 :].strip() or title
    else:
        title = body[:140].strip()
        summary = (body[140:].strip() or title)
    return title, summary[:500]


def _build_finding(
    source_key: str,
    source_name: str,
    body: str,
    href: str,
    ts: str | None,
    fetched_at: str,
) -> Dict[str, Any]:
    url = f"https://x.com{href}"
    title, summary = _split_title_summary(body)
    return {
        "id": _finding_id(source_key, url),
        "title": title,
        "summary": summary,
        "url": url,
        "source_key": source_key,
        "source_name": source_name,
        "published_at": ts,
        "fetched_at": fetched_at,
        "raw_pub": None,
    }


def _scrape(page, url: str) -> List[Dict[str, Any]]:
    """Navigate to the profile and return the rendered tweets.

    Raises RuntimeError with a readable code if we detect a login wall or
    the timeline never renders.
    """
    page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)

    # X redirects unauthenticated viewers to /i/flow/login or /login.
    current = page.url or ""
    if "/login" in current or "/i/flow/login" in current:
        raise RuntimeError("login_wall")

    try:
        page.wait_for_selector('article[data-testid="tweet"]', timeout=TWEET_WAIT_MS)
    except PWTimeout:
        # Could be slow render or a suspended account. Treat as empty.
        return []

    # Let a few more tweets paint in.
    page.wait_for_timeout(SETTLE_MS)

    tweets = page.evaluate(
        """
        () => {
          const out = [];
          document.querySelectorAll('article[data-testid="tweet"]').forEach((a) => {
            const textEl = a.querySelector('[data-testid="tweetText"]');
            const body = textEl ? textEl.innerText : null;
            const timeEl = a.querySelector('time');
            const ts = timeEl ? timeEl.getAttribute('datetime') : null;
            const linkEl = timeEl && timeEl.closest('a');
            const href = linkEl ? linkEl.getAttribute('href') : null;
            out.push({ ts, href, body });
          });
          return out;
        }
        """
    )
    return tweets or []


def _filter_to_author(tweets: List[Dict[str, Any]], handle: str) -> List[Dict[str, Any]]:
    """Drop retweets/promoted/quote-tweets from other authors."""
    prefix = f"/{handle}/"
    return [
        t for t in tweets
        if t.get("href", "").startswith(prefix) and t.get("body")
    ]


def _apply_relevance(tweets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        t for t in tweets
        if any(kw in (t.get("body") or "").lower() for kw in KEYWORDS)
    ]


def _load_existing() -> Dict[str, Any]:
    if not CACHE.exists():
        return {"fetched_at": None, "sources": {}, "findings": []}
    try:
        return json.loads(CACHE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"fetched_at": None, "sources": {}, "findings": []}


def _atomic_write(payload: Dict[str, Any]) -> None:
    TMP.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(TMP, CACHE)


def main() -> int:
    storage_state = os.environ.get("X_STORAGE_STATE_PATH")
    if not storage_state or not Path(storage_state).exists():
        print(
            "ERROR: X_STORAGE_STATE_PATH is unset or points at a missing file. "
            "Run tools/export_x_cookies.py locally and store the result in the "
            "X_COOKIES_JSON GitHub Secret.",
            file=sys.stderr,
        )
        return 2

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    sources_status: Dict[str, Dict[str, Any]] = {}
    new_findings: List[Dict[str, Any]] = []
    login_wall_hit = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state=storage_state)
        # Mild stealth so webdriver flag doesn't trip anti-bot heuristics.
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        page = context.new_page()

        for source_key, source_name, url, concord_filter in ACCOUNTS:
            handle = url.rstrip("/").split("/")[-1]
            try:
                raw = _scrape(page, url)
                by_author = _filter_to_author(raw, handle)
                filtered = _apply_relevance(by_author) if concord_filter else by_author
                for t in filtered:
                    new_findings.append(
                        _build_finding(
                            source_key,
                            source_name,
                            t["body"],
                            t["href"],
                            t.get("ts"),
                            now_iso,
                        )
                    )
                sources_status[source_key] = {
                    "name": source_name,
                    "ok": True,
                    "count": len(filtered),
                    "error": None,
                }
            except RuntimeError as exc:
                code = str(exc)
                if code == "login_wall":
                    login_wall_hit = True
                sources_status[source_key] = {
                    "name": source_name,
                    "ok": False,
                    "count": 0,
                    "error": code,
                }
            except Exception as exc:  # noqa: BLE001 — surface the rest
                sources_status[source_key] = {
                    "name": source_name,
                    "ok": False,
                    "count": 0,
                    "error": f"{type(exc).__name__}: {exc}",
                }

        browser.close()

    payload = _load_existing()
    existing_ids = {f.get("id") for f in payload.get("findings", [])}

    added = 0
    for f in new_findings:
        if f["id"] in existing_ids:
            continue
        payload.setdefault("findings", []).append(f)
        existing_ids.add(f["id"])
        added += 1

    payload["fetched_at"] = now_iso
    payload.setdefault("sources", {}).update(sources_status)

    # Newest first; items without published_at go to the bottom of their bucket.
    payload["findings"].sort(
        key=lambda f: (f.get("published_at") or "", 0 if f.get("published_at") else 1),
        reverse=True,
    )
    payload["findings"] = payload["findings"][:MAX_FINDINGS]

    _atomic_write(payload)

    err_count = sum(1 for s in sources_status.values() if not s["ok"])
    print(
        f"[x-sweep] added={added} total={len(payload['findings'])} "
        f"errors={err_count} sources={list(sources_status.keys())}"
    )

    # Fail the workflow if the session cookies are dead — that's the one
    # case that needs Seth to act.
    if login_wall_hit:
        print(
            "ERROR: X rejected the stored session (login wall). "
            "Re-run tools/export_x_cookies.py locally and update "
            "the X_COOKIES_JSON GitHub Secret.",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
