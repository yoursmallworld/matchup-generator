"""
Concord News agent — scrapes a small set of Concord-area sources and writes
the results to cache/concord_news.json for the "Concord News" tab to read.

Design notes
------------
- Pure-Python module, no Streamlit dependency, so it can run inside the app
  or from a scheduled task (cron / Cowork scheduled-tasks / GitHub Actions).
- Sources are RSS feeds wherever possible — they're legal, reliable, and
  don't break on HTML redesigns the way scraping does.
- Facebook / Instagram are intentionally excluded: Meta's Graph API doesn't
  grant random apps read access to public feeds, CrowdTangle shut down in
  2024, and unofficial scrapers violate ToS and break constantly. PD cross-
  posts most of the same content to their city-hosted press release page,
  which we do cover.
- Output shape is intentionally flat so the Streamlit tab can render it
  without knowing anything about feed internals.

Usage
-----
    # one-shot fetch, prints a summary, writes cache/concord_news.json
    python -m concord_news

    # programmatic
    from concord_news import fetch_all, load_cached, CACHE_FILE
    findings = fetch_all()                   # fresh fetch
    cached   = load_cached()                 # whatever's on disk
"""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote_plus
from xml.etree import ElementTree as ET

import requests

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
CACHE_DIR = BASE_DIR / "cache"
CACHE_FILE = CACHE_DIR / "concord_news.json"
DISMISSED_FILE = CACHE_DIR / "concord_news_dismissed.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)
REQUEST_TIMEOUT = 15  # seconds
MAX_ITEMS_PER_SOURCE = 25
MAX_SUMMARY_CHARS = 400


@dataclass(frozen=True)
class Source:
    """One RSS/Atom feed we poll."""
    key: str        # short stable id (e.g. "city_press")
    name: str       # display name in the UI
    url: str
    # If True we only keep items whose title/summary actually mentions
    # Concord — useful for generic county or regional feeds.
    concord_filter: bool = False
    # Optional substring — if set, an item is only kept when this string
    # appears in the item URL. Used for Patch, whose Concord feed sneaks
    # in cross-promoted regional content ("across-ca", "napavalley", etc).
    url_must_contain: Optional[str] = None
    # Optional relevance filter — if set, require BOTH "concord" AND at
    # least one of these keywords to appear (lowercased) in title+summary.
    # Used for regional sources like Claycord that cover all of Contra
    # Costa but only occasionally produce Concord-specific content.
    # Keywords should be lowercased substrings.
    relevance_keywords: Tuple[str, ...] = ()


# Order matters only for display fallback — the UI sorts by date anyway.
SOURCES: Tuple[Source, ...] = (
    Source(
        key="city_press",
        name="City of Concord — Press Releases",
        url="https://www.cityofconcord.org/RSSFeed.aspx?ModID=1&CID=Press-Releases-11",
    ),
    Source(
        key="city_news",
        name="City of Concord — News & Announcements",
        url="https://www.cityofconcord.org/RSSFeed.aspx?ModID=1&CID=News-Announcements-5",
    ),
    Source(
        key="city_all_newsflash",
        name="City of Concord — All News Flash",
        url="https://www.cityofconcord.org/RSSFeed.aspx?ModID=1&CID=All-newsflash.xml",
    ),
    Source(
        key="city_alerts",
        name="City of Concord — Alerts",
        url="https://www.cityofconcord.org/RSSFeed.aspx?ModID=63&CID=All-0",
    ),
    Source(
        key="patch",
        name="Concord Patch",
        # Note: the "/feeds/california/concord" variant returns the all-of-California
        # feed; the "-ca" suffix is what pins it to Concord specifically. Even then,
        # the feed includes regional cross-promos — filter by URL slug to keep only
        # local stories.
        url="https://patch.com/feeds/california/concord-ca",
        url_must_contain="/concord-ca/",
    ),
    Source(
        key="claycord",
        name="Claycord",
        url="https://www.claycord.com/feed/",
        # Claycord covers all of Contra Costa and most of their daily output
        # is regional filler (BART, Bay Area freeways, gas prices, Oakland
        # stuff). We only care about Concord-specific items, and among those
        # only ones that look like police/fire/civic/local events — not
        # "best brunch in Concord" type listicles.
        relevance_keywords=(
            # Police / crime / safety
            "police", "officer", "arrest", "arrested", "suspect", "suspects",
            "shooting", "shooter", "shot", "homicide", "murder", "stabbing",
            "stabbed", "robbery", "robbed", "burglary", "burglar", "theft",
            "stolen", "assault", "gunman", "armed", "sentenced", "charged",
            "guilty", "indicted", "investigation", "investigating",
            "concord pd", "concord police", "pd says",
            # Fire / EMS / crashes
            "fire", "firefighter", "crash", "collision", "fatal",
            "con fire", "contra costa fire",
            # Civic / government
            "city council", "mayor", "city hall", "ordinance", "city of concord",
            # Concord-specific landmarks
            "todos santos", "concord pavilion", "monument blvd", "concord blvd",
            "galindo", "port chicago", "willow pass", "diamond blvd",
            "buchanan field", "concord naval", "naval weapons",
            "pixie playland", "waterworld",
            # Big civic events
            "ribbon-cutting", "ribbon cutting", "groundbreaking",
        ),
    ),
    Source(
        key="google_news",
        name="Google News — \"Concord, CA\"",
        url="https://news.google.com/rss/search?q=%22Concord%2C+CA%22&hl=en-US&gl=US&ceid=US:en",
        concord_filter=True,  # Google News catches unrelated Concords (NH, NC, MA)
    ),
)


@dataclass
class Finding:
    id: str
    title: str
    summary: str
    url: str
    source_key: str
    source_name: str
    published_at: Optional[str]  # ISO-8601 UTC string, or None
    fetched_at: str              # ISO-8601 UTC string
    raw_pub: Optional[str] = None  # original pubDate string, for debugging


# --------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------


def _http_get(url: str) -> Optional[bytes]:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/xml, text/xml, */*"},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code >= 400:
            return None
        return resp.content
    except requests.RequestException:
        return None


def _strip_html(s: str) -> str:
    if not s:
        return ""
    # Replace CDATA markers, then strip tags with a conservative regex.
    s = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", s, flags=re.DOTALL)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clip(s: str, n: int = MAX_SUMMARY_CHARS) -> str:
    if len(s) <= n:
        return s
    cut = s[:n]
    # break on last sentence-ish boundary
    for sep in (". ", "? ", "! "):
        idx = cut.rfind(sep)
        if idx > n * 0.6:
            return cut[:idx + 1] + " …"
    return cut.rstrip() + "…"


def _parse_date(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# RSS 2.0 uses no namespace; Atom uses a namespace. We handle both.
_ATOM_NS = "{http://www.w3.org/2005/Atom}"


def _find_items(root: ET.Element) -> List[ET.Element]:
    # RSS 2.0 → <channel><item>
    items = root.findall(".//item")
    if items:
        return items
    # Atom → <feed><entry>
    return root.findall(f".//{_ATOM_NS}entry")


def _elem_text(parent: ET.Element, tag: str) -> str:
    # Try plain tag first, then atom-namespaced
    for t in (tag, f"{_ATOM_NS}{tag}"):
        node = parent.find(t)
        if node is not None and (node.text or node.attrib):
            # Atom <link href="..."/> case
            if tag == "link" and "href" in node.attrib:
                return node.attrib["href"]
            return node.text or ""
    return ""


def _parse_feed(source: Source, xml_bytes: bytes) -> List[Finding]:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []

    findings: List[Finding] = []
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for item in _find_items(root)[:MAX_ITEMS_PER_SOURCE]:
        title_raw = _elem_text(item, "title")
        link_raw = _elem_text(item, "link")
        desc_raw = (
            _elem_text(item, "description")
            or _elem_text(item, "summary")
            or _elem_text(item, "content")
        )
        pub_raw = (
            _elem_text(item, "pubDate")
            or _elem_text(item, "updated")
            or _elem_text(item, "published")
        )

        title = _strip_html(title_raw)
        if not title:
            continue

        summary = _clip(_strip_html(desc_raw))
        url = (link_raw or "").strip()
        pub_dt = _parse_date(pub_raw)

        if source.concord_filter:
            blob = f"{title} {summary}".lower()
            # Very cheap gate: require Concord AND some California signal,
            # so we don't get Concord NH / MA / NC hits from Google News.
            has_concord = "concord" in blob
            has_ca = any(s in blob for s in (
                "concord, ca", "concord ca", "concord, california",
                "california", "bay area", "contra costa", "cityofconcord",
            ))
            if not (has_concord and has_ca):
                continue

        if source.url_must_contain and source.url_must_contain not in (url or ""):
            continue

        if source.relevance_keywords:
            blob = f"{title} {summary}".lower()
            if "concord" not in blob:
                continue
            if not any(kw in blob for kw in source.relevance_keywords):
                continue

        fingerprint = hashlib.sha1(
            f"{source.key}|{url or title}".encode("utf-8")
        ).hexdigest()[:16]

        findings.append(Finding(
            id=fingerprint,
            title=title,
            summary=summary,
            url=url,
            source_key=source.key,
            source_name=source.name,
            published_at=pub_dt.isoformat(timespec="seconds") if pub_dt else None,
            fetched_at=fetched_at,
            raw_pub=pub_raw or None,
        ))

    return findings


def fetch_source(source: Source) -> Tuple[List[Finding], Optional[str]]:
    """Fetch and parse one source. Returns (findings, error_message)."""
    data = _http_get(source.url)
    if data is None:
        return [], f"fetch failed ({source.url})"
    findings = _parse_feed(source, data)
    return findings, None


def fetch_all(sources: Iterable[Source] = SOURCES) -> Dict[str, Any]:
    """Fetch every source and return a summary dict ready to be JSON-dumped.

    The returned shape is:
        {
          "fetched_at": "2026-04-21T07:00:00+00:00",
          "sources": {
              "city_press": {"name": "...", "ok": true, "count": 3, "error": null},
              ...
          },
          "findings": [ Finding.dict, ... ]   # sorted newest first
        }
    """
    started = datetime.now(timezone.utc)
    per_source: Dict[str, Dict[str, Any]] = {}
    all_findings: List[Finding] = []

    for src in sources:
        findings, err = fetch_source(src)
        per_source[src.key] = {
            "name": src.name,
            "ok": err is None,
            "count": len(findings),
            "error": err,
        }
        all_findings.extend(findings)

    # Dedupe across sources by URL first, then by title fingerprint.
    seen_urls: set = set()
    seen_title_keys: set = set()
    deduped: List[Finding] = []
    for f in all_findings:
        url_key = f.url.strip().lower() if f.url else ""
        title_key = re.sub(r"\s+", " ", f.title.lower()).strip()
        if url_key and url_key in seen_urls:
            continue
        if title_key and title_key in seen_title_keys:
            continue
        if url_key:
            seen_urls.add(url_key)
        if title_key:
            seen_title_keys.add(title_key)
        deduped.append(f)

    # Sort: newest published first, undated fall to the end.
    def _sort_key(f: Finding) -> Tuple[int, str]:
        if f.published_at:
            return (0, f.published_at)  # dated items come first
        return (1, f.fetched_at or "")

    deduped.sort(key=_sort_key, reverse=True)
    # But we want newest-first among dated, which means we need to flip
    # the secondary key — easier done as a second pass:
    deduped = sorted(
        deduped,
        key=lambda f: (f.published_at or f.fetched_at or "", 0 if f.published_at else 1),
        reverse=True,
    )

    return {
        "fetched_at": started.isoformat(timespec="seconds"),
        "sources": per_source,
        "findings": [asdict(f) for f in deduped],
    }


# --------------------------------------------------------------------------
# Persistence
# --------------------------------------------------------------------------


def save_cache(payload: Dict[str, Any], path: Path = CACHE_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def load_cached(path: Path = CACHE_FILE) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def load_dismissed_ids(path: Path = DISMISSED_FILE) -> set:
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return set(data)
    except (OSError, json.JSONDecodeError):
        pass
    return set()


def save_dismissed_ids(ids: Iterable[str], path: Path = DISMISSED_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sorted(set(ids)), indent=2), encoding="utf-8")


def fetch_and_save() -> Dict[str, Any]:
    payload = fetch_all()
    save_cache(payload)
    return payload


# --------------------------------------------------------------------------
# CLI entry point for scheduled tasks
# --------------------------------------------------------------------------


def _main(argv: List[str]) -> int:
    payload = fetch_and_save()
    summary = payload["sources"]
    total = len(payload["findings"])
    print(f"Fetched {total} unique findings at {payload['fetched_at']}")
    for key, info in summary.items():
        state = "ok " if info["ok"] else "ERR"
        extra = f"  ({info['error']})" if info["error"] else ""
        print(f"  [{state}] {info['name']:45s} count={info['count']}{extra}")
    print(f"Cache written to {CACHE_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
