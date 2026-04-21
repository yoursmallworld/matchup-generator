"""
Streamlit tab: surface recent Concord news so Seth can tell at a glance
whether anything happened in town he should know about.

Data flow
---------
- `concord_news.py` fetches RSS sources (city, Patch, Claycord, Google News)
  into `cache/concord_news.json`. GitHub Actions runs this same module on
  a daily cron and commits the result back to the repo.
- `tools/x_sweep.py` (also on the Actions cron) scrapes @ContraCostaFire
  and @CHP_ContraCosta into `cache/concord_news_x.json`.
- A separate scheduled Claude-in-Chrome task writes Concord PD + City of
  Concord Facebook posts into `cache/concord_news_fb.json`. FB stays on
  the desktop because Meta rejects datacenter logins.
- This tab reads all three files, merges them, dedupes, sorts newest-first,
  and filters out items the user has dismissed.

Design posture
--------------
- Read-only UI. Refresh button re-runs the RSS fetcher inline (fast —
  6 HTTP calls). The FB and X caches are never fetched live from the tab
  — those require browsers the tab doesn't have.
- Dismissals are persisted to `cache/concord_news_dismissed.json` so
  they survive refreshes and app reboots.
- Nothing in this tab ever writes to the Smallworld backend. It's a
  read-only situational-awareness panel for now.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st

import concord_news

# session_state keys — prefixed to avoid collisions with the push tab
SS_CACHED = "cn_cached_payload"
SS_DISMISSED = "cn_dismissed_ids"
SS_LAST_REFRESH = "cn_last_refresh"
SS_REFRESH_ERROR = "cn_refresh_error"
SS_SHOW_DISMISSED = "cn_show_dismissed"
SS_WINDOW_HOURS = "cn_window_hours"

FB_CACHE_FILE = concord_news.CACHE_DIR / "concord_news_fb.json"
X_CACHE_FILE = concord_news.CACHE_DIR / "concord_news_x.json"
PULSEPOINT_CACHE_FILE = concord_news.CACHE_DIR / "concord_news_pulsepoint.json"

# Time-window options for "how far back to show". Default: 72 hours.
WINDOW_OPTIONS: Dict[str, Optional[int]] = {
    "Last 24 hours": 24,
    "Last 72 hours": 72,
    "Last 7 days": 24 * 7,
    "All": None,
}
DEFAULT_WINDOW = "Last 72 hours"


# ---- helpers -------------------------------------------------------------


def _load_json_cache(path: Path) -> Dict[str, Any]:
    empty = {"fetched_at": None, "sources": {}, "findings": []}
    if not path.exists():
        return empty
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return empty


def _load_all_findings() -> Dict[str, Any]:
    """Read the RSS, FB, X, and PulsePoint caches, merge them, return a
    combined payload."""
    rss = concord_news.load_cached() or {
        "fetched_at": None,
        "sources": {},
        "findings": [],
    }
    fb_payload = _load_json_cache(FB_CACHE_FILE)
    x_payload = _load_json_cache(X_CACHE_FILE)
    pp_payload = _load_json_cache(PULSEPOINT_CACHE_FILE)

    merged = (
        list(rss.get("findings", []))
        + list(fb_payload.get("findings", []))
        + list(x_payload.get("findings", []))
        + list(pp_payload.get("findings", []))
    )

    # Final cross-cache dedupe by URL → title.
    seen_urls: set = set()
    seen_titles: set = set()
    deduped: List[Dict[str, Any]] = []
    for f in merged:
        url_key = (f.get("url") or "").strip().lower()
        title_key = (f.get("title") or "").strip().lower()
        if url_key and url_key in seen_urls:
            continue
        if title_key and title_key in seen_titles:
            continue
        if url_key:
            seen_urls.add(url_key)
        if title_key:
            seen_titles.add(title_key)
        deduped.append(f)

    deduped.sort(
        key=lambda f: (f.get("published_at") or f.get("fetched_at") or "", 0 if f.get("published_at") else 1),
        reverse=True,
    )

    sources = dict(rss.get("sources") or {})
    sources.update(fb_payload.get("sources") or {})
    sources.update(x_payload.get("sources") or {})
    sources.update(pp_payload.get("sources") or {})

    return {
        "findings": deduped,
        "sources": sources,
        "rss_fetched_at": rss.get("fetched_at"),
        "fb_fetched_at": fb_payload.get("fetched_at"),
        "x_fetched_at": x_payload.get("fetched_at"),
        "pp_fetched_at": pp_payload.get("fetched_at"),
    }


def _human_timestamp(iso: Optional[str]) -> str:
    if not iso:
        return "never"
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return iso
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    local = dt.astimezone()  # local timezone of the Streamlit host
    return local.strftime("%b %d, %Y %I:%M %p %Z").strip()


def _human_published(iso: Optional[str]) -> str:
    if not iso:
        return "date unknown"
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return iso
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    local = dt.astimezone()
    now = datetime.now(local.tzinfo)
    delta = now - local
    if delta.total_seconds() < 60:
        return "just now"
    minutes = int(delta.total_seconds() // 60)
    if minutes < 60:
        return f"{minutes} min ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    if days < 14:
        return f"{days}d ago"
    return local.strftime("%b %d")


def _finding_timestamp(f: Dict[str, Any]) -> Optional[datetime]:
    """Best-effort parse of a finding's publish time. Falls back to
    fetched_at so cache items without a publish date don't vanish."""
    for key in ("published_at", "fetched_at"):
        raw = f.get(key)
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return None


def _within_window(f: Dict[str, Any], hours: Optional[int]) -> bool:
    """Show everything if hours is None (All). Otherwise drop items older
    than `hours`. Items with no parseable timestamp are shown (we'd rather
    over-include than hide something new that lacks a date)."""
    if hours is None:
        return True
    dt = _finding_timestamp(f)
    if dt is None:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    return dt >= cutoff


def _load_dismissed() -> set:
    if SS_DISMISSED in st.session_state:
        return st.session_state[SS_DISMISSED]
    ids = concord_news.load_dismissed_ids()
    st.session_state[SS_DISMISSED] = ids
    return ids


def _dismiss(finding_id: str) -> None:
    ids = _load_dismissed()
    ids.add(finding_id)
    st.session_state[SS_DISMISSED] = ids
    concord_news.save_dismissed_ids(ids)


def _undismiss(finding_id: str) -> None:
    ids = _load_dismissed()
    ids.discard(finding_id)
    st.session_state[SS_DISMISSED] = ids
    concord_news.save_dismissed_ids(ids)


def _clear_dismissed() -> None:
    st.session_state[SS_DISMISSED] = set()
    concord_news.save_dismissed_ids(set())


def _refresh_rss() -> None:
    try:
        concord_news.fetch_and_save()
        st.session_state[SS_LAST_REFRESH] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        st.session_state[SS_REFRESH_ERROR] = None
    except Exception as exc:  # noqa: BLE001 — surface any failure to the UI
        st.session_state[SS_REFRESH_ERROR] = f"{type(exc).__name__}: {exc}"


# ---- render -------------------------------------------------------------


def render() -> None:
    st.subheader("Concord News")
    st.caption(
        "Daily sweep of local news so you don't miss anything. "
        "**RSS** (City of Concord, Patch, Claycord, Google News), "
        "**X** (@ContraCostaFire, @CHP_ContraCosta, @CHPAlerts — filtered "
        "for Concord / Contra Costa mentions), and **PulsePoint** (live "
        "ConFire dispatch feed — filtered for newsworthy incidents inside "
        "Concord, same source Patch reporters watch) refresh automatically "
        "every morning on a GitHub Actions cron — no Mac required. "
        "**Facebook** (Concord PD, City of Concord) arrives via the "
        "Claude-in-Chrome task on days the Mac is awake. "
        "The button below re-runs RSS only; X, PulsePoint, and FB stay on "
        "their own schedules."
    )

    col_refresh, col_window, col_toggle, col_clear = st.columns([1, 1, 1, 1])
    with col_refresh:
        if st.button("Refresh RSS now", key="cn_refresh_btn"):
            with st.spinner("Fetching sources…"):
                _refresh_rss()
            st.rerun()
    with col_window:
        window_choice = st.selectbox(
            "Show",
            options=list(WINDOW_OPTIONS.keys()),
            index=list(WINDOW_OPTIONS.keys()).index(
                st.session_state.get(SS_WINDOW_HOURS, DEFAULT_WINDOW)
            ),
            key="cn_window_select",
        )
        st.session_state[SS_WINDOW_HOURS] = window_choice
    with col_toggle:
        st.session_state[SS_SHOW_DISMISSED] = st.toggle(
            "Show dismissed",
            value=st.session_state.get(SS_SHOW_DISMISSED, False),
            key="cn_show_dismissed_toggle",
        )
    with col_clear:
        if st.button("Clear all dismissals", key="cn_clear_dismissed_btn"):
            _clear_dismissed()
            st.rerun()

    err = st.session_state.get(SS_REFRESH_ERROR)
    if err:
        st.error(f"Last refresh failed — {err}")

    payload = _load_all_findings()
    findings = payload["findings"]
    dismissed = _load_dismissed()
    show_dismissed = st.session_state.get(SS_SHOW_DISMISSED, False)
    window_hours = WINDOW_OPTIONS[window_choice]

    in_window = [f for f in findings if _within_window(f, window_hours)]
    visible = [f for f in in_window if show_dismissed or f.get("id") not in dismissed]

    # Status line
    rss_ts = payload.get("rss_fetched_at")
    fb_ts = payload.get("fb_fetched_at")
    x_ts = payload.get("x_fetched_at")
    pp_ts = payload.get("pp_fetched_at")
    st.caption(
        f"RSS: **{_human_timestamp(rss_ts)}** · "
        f"X: **{_human_timestamp(x_ts)}** · "
        f"PulsePoint: **{_human_timestamp(pp_ts)}** · "
        f"FB: **{_human_timestamp(fb_ts)}** · "
        f"{len(visible)} of {len(in_window)} in window "
        f"({len(findings)} total, {len(dismissed)} dismissed)"
    )

    if not findings:
        st.info(
            "No cached findings yet. Click **Refresh RSS now** to fetch, or wait "
            "for the scheduled task to run."
        )
        return

    if not in_window:
        st.info(
            f"Nothing from the **{window_choice.lower()}** window. Widen the "
            "**Show** dropdown or dismiss fewer things."
        )
        return

    if not visible:
        st.info("Everything in this window is dismissed. Toggle **Show dismissed** to see them again.")
        return

    # Render each finding as a compact card
    for f in visible:
        fid = f.get("id") or ""
        title = f.get("title") or "(no title)"
        summary = f.get("summary") or ""
        url = f.get("url") or ""
        source_name = f.get("source_name") or f.get("source_key") or "Source"
        published = _human_published(f.get("published_at"))

        is_dismissed = fid in dismissed
        with st.container(border=True):
            title_col, dismiss_col = st.columns([10, 1])
            with title_col:
                if url:
                    st.markdown(f"**[{title}]({url})**")
                else:
                    st.markdown(f"**{title}**")
                st.caption(f"{source_name} · {published}")
                if summary:
                    st.write(summary)
            with dismiss_col:
                if is_dismissed:
                    if st.button("Undo", key=f"cn_undo_{fid}"):
                        _undismiss(fid)
                        st.rerun()
                else:
                    if st.button("Dismiss", key=f"cn_dismiss_{fid}"):
                        _dismiss(fid)
                        st.rerun()

    # Debug expander
    with st.expander("Source status"):
        sources = payload.get("sources") or {}
        if not sources:
            st.write("No source metadata — cache empty.")
        else:
            rows = []
            for key, info in sources.items():
                rows.append({
                    "source": info.get("name") or key,
                    "ok": info.get("ok"),
                    "count": info.get("count"),
                    "error": info.get("error") or "",
                })
            st.dataframe(rows, hide_index=True, use_container_width=True)
