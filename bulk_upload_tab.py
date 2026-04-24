"""
Streamlit tab: bulk-upload event screenshots, extract via Claude, review
in an editable grid, and push to Smallworld.

Designed to piggyback on the existing "Push to Smallworld" tab for auth:
users sign in there, and this tab reads the same `sw_session` key from
session_state. This keeps the auth UX single-source and avoids the user
typing the Smallworld password twice.

Entry point: `render()`. Called from app.py.
"""

from __future__ import annotations

import io
import time
import traceback
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st
from PIL import Image

import bulk_upload
from smallworld_client import (
    EventDraft,
    SmallworldError,
    SmallworldSession,
    create_event,
    fetch_event_topics,
    upload_image,
)

# Keep this key in sync with smallworld_push.SS_SESSION so this tab sees the
# same login. Duplicated as a string here (rather than imported) to avoid an
# import cycle when smallworld_push someday imports from this module.
_SS_SW_SESSION = "sw_session"
_SS_SW_TOPICS = "sw_topics"

# This tab's own session-state keys. Namespaced with "bu_" so collisions with
# other tabs are impossible.
SS_EXTRACTED = "bu_extracted"          # List[Dict] — one per uploaded image
SS_THUMB_KEYS = "bu_thumb_keys"        # Dict[env, str]: cached placeholder S3 keys
SS_PUSH_LOG = "bu_push_log"            # List[Dict] — last push run's results

BASE_DIR = Path(__file__).parent
PLACEHOLDER_PATH = BASE_DIR / "assets" / "placeholder_event.png"

# Default end-time when the flyer doesn't state one. We still flag this as
# a concern so the user knows to double-check.
DEFAULT_DURATION = timedelta(hours=2)


# ---- Helpers -------------------------------------------------------------


def _parse_iso_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_hm_time(s: Optional[str]) -> Optional[dtime]:
    if not s:
        return None
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return None


def _combine_local(d: Optional[date], t: Optional[dtime]) -> Optional[datetime]:
    """Combine into a naive local datetime (TZ handled by smallworld_client on push)."""
    if d is None:
        return None
    return datetime.combine(d, t or dtime(hour=19))


def _sw_session() -> Optional[SmallworldSession]:
    sess = st.session_state.get(_SS_SW_SESSION)
    return sess if isinstance(sess, SmallworldSession) else None


def _sw_topics() -> Dict[str, int]:
    topics = st.session_state.get(_SS_SW_TOPICS) or {}
    return topics if isinstance(topics, dict) else {}


def _ensure_placeholder_thumb(session: SmallworldSession) -> str:
    """
    Upload the yellow placeholder PNG once per environment and cache the S3
    key in session_state. Every bulk-created event reuses the same thumbnail
    until a human replaces it in the main Smallworld CMS.
    """
    cache: Dict[str, str] = st.session_state.get(SS_THUMB_KEYS) or {}
    if session.env in cache and cache[session.env]:
        return cache[session.env]

    if not PLACEHOLDER_PATH.exists():
        raise RuntimeError(f"Missing placeholder thumbnail at {PLACEHOLDER_PATH}.")

    key = upload_image(session, PLACEHOLDER_PATH.read_bytes(), mime_type="image/png")
    cache[session.env] = key
    st.session_state[SS_THUMB_KEYS] = cache
    return key


# ---- Extraction ----------------------------------------------------------


def _run_extraction(
    files: List[Any],
    api_key: str,
    instructions: Dict[str, str],
) -> List[Dict[str, Any]]:
    """
    Extract every uploaded file. Streams progress into the UI.

    `instructions` maps filename → free-text guidance that gets passed
    through to Claude on both the extract and fact-check passes.
    """
    out: List[Dict[str, Any]] = []
    progress = st.progress(0.0, text=f"Extracting 0 / {len(files)}…")
    today = date.today()

    for i, f in enumerate(files):
        data = f.getvalue()
        mime = f.type or "image/png"
        extra = (instructions.get(f.name) or "").strip()
        try:
            info = bulk_upload.extract_and_factcheck(
                data,
                mime_type=mime,
                upload_date=today,
                api_key=api_key,
                user_instructions=extra,
            )
            info["_error"] = None
        except Exception as e:  # noqa: BLE001
            info = {
                "title": "",
                "description": "",
                "start_date": None,
                "start_time": None,
                "end_date": None,
                "end_time": None,
                "location": "",
                "concerns": [],
                "_error": f"{type(e).__name__}: {e}",
            }

        # UI-only fields
        info["_filename"] = f.name
        info["_mime"] = mime
        info["_bytes"] = data
        info["_include"] = info["_error"] is None
        info["_topic_id"] = None
        info["_user_instructions"] = extra

        out.append(info)
        progress.progress((i + 1) / len(files), text=f"Extracting {i + 1} / {len(files)}…")

    progress.empty()
    return out


# ---- Per-row edit UI -----------------------------------------------------


def _render_row(idx: int, info: Dict[str, Any], topics: Dict[str, int]) -> None:
    """Render one extracted-event row with editable fields."""
    label = info.get("title") or info.get("_filename") or f"Event #{idx + 1}"
    if not info.get("_include"):
        label = f"🚫 {label}"
    elif info.get("concerns"):
        label = f"⚠️ {label}"

    with st.expander(label, expanded=True):
        if info.get("_error"):
            st.error(f"Extraction failed: {info['_error']}")
            return

        # Echo the instructions the uploader gave Claude for this image
        # (if any) so it's easy to see at a glance what the LLM was told
        # to focus on.
        if info.get("_user_instructions"):
            st.caption(f"📝 Your instructions: _{info['_user_instructions']}_")

        # Concerns banner at the top so the reviewer sees them first.
        for c in info.get("concerns") or []:
            st.warning(c)

        col_img, col_fields = st.columns([1, 2])

        with col_img:
            try:
                img = Image.open(io.BytesIO(info["_bytes"]))
                st.image(img, use_container_width=True)
            except Exception:  # noqa: BLE001
                st.caption("(preview unavailable)")
            st.caption(info.get("_filename", ""))

        with col_fields:
            info["title"] = st.text_input(
                "Title",
                value=info.get("title", ""),
                key=f"bu_title_{idx}",
            )
            info["description"] = st.text_area(
                "Description",
                value=info.get("description", ""),
                key=f"bu_desc_{idx}",
                height=100,
            )

            c1, c2 = st.columns(2)
            with c1:
                d = _parse_iso_date(info.get("start_date"))
                info["_start_date"] = st.date_input(
                    "Start date",
                    value=d if d else None,
                    key=f"bu_sdate_{idx}",
                    format="YYYY-MM-DD",
                )
                t = _parse_hm_time(info.get("start_time"))
                info["_start_time"] = st.time_input(
                    "Start time",
                    value=t if t else dtime(hour=19),
                    key=f"bu_stime_{idx}",
                )
            with c2:
                ed = _parse_iso_date(info.get("end_date")) or _parse_iso_date(info.get("start_date"))
                info["_end_date"] = st.date_input(
                    "End date",
                    value=ed if ed else None,
                    key=f"bu_edate_{idx}",
                    format="YYYY-MM-DD",
                )
                et = _parse_hm_time(info.get("end_time"))
                if et is None and info.get("_start_time"):
                    # Default end = start + 2h, per the flag-it-and-flag-it spec.
                    st_dt = datetime.combine(date.today(), info["_start_time"])
                    et = (st_dt + DEFAULT_DURATION).time()
                info["_end_time"] = st.time_input(
                    "End time",
                    value=et if et else dtime(hour=21),
                    key=f"bu_etime_{idx}",
                )

            info["location"] = st.text_input(
                "Location",
                value=info.get("location", ""),
                key=f"bu_loc_{idx}",
            )

            # Topic picker — per row, as requested.
            if topics:
                topic_names = sorted(topics.keys())
                prev_topic_name = next(
                    (n for n, tid in topics.items() if tid == info.get("_topic_id")),
                    None,
                )
                default_idx = (
                    topic_names.index(prev_topic_name)
                    if prev_topic_name in topic_names
                    else 0
                )
                chosen = st.selectbox(
                    "Topic",
                    options=topic_names,
                    index=default_idx,
                    key=f"bu_topic_{idx}",
                )
                info["_topic_id"] = topics[chosen]
            else:
                st.info("Click **Load topics** above to populate the topic dropdown.")

            info["_include"] = st.checkbox(
                "Include in push",
                value=info.get("_include", True),
                key=f"bu_include_{idx}",
            )


# ---- Push ---------------------------------------------------------------


def _do_push(
    session: SmallworldSession,
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Bulk-uploaded events always go in as DRAFTS — no exceptions.

    These rows are LLM-extracted and need a human QA pass in the Smallworld
    CMS (pick a real thumbnail, sanity-check copy, etc.) before they go
    public. Hard-coding publish=False here rather than exposing a toggle
    removes the "oh I forgot to check the draft box" failure mode.
    """
    log: List[Dict[str, Any]] = []

    try:
        thumb_key = _ensure_placeholder_thumb(session)
    except Exception as e:  # noqa: BLE001
        st.error(f"Placeholder thumbnail upload failed: {e}")
        return log

    progress = st.progress(0.0, text=f"Pushing 0 / {len(rows)}…")
    for i, info in enumerate(rows):
        entry: Dict[str, Any] = {
            "title": info.get("title", ""),
            "status": "pending",
            "error": None,
            "event_id": None,
        }

        try:
            start_dt = _combine_local(info.get("_start_date"), info.get("_start_time"))
            if start_dt is None:
                raise ValueError("Start date is required.")
            end_dt = _combine_local(info.get("_end_date") or info.get("_start_date"),
                                    info.get("_end_time"))
            if end_dt is None:
                end_dt = start_dt + DEFAULT_DURATION

            topic_id = info.get("_topic_id")
            if not topic_id:
                raise ValueError("Topic is required.")

            draft = EventDraft(
                title=(info.get("title") or "").strip() or "(untitled event)",
                description=(info.get("description") or "").strip(),
                topic_id=int(topic_id),
                start_at=start_dt,
                end_at=end_dt,
                location=(info.get("location") or "").strip(),
                thumbnail_path=thumb_key,
            )
            # publish=False → event is created with status="DRAFT". A human
            # has to open it in the Smallworld CMS and hit Publish.
            resp = create_event(session, draft, publish=False)

            data = resp.get("data") if isinstance(resp, dict) else None
            event_id = None
            if isinstance(data, dict):
                event_id = data.get("id") or data.get("_id")
            elif isinstance(resp, dict):
                event_id = resp.get("id")

            entry["status"] = "ok"
            entry["event_id"] = event_id
        except SmallworldError as e:
            entry["status"] = "error"
            entry["error"] = f"{e} :: {(e.body or '')[:200]}"
        except Exception as e:  # noqa: BLE001
            entry["status"] = "error"
            entry["error"] = f"{type(e).__name__}: {e}"

        log.append(entry)
        progress.progress((i + 1) / len(rows), text=f"Pushing {i + 1} / {len(rows)}…")
        # Gentle pacing for the backend, mirroring smallworld_push.
        time.sleep(0.2)

    progress.empty()
    return log


# ---- Main render --------------------------------------------------------


def render() -> None:
    st.subheader("Bulk Upload from Screenshots")
    st.caption(
        "Drop event screenshots (Instagram, flyers, etc.). Claude extracts "
        "title, description, date/time, and location, then fact-checks "
        "itself. Review + edit below, then push to Smallworld. All bulk-"
        "uploaded events land as **drafts** — a human has to hit Publish "
        "in the main CMS after swapping in a real thumbnail. The "
        "destination environment (STG vs PROD) follows whichever env you "
        "signed into on the Push to Smallworld tab."
    )

    api_key = None
    if hasattr(st, "secrets"):
        api_key = st.secrets.get("ANTHROPIC_API_KEY")
    if not api_key:
        st.error(
            "Set `ANTHROPIC_API_KEY` in Streamlit Cloud → Settings → Secrets "
            "(or `.streamlit/secrets.toml` locally) to use this tab."
        )
        return

    # ---- Upload + extract -----------------------------------------------
    with st.container(border=True):
        files = st.file_uploader(
            "Drop event screenshots",
            type=["png", "jpg", "jpeg", "webp"],
            accept_multiple_files=True,
            key="bu_uploader",
        )

        # Per-image instructions. Filename-keyed text boxes so the uploader
        # can tell Claude what to focus on for each specific screenshot
        # (e.g. "Focus on the May 1 reception but mention the broader
        # exhibition in the description.") Both the extract pass and the
        # fact-check pass see the note, so the fact-checker flags when the
        # extractor didn't follow it.
        instructions: Dict[str, str] = {}
        if files:
            with st.expander(
                f"Optional instructions per image ({len(files)} file(s))",
                expanded=True,
            ):
                st.caption(
                    "Tell Claude what to focus on for each image. Skip any "
                    "where the default extraction is fine."
                )
                for f in files:
                    instructions[f.name] = st.text_area(
                        label=f.name,
                        key=f"bu_instr_{f.name}",
                        placeholder=(
                            "e.g. \"Focus on the May 1 reception 5–7pm, "
                            "but mention the broader exhibition in the "
                            "description.\""
                        ),
                        height=68,
                    )

        col_a, col_b = st.columns([1, 1])
        with col_a:
            if st.button(
                "Extract events",
                disabled=not files,
                type="primary",
                use_container_width=True,
            ):
                st.session_state[SS_EXTRACTED] = _run_extraction(
                    files or [], api_key, instructions
                )
                st.session_state[SS_PUSH_LOG] = []
        with col_b:
            if st.button(
                "Clear",
                disabled=not st.session_state.get(SS_EXTRACTED),
                use_container_width=True,
            ):
                st.session_state.pop(SS_EXTRACTED, None)
                st.session_state.pop(SS_PUSH_LOG, None)
                st.rerun()

    extracted: List[Dict[str, Any]] = st.session_state.get(SS_EXTRACTED) or []
    if not extracted:
        st.info("Upload one or more screenshots, then click **Extract events**.")
        return

    # ---- Auth + topic gate ----------------------------------------------
    session = _sw_session()
    if session is None:
        st.warning(
            "Sign in via the **Push to Smallworld** tab first — this tab "
            "reuses that session. Whichever environment (STG or PROD) you "
            "sign in to over there is where these events will land."
        )
        return

    # Env banner — big and color-coded so you can't miss it. PROD gets red,
    # STG gets blue. The destination env is whatever the `sw_session` was
    # created under, i.e. whatever you picked on the Push to Smallworld tab.
    env_label = session.env.upper()
    if session.env == "prod":
        st.error(
            f"⚠️ Destination: **PROD** — live Smallworld. "
            f"Events will land as **DRAFTS** and will not be visible to users "
            f"until someone clicks Publish in the Smallworld CMS. "
            f"Signed in as {session.email}."
        )
    else:
        st.info(
            f"🧪 Destination: **{env_label}** — staging environment. "
            f"Events will land as **DRAFTS**. "
            f"Signed in as {session.email}. To push to PROD instead, sign "
            f"out and sign back in on the Push to Smallworld tab with the "
            f"PROD environment selected."
        )

    topics = _sw_topics()
    col_t1, col_t2 = st.columns([3, 1])
    with col_t1:
        st.caption(
            f"Topics loaded: **{len(topics)}**"
            if topics
            else "Topics not loaded yet — click **Load topics** to populate the topic dropdown in each row."
        )
    with col_t2:
        if st.button("Load topics", use_container_width=True, key="bu_load_topics"):
            try:
                topics = fetch_event_topics(session)
                st.session_state[_SS_SW_TOPICS] = topics
            except SmallworldError as e:
                st.error(f"{e}: {(e.body or '')[:200]}")

    # ---- Per-row review -------------------------------------------------
    st.divider()
    st.markdown(f"**Review {len(extracted)} extracted event(s)**")
    for i, info in enumerate(extracted):
        _render_row(i, info, topics)

    # ---- Push ------------------------------------------------------------
    st.divider()
    included = [r for r in extracted if r.get("_include") and not r.get("_error")]
    missing_topic = [r for r in included if not r.get("_topic_id")]
    col_p1, col_p2 = st.columns([2, 1])
    with col_p1:
        st.caption(
            f"{len(included)} of {len(extracted)} row(s) included. "
            + (f"⚠️ {len(missing_topic)} missing a topic." if missing_topic else "")
        )
    with col_p2:
        do_push = st.button(
            f"Push as drafts to {env_label}",
            disabled=(not included) or bool(missing_topic),
            type="primary",
            use_container_width=True,
            key="bu_push",
        )

    if do_push:
        st.session_state[SS_PUSH_LOG] = _do_push(session, included)

    push_log: List[Dict[str, Any]] = st.session_state.get(SS_PUSH_LOG) or []
    if push_log:
        oks = sum(1 for e in push_log if e["status"] == "ok")
        st.markdown(f"**Push result: {oks} / {len(push_log)} succeeded**")
        for entry in push_log:
            if entry["status"] == "ok":
                st.success(f"✅ {entry['title']} — id {entry.get('event_id', '?')}")
            else:
                st.error(f"❌ {entry['title']} — {entry.get('error', 'unknown error')}")
