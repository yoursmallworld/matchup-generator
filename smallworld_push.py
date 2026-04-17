"""
Streamlit tab: push sports-game events from the scraped schedule into the
Smallworld backend (stg by default, prod behind an explicit confirmation).

This module exposes a single entry point, `render(...)`, that is called from
app.py inside `with tab5:`.

Safety posture (see worst-case risk analysis in the research memo):
  - stg is the default environment; prod requires typing the literal string
    "PROD" to enable pushing
  - A "Dry run (force DRAFT)" checkbox is on by default; unchecking it is
    required to actually publish
  - A one-row "Test push first" shortcut is provided and recommended before
    any batch push
"""

from __future__ import annotations

import io
import time
import traceback
from datetime import datetime, time as dtime, timedelta
from typing import Any, Callable, Dict, List, Optional

import streamlit as st

from smallworld_client import (
    ENVIRONMENTS,
    EventDraft,
    SmallworldError,
    SmallworldSession,
    create_event,
    fetch_event_topics,
    sign_in,
    upload_image,
)

# Keys used for st.session_state — prefixed to avoid collisions with other tabs
SS_SESSION = "sw_session"
SS_TOPICS = "sw_topics"
SS_PUSH_LOG = "sw_push_log"
SS_SELECTED_TOPIC = "sw_selected_topic"
SS_PROD_ACK = "sw_prod_ack"


# ---- Helpers --------------------------------------------------------------


def _parse_game_datetime(date_str: str, time_str: str) -> datetime:
    """
    Parse scraper date ("M/D/YYYY") + time ("7:00pm") into a local datetime.

    Falls back to date-only (midnight) if time can't be parsed.
    """
    # Normalize time: "7:00pm" -> "7:00 PM"
    t = (time_str or "").strip().upper().replace(" ", "")
    t = t.replace("AM", " AM").replace("PM", " PM").strip()
    for fmt in ("%I:%M %p", "%I %p", "%H:%M"):
        try:
            parsed_time = datetime.strptime(t, fmt).time()
            break
        except ValueError:
            continue
    else:
        parsed_time = dtime(hour=19)  # fallback: 7pm

    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            parsed_date = datetime.strptime(date_str, fmt).date()
            break
        except (ValueError, TypeError):
            continue
    else:
        parsed_date = datetime.now().date()

    return datetime.combine(parsed_date, parsed_time)


def _default_title(g: Dict[str, Any]) -> str:
    home, away = (
        (g["school"], g["opponent"])
        if g.get("home_away") == "Home"
        else (g["opponent"], g["school"])
    )
    return f"{g['gender']} {g['sport']}: {home} vs {away}"


def _default_description(g: Dict[str, Any]) -> str:
    return (
        f"{g['gender']} {g['sport']} — {g['school']} "
        f"{'hosts' if g.get('home_away') == 'Home' else 'travels to'} {g['opponent']}."
    )


def _image_bytes_for_game(
    g: Dict[str, Any],
    *,
    generate_matchup_graphic: Callable,
    team_name_to_id: Callable,
) -> bytes:
    """Generate the PIL matchup image for this game and return JPEG bytes."""
    home_name, away_name = (
        (g["school"], g["opponent"])
        if g.get("home_away") == "Home"
        else (g["opponent"], g["school"])
    )
    img = generate_matchup_graphic(
        team_name_to_id(home_name),
        team_name_to_id(away_name),
        g["sport"],
        g["date_sort"],
        gender=g["gender"],
    )
    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=90)
    return buf.getvalue()


# ---- Auth UI --------------------------------------------------------------


def _render_auth(env: str) -> None:
    session: Optional[SmallworldSession] = st.session_state.get(SS_SESSION)

    if session and session.env == env:
        col_a, col_b, col_c = st.columns([3, 1, 1])
        with col_a:
            st.success(
                f"Signed in as **{session.email}** on **{env.upper()}** "
                f"(token expires {session.expires_at.strftime('%H:%M UTC')})"
            )
        with col_b:
            if st.button("Sign out", key="sw_signout", use_container_width=True):
                for k in (SS_SESSION, SS_TOPICS, SS_SELECTED_TOPIC):
                    st.session_state.pop(k, None)
                st.rerun()
        with col_c:
            admin_url = ENVIRONMENTS[env]["admin_site"]
            st.link_button("Open CMS", admin_url, use_container_width=True)
        return

    # Not signed in (or env changed) — show the sign-in form
    if session and session.env != env:
        st.info(
            f"Signed in on {session.env.upper()} but {env.upper()} selected — "
            "sign in again to switch."
        )

    with st.form("sw_signin_form", clear_on_submit=False):
        email = st.text_input("Email", key="sw_email")
        password = st.text_input("Password", type="password", key="sw_pw")
        submitted = st.form_submit_button(f"Sign in to {env.upper()}")
        if submitted:
            if not email or not password:
                st.error("Email and password are required.")
                return
            try:
                new_session = sign_in(env, email, password)
                st.session_state[SS_SESSION] = new_session
                # Clear topics cache on new sign-in
                st.session_state.pop(SS_TOPICS, None)
                st.session_state.pop(SS_SELECTED_TOPIC, None)
                st.success("Signed in.")
                st.rerun()
            except SmallworldError as e:
                st.error(f"{e} — server said: {e.body[:300] if e.body else ''}")


# ---- Topic picker ---------------------------------------------------------


def _render_topic_picker(session: SmallworldSession) -> Optional[int]:
    topics: Dict[str, int] = st.session_state.get(SS_TOPICS) or {}

    col_a, col_b = st.columns([3, 1])
    with col_b:
        if st.button("🔄 Load topics", key="sw_load_topics", use_container_width=True):
            try:
                topics = fetch_event_topics(session)
                st.session_state[SS_TOPICS] = topics
            except SmallworldError as e:
                st.error(f"{e}: {e.body[:200] if e.body else ''}")
                return None

    if not topics:
        with col_a:
            st.caption("Click **Load topics** to fetch the topic list from the API.")
        return None

    names = sorted(topics.keys())
    # Try to default to anything that looks like Sports
    default_idx = 0
    for i, n in enumerate(names):
        if "sport" in n.lower():
            default_idx = i
            break

    prev = st.session_state.get(SS_SELECTED_TOPIC)
    if prev in names:
        default_idx = names.index(prev)

    with col_a:
        selected = st.selectbox(
            "Topic for these events",
            names,
            index=default_idx,
            key="sw_topic_select",
            help="All pushed events will use this topic.",
        )
    st.session_state[SS_SELECTED_TOPIC] = selected
    return topics[selected]


# ---- Grid builder ---------------------------------------------------------


def _build_grid_rows(games: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform scraper game dicts into the editable grid row shape."""
    rows = []
    for g in games:
        start_dt = _parse_game_datetime(g.get("date", ""), g.get("time", ""))
        rows.append(
            {
                "Push": False,
                "Title": _default_title(g),
                "Description": _default_description(g),
                "Start": start_dt,
                "Duration (hrs)": 2.0,
                "Location": g.get("venue", ""),
                "Hosted by": g["school"],
                "Details URL": g.get("game_url", ""),
                "Special instructions": "",
                # Hidden context columns used when we generate the image
                "_sport": g["sport"],
                "_gender": g["gender"],
                "_school": g["school"],
                "_opponent": g["opponent"],
                "_home_away": g.get("home_away", "Home"),
                "_date_sort": g.get("date_sort", ""),
            }
        )
    return rows


# ---- Push loop ------------------------------------------------------------


def _do_push(
    session: SmallworldSession,
    rows: List[Dict[str, Any]],
    topic_id: int,
    *,
    publish: bool,
    generate_matchup_graphic: Callable,
    team_name_to_id: Callable,
) -> List[Dict[str, Any]]:
    log: List[Dict[str, Any]] = []
    progress = st.progress(0.0, text="Starting push…")
    total = len(rows)

    for i, row in enumerate(rows):
        label = f"[{i + 1}/{total}] {row['Title'][:60]}"
        progress.progress(i / max(total, 1), text=label)
        entry: Dict[str, Any] = {
            "idx": i,
            "title": row["Title"],
            "status": "pending",
            "event_id": None,
            "error": None,
            "published": publish,
        }
        try:
            # Re-hydrate a pseudo-game dict for the image generator
            pseudo_game = {
                "school": row["_school"],
                "opponent": row["_opponent"],
                "sport": row["_sport"],
                "gender": row["_gender"],
                "home_away": row["_home_away"],
                "date_sort": row["_date_sort"],
            }
            image_bytes = _image_bytes_for_game(
                pseudo_game,
                generate_matchup_graphic=generate_matchup_graphic,
                team_name_to_id=team_name_to_id,
            )
            thumb_key = upload_image(session, image_bytes, mime_type="image/jpeg")

            duration_hrs = float(row.get("Duration (hrs)", 2.0) or 2.0)
            start_dt = row["Start"]
            if not isinstance(start_dt, datetime):
                start_dt = _parse_game_datetime(row.get("_date_sort", ""), "")
            start_dt = start_dt.replace(second=0, microsecond=0)
            end_dt = start_dt + timedelta(hours=duration_hrs)

            draft = EventDraft(
                title=row["Title"],
                description=row["Description"],
                topic_id=topic_id,
                start_at=start_dt,
                end_at=end_dt,
                location=row.get("Location", "") or "",
                hosted_by=row.get("Hosted by", "") or "",
                special_instructions=row.get("Special instructions", "") or "",
                thumbnail_path=thumb_key,
                details_url=row.get("Details URL", "") or "",
            )
            resp = create_event(session, draft, publish=publish)

            # Response shape: try a few likely paths to pull the id out
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
            entry["error"] = f"{type(e).__name__}: {e}\n{traceback.format_exc()[:400]}"

        log.append(entry)
        # Gentle client-side throttle to avoid bursting the API
        time.sleep(0.2)

    progress.progress(1.0, text=f"Done — {sum(1 for e in log if e['status'] == 'ok')}/{total} succeeded")
    return log


# ---- Main entry point -----------------------------------------------------


def render(
    *,
    games: List[Dict[str, Any]],
    generate_matchup_graphic: Callable,
    team_name_to_id: Callable,
) -> None:
    """Render the Push to Smallworld tab."""
    st.subheader("Push to Smallworld")
    st.caption(
        "Send selected games to the Smallworld backend as Events. "
        "Default environment is **STG** — the consumer site at "
        "smallworld-stg.web.app and the CMS at smallworld-stg-admin.web.app. "
        "Prod pushes require an explicit confirmation below."
    )

    # Environment toggle -----------------------------------------------------
    env = st.radio(
        "Environment",
        ["stg", "prod"],
        format_func=lambda e: e.upper(),
        horizontal=True,
        index=0,
        key="sw_env",
    )

    if env == "prod":
        st.warning(
            "⚠️ **PROD** pushes write to the live consumer site at "
            "yoursmallworld.com. Type **PROD** in the box below to enable."
        )
        prod_confirm = st.text_input(
            "Type PROD to enable production pushes",
            key="sw_prod_confirm",
            placeholder="PROD",
        )
        st.session_state[SS_PROD_ACK] = prod_confirm.strip() == "PROD"
        if not st.session_state[SS_PROD_ACK]:
            st.info("Prod is locked until you type PROD above.")

    st.divider()

    # Sign in ----------------------------------------------------------------
    _render_auth(env)
    session: Optional[SmallworldSession] = st.session_state.get(SS_SESSION)
    if not session or session.env != env:
        return

    st.divider()

    # Topic picker -----------------------------------------------------------
    topic_id = _render_topic_picker(session)
    if topic_id is None:
        return

    st.divider()

    # Games to push ----------------------------------------------------------
    if not games:
        st.info(
            "No games loaded yet — go to the **Upcoming Games** tab first "
            "and refresh the schedule."
        )
        return

    st.markdown(f"**{len(games)} games** available from the scraper. "
                "Edit any field below, then tick **Push** on the rows you want "
                "to send.")

    rows = _build_grid_rows(games)

    edited = st.data_editor(
        rows,
        column_config={
            "Push": st.column_config.CheckboxColumn("Push", default=False),
            "Title": st.column_config.TextColumn("Title", width="large"),
            "Description": st.column_config.TextColumn("Description", width="large"),
            "Start": st.column_config.DatetimeColumn(
                "Start",
                format="YYYY-MM-DD HH:mm",
                step=60,
            ),
            "Duration (hrs)": st.column_config.NumberColumn(
                "Duration (hrs)",
                min_value=0.5,
                max_value=12.0,
                step=0.5,
                format="%.1f",
            ),
            "Details URL": st.column_config.LinkColumn("Details URL"),
            # Hidden columns
            "_sport": None,
            "_gender": None,
            "_school": None,
            "_opponent": None,
            "_home_away": None,
            "_date_sort": None,
        },
        disabled=["_sport", "_gender", "_school", "_opponent", "_home_away", "_date_sort"],
        hide_index=True,
        use_container_width=True,
        num_rows="fixed",
        key="sw_grid",
    )

    selected = [r for r in edited if r.get("Push")]

    st.divider()

    # Push controls ----------------------------------------------------------
    col_a, col_b, col_c = st.columns([2, 1, 1])
    with col_a:
        dry_run = st.checkbox(
            "Dry run (force DRAFT — won't appear publicly)",
            value=True,
            key="sw_dry_run",
            help="Strongly recommended for a first pass. Leave ticked until "
                 "you've verified a drafted event in the CMS.",
        )
    with col_b:
        test_one = st.button(
            "Test push (1 row)",
            use_container_width=True,
            disabled=not selected,
            help="Pushes only the first selected row. Use this to verify "
                 "the wire format before a batch push.",
        )
    with col_c:
        push_all = st.button(
            "Push selected",
            type="primary",
            use_container_width=True,
            disabled=not selected or (env == "prod" and not st.session_state.get(SS_PROD_ACK)),
        )

    if not selected:
        st.caption("Tick the **Push** column on one or more rows to enable the push buttons.")

    # Execute push -----------------------------------------------------------
    rows_to_push = None
    if test_one and selected:
        rows_to_push = selected[:1]
    elif push_all and selected:
        rows_to_push = selected

    if rows_to_push:
        publish = not dry_run
        st.info(
            f"Pushing **{len(rows_to_push)}** event(s) to **{env.upper()}** "
            f"as **{'PUBLISHED' if publish else 'DRAFT'}**…"
        )
        try:
            log = _do_push(
                session,
                rows_to_push,
                topic_id,
                publish=publish,
                generate_matchup_graphic=generate_matchup_graphic,
                team_name_to_id=team_name_to_id,
            )
        except Exception as e:  # noqa: BLE001
            st.error(f"Push aborted: {e}")
            log = []

        # Append to the running session log so the user can see all pushes
        existing_log = st.session_state.get(SS_PUSH_LOG, [])
        st.session_state[SS_PUSH_LOG] = existing_log + log

    # Push log ---------------------------------------------------------------
    full_log = st.session_state.get(SS_PUSH_LOG, [])
    if full_log:
        st.divider()
        st.markdown("### Push log (this session)")
        st.dataframe(
            full_log,
            use_container_width=True,
            hide_index=True,
        )

        ok_count = sum(1 for e in full_log if e["status"] == "ok")
        err_count = sum(1 for e in full_log if e["status"] == "error")
        st.caption(f"{ok_count} ok · {err_count} error(s) · {len(full_log)} total")

        col_clear, col_csv = st.columns([1, 1])
        with col_clear:
            if st.button("Clear log", key="sw_clear_log"):
                st.session_state.pop(SS_PUSH_LOG, None)
                st.rerun()
        with col_csv:
            # Build a minimal CSV in-memory for download
            csv_lines = ["idx,title,status,event_id,published,error"]
            for e in full_log:
                csv_lines.append(
                    ",".join(
                        str(v).replace(",", ";").replace("\n", " ")
                        for v in [
                            e.get("idx"),
                            e.get("title", ""),
                            e.get("status", ""),
                            e.get("event_id") or "",
                            e.get("published"),
                            (e.get("error") or "")[:200],
                        ]
                    )
                )
            st.download_button(
                "Download log (CSV)",
                data="\n".join(csv_lines).encode(),
                file_name=f"smallworld_push_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True,
            )
