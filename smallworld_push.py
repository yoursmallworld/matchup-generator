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
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

import streamlit as st

from smallworld_client import (
    ENVIRONMENTS,
    EventDraft,
    SmallworldError,
    SmallworldSession,
    create_event,
    fetch_event_topics,
    list_all_admin_events,
    sign_in,
    upload_image,
)

# Keys used for st.session_state — prefixed to avoid collisions with other tabs
SS_SESSION = "sw_session"
SS_TOPICS = "sw_topics"
SS_PUSH_LOG = "sw_push_log"
SS_SELECTED_TOPIC = "sw_selected_topic"
SS_PROD_ACK = "sw_prod_ack"

# Remote-events cache — populated by a single GET /v1/admin/events call so the
# grid reflects what's already in the backend (pushed by anyone on the team,
# or created directly in the CMS), not just what this local install pushed.
SS_REMOTE_EVENTS = "sw_remote_events"     # dict: {env: [event, ...]}
SS_REMOTE_FETCHED_AT = "sw_remote_fetched_at"  # dict: {env: datetime}
SS_REMOTE_ERROR = "sw_remote_error"       # dict: {env: str}


# ---- Remote event matching ------------------------------------------------


def _event_title(event: Dict[str, Any]) -> str:
    """Pull the title out of a backend event, wherever it happens to live."""
    if not isinstance(event, dict):
        return ""
    # Matches the POST payload shape (content.title) plus common flat fallbacks.
    content = event.get("content")
    if isinstance(content, dict) and content.get("title"):
        return str(content["title"])
    return str(event.get("title") or event.get("name") or "")


def _event_start_date(event: Dict[str, Any]) -> Optional[str]:
    """Return the YYYY-MM-DD portion of the event's startAt, or None."""
    start = event.get("startAt") if isinstance(event, dict) else None
    if not start:
        return None
    try:
        # "2026-01-26T03:00:00.000Z" -> "2026-01-26". Good enough for same-day
        # matching; we also accept ±1 day to absorb UTC/PT boundary drift.
        return str(start)[:10]
    except Exception:  # noqa: BLE001
        return None


def _event_status(event: Dict[str, Any]) -> str:
    """Return 'PUBLISHED' / 'DRAFT' / '' from the event dict."""
    if not isinstance(event, dict):
        return ""
    status = event.get("status") or ""
    return str(status).upper()


def _dates_match(event_date: Optional[str], game_date: Optional[str]) -> bool:
    """Same day, or adjacent day (to handle UTC ↔ PT boundary crossings)."""
    if not event_date or not game_date:
        return False
    if event_date == game_date:
        return True
    try:
        ed = datetime.strptime(event_date, "%Y-%m-%d").date()
        gd = datetime.strptime(game_date, "%Y-%m-%d").date()
        return abs((ed - gd).days) <= 1
    except ValueError:
        return False


def _match_event_for_game(
    events: List[Dict[str, Any]], game: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Find a backend event that plausibly represents this scraped game.

    Heuristic (all case-insensitive, all substring-in-title):
      1. Same day (±1 for TZ boundary drift)
      2. Both school names appear in the title
      3. Sport appears in the title
      4. Gender appears in the title

    Requiring sport + gender prevents false positives when the same two
    schools play each other multiple times on one day — e.g. Friday double-
    headers with boys + girls basketball, or football + volleyball between
    the same rivals. Our own pushed titles always include gender + sport
    (format: "{gender} {sport}: {home} vs {away}"), so we never miss our
    own pushes; CMS-hand-created events need to follow the same convention
    to be detected.
    """
    school = (game.get("school") or "").strip().lower()
    opponent = (game.get("opponent") or "").strip().lower()
    sport = (game.get("sport") or "").strip().lower()
    gender = (game.get("gender") or "").strip().lower()
    game_date = game.get("date_sort") or ""
    if not school or not opponent or not game_date:
        return None

    candidates: List[Dict[str, Any]] = []
    for ev in events:
        if not _dates_match(_event_start_date(ev), game_date):
            continue
        title = _event_title(ev).lower()
        if not title:
            continue
        if school not in title or opponent not in title:
            continue
        # Require sport + gender when we have them, to disambiguate
        # same-day rematches in different sports / different genders.
        if sport and sport not in title:
            continue
        if gender and gender not in title:
            continue
        candidates.append(ev)

    if not candidates:
        return None
    # Prefer published > draft; break ties by most recent updatedAt/createdAt.
    def sort_key(ev: Dict[str, Any]):
        status_rank = 0 if _event_status(ev) == "PUBLISHED" else 1
        stamp = ev.get("updatedAt") or ev.get("createdAt") or ""
        return (status_rank, "" if stamp is None else str(stamp))

    candidates.sort(key=sort_key, reverse=True)
    # sort_key ranks PUBLISHED=0 first, so reverse=True would flip it —
    # recompute without reverse, picking min by status_rank then max by stamp.
    candidates.sort(key=lambda e: _event_status(e) != "PUBLISHED")
    return candidates[0]


def _format_relative_time(iso_str: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - dt.astimezone(timezone.utc)
        secs = delta.total_seconds()
        if secs < 60:
            return "just now"
        if secs < 3600:
            return f"{int(secs // 60)}m ago"
        if secs < 86400:
            return f"{int(secs // 3600)}h ago"
        return f"{int(secs // 86400)}d ago"
    except Exception:  # noqa: BLE001
        return ""


def _format_status_cell(event: Optional[Dict[str, Any]]) -> str:
    """Render the value shown in the 'Already on Smallworld' grid column."""
    if not event:
        return ""
    status = _event_status(event)
    label = "Published" if status == "PUBLISHED" else ("Draft" if status == "DRAFT" else status.title() or "Exists")
    # Use updatedAt if available, else createdAt, else blank.
    stamp = event.get("updatedAt") or event.get("createdAt") or ""
    rel = _format_relative_time(str(stamp)) if stamp else ""
    return f"{label} — {rel}" if rel else label


# ---- Remote events cache helpers -----------------------------------------


def _get_remote_events(env: str) -> List[Dict[str, Any]]:
    store = st.session_state.get(SS_REMOTE_EVENTS) or {}
    return store.get(env, [])


def _set_remote_events(env: str, events: List[Dict[str, Any]]) -> None:
    store = st.session_state.get(SS_REMOTE_EVENTS) or {}
    store[env] = events
    st.session_state[SS_REMOTE_EVENTS] = store
    fetched = st.session_state.get(SS_REMOTE_FETCHED_AT) or {}
    fetched[env] = datetime.now(timezone.utc)
    st.session_state[SS_REMOTE_FETCHED_AT] = fetched
    errs = st.session_state.get(SS_REMOTE_ERROR) or {}
    errs.pop(env, None)
    st.session_state[SS_REMOTE_ERROR] = errs


def _set_remote_error(env: str, msg: str) -> None:
    errs = st.session_state.get(SS_REMOTE_ERROR) or {}
    errs[env] = msg
    st.session_state[SS_REMOTE_ERROR] = errs


def _refresh_remote_events(session: SmallworldSession) -> None:
    """Fetch the latest events list from the backend and stash in session_state."""
    try:
        events = list_all_admin_events(session, max_pages=10, page_size=100)
        _set_remote_events(session.env, events)
    except SmallworldError as e:
        _set_remote_error(
            session.env,
            f"{e} — server said: {(e.body or '')[:200]}",
        )
    except Exception as e:  # noqa: BLE001
        _set_remote_error(session.env, f"{type(e).__name__}: {e}")


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


def _hosted_by_name(school: str) -> str:
    """
    Format the school name for the event's 'Hosted by' field.

    Scraper returns bare names like 'Concord' or 'Monte Vista'; the public
    event page should show 'Concord High School'. If the name already ends
    in 'High School' / 'HS' / 'Academy' / 'Prep' / etc., leave it alone.
    """
    if not school:
        return school
    lower = school.lower().strip()
    # Already includes a school-type suffix — don't double-tag
    suffixes = ("high school", "academy", "prep", "college prep", "school")
    if any(lower.endswith(s) for s in suffixes):
        return school
    # Short-form "HS" / "H.S." → expand to "High School"
    for short in (" H.S.", " H.S", " HS", " hs", " h.s.", " h.s"):
        if school.endswith(short):
            return school[: -len(short)] + " High School"
    return f"{school} High School"


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


def _build_grid_rows(
    games: List[Dict[str, Any]],
    remote_events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Transform scraper game dicts into the editable grid row shape.

    `remote_events` is the latest list pulled from GET /v1/admin/events; each
    game is matched against it to populate the 'Already on Smallworld' column.
    """
    rows = []
    for g in games:
        start_dt = _parse_game_datetime(g.get("date", ""), g.get("time", ""))
        match = _match_event_for_game(remote_events, g)
        rows.append(
            {
                "Push": False,
                "Already on Smallworld": _format_status_cell(match),
                "Title": _default_title(g),
                "Description": _default_description(g),
                "Start": start_dt,
                "Duration (hrs)": 2.0,
                "Location": g.get("venue", ""),
                "Hosted by": _hosted_by_name(g["school"]),
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

    # Fetch the remote events list on first render of this env so the grid
    # can show what's already on Smallworld (from anyone on the team, via
    # this tool OR the CMS). The user can refresh on demand.
    fetched_at_map = st.session_state.get(SS_REMOTE_FETCHED_AT) or {}
    last_fetched: Optional[datetime] = fetched_at_map.get(env)
    err_map = st.session_state.get(SS_REMOTE_ERROR) or {}
    last_error: Optional[str] = err_map.get(env)

    col_r1, col_r2 = st.columns([3, 1])
    with col_r2:
        refresh_clicked = st.button(
            "🔄 Refresh existing events",
            key="sw_refresh_remote",
            use_container_width=True,
            help=(
                "Re-fetch GET /v1/admin/events so the 'Already on Smallworld' "
                "column reflects anything pushed by teammates or created in "
                "the CMS since you opened this tab."
            ),
        )

    if refresh_clicked or (last_fetched is None and last_error is None):
        with st.spinner("Fetching existing events from Smallworld…"):
            _refresh_remote_events(session)
        last_fetched = (st.session_state.get(SS_REMOTE_FETCHED_AT) or {}).get(env)
        last_error = (st.session_state.get(SS_REMOTE_ERROR) or {}).get(env)

    remote_events = _get_remote_events(env)

    with col_r1:
        if last_error:
            st.error(
                f"Couldn't load existing events — the 'Already on Smallworld' "
                f"column will be blank until this is fixed.\n\n`{last_error}`"
            )
        elif last_fetched:
            st.caption(
                f"📡 Pulled **{len(remote_events)}** existing events from "
                f"{env.upper()} at "
                f"{last_fetched.strftime('%H:%M:%S UTC')} "
                f"({_format_relative_time(last_fetched.isoformat())})."
            )

    # Debug expander — surfaces what the GET /v1/admin/events call actually
    # returned, so we can tell whether a missing 'Already on Smallworld' marker
    # is a fetch issue (event not returned by the API, e.g. drafts filtered
    # out) or a match-heuristic issue (event returned but didn't match the
    # title/date checks).
    if remote_events:
        with st.expander(
            f"🔍 Debug: inspect the {len(remote_events)} events the API returned",
            expanded=False,
        ):
            # Summary counts by status
            status_counts: Dict[str, int] = {}
            for ev in remote_events:
                s = _event_status(ev) or "(none)"
                status_counts[s] = status_counts.get(s, 0) + 1
            st.caption(
                "Status breakdown: "
                + ", ".join(f"{s}={n}" for s, n in sorted(status_counts.items()))
            )
            # Compact table of the events
            compact = [
                {
                    "id": ev.get("id"),
                    "status": _event_status(ev),
                    "startAt (UTC date)": _event_start_date(ev),
                    "title": _event_title(ev),
                    "createdAt": ev.get("createdAt"),
                    "updatedAt": ev.get("updatedAt"),
                }
                for ev in remote_events[:50]
            ]
            st.dataframe(compact, use_container_width=True, hide_index=True)
            if len(remote_events) > 50:
                st.caption(f"Showing first 50 of {len(remote_events)}.")

    rows = _build_grid_rows(games, remote_events)

    # Summary caption above the grid
    pushed_count = sum(1 for r in rows if r["Already on Smallworld"])
    if pushed_count:
        st.caption(
            f"🗂 **{pushed_count}** of these {len(rows)} games already exist on "
            f"{env.upper()} (shown in the *Already on Smallworld* column). "
            "Anything created via the CMS or pushed by a teammate will appear here."
        )

    edited = st.data_editor(
        rows,
        column_config={
            "Push": st.column_config.CheckboxColumn("Push", default=False),
            "Already on Smallworld": st.column_config.TextColumn(
                "Already on Smallworld",
                width="small",
                help=(
                    "Whether a matching event already exists in the selected "
                    "environment — from this tool, a teammate, or a manual CMS "
                    "entry. Match is same-day + both team names in the title."
                ),
            ),
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
        disabled=[
            "Already on Smallworld",
            "_sport", "_gender", "_school", "_opponent", "_home_away",
            "_date_sort",
        ],
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

        # If anything succeeded, re-pull the remote events list so the grid
        # immediately reflects the newly created events (no manual refresh
        # needed, and teammates on other Streamlit instances will see them
        # the next time they click Refresh).
        if any(e["status"] == "ok" for e in log):
            with st.spinner("Refreshing existing-events list…"):
                _refresh_remote_events(session)

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
