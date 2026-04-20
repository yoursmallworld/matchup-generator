"""
Smallworld API client for pushing events from the matchup generator.

Separates the wire protocol from the Streamlit UI so the push logic can be
scripted / unit-tested independently.

Shape of the admin events payload was captured from the stg CMS network traffic
(POST /v1/admin/events). See the bulk-event-upload-research memo for background.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

# ---- Environment config ---------------------------------------------------

ENVIRONMENTS: Dict[str, Dict[str, str]] = {
    "stg": {
        "api_base": "https://api-stg.yoursmallworld.com/api",
        # Firebase Web API keys are public — they go in the browser bundle.
        # This one was pulled straight from the stg site bundle.
        "firebase_api_key": "AIzaSyBxKL9gAe1UjRZ4q0_XJrx-1SymrwNYD1I",
        "admin_site": "https://smallworld-stg-admin.web.app",
        "consumer_site": "https://smallworld-stg.web.app",
    },
    "prod": {
        "api_base": "https://api.yoursmallworld.com/api",
        # Both stg and prod sites run in the same Firebase project
        # (smallworld-uat) so the same Web API key works for both.
        "firebase_api_key": "AIzaSyBxKL9gAe1UjRZ4q0_XJrx-1SymrwNYD1I",
        "admin_site": "https://smallworld-prd-admin.web.app",
        "consumer_site": "https://yoursmallworld.com",
    },
}

FIREBASE_SIGNIN = (
    "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
)
FIREBASE_REFRESH = "https://securetoken.googleapis.com/v1/token"


class SmallworldError(Exception):
    """Wraps an API error with status + response body for UI display."""

    def __init__(self, msg: str, status: Optional[int] = None, body: Optional[str] = None):
        super().__init__(msg)
        self.status = status
        self.body = body


# ---- Session --------------------------------------------------------------


@dataclass
class SmallworldSession:
    env: str
    email: str
    id_token: str
    refresh_token: str
    expires_at: datetime  # UTC, when id_token expires
    topics: Dict[str, int] = field(default_factory=dict)  # topic name -> id

    def api_base(self) -> str:
        return ENVIRONMENTS[self.env]["api_base"]

    def admin_site(self) -> str:
        return ENVIRONMENTS[self.env]["admin_site"]

    def consumer_site(self) -> str:
        return ENVIRONMENTS[self.env]["consumer_site"]


def sign_in(env: str, email: str, password: str) -> SmallworldSession:
    """Sign in via Firebase identitytoolkit. Returns a session on success."""
    if env not in ENVIRONMENTS:
        raise ValueError(f"Unknown env: {env}")
    api_key = ENVIRONMENTS[env]["firebase_api_key"]
    res = requests.post(
        f"{FIREBASE_SIGNIN}?key={api_key}",
        json={"email": email, "password": password, "returnSecureToken": True},
        timeout=15,
    )
    if res.status_code != 200:
        raise SmallworldError(
            f"Sign-in failed ({res.status_code})",
            status=res.status_code,
            body=res.text,
        )
    data = res.json()
    expires_in = int(data.get("expiresIn", 3600))
    return SmallworldSession(
        env=env,
        email=data.get("email", email),
        id_token=data["idToken"],
        refresh_token=data["refreshToken"],
        # Subtract 60s as a safety margin so we refresh slightly early
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=expires_in - 60),
    )


def refresh_if_needed(session: SmallworldSession) -> None:
    """Refresh id_token if it's within 5 min of expiry."""
    now = datetime.now(timezone.utc)
    if session.expires_at > now + timedelta(minutes=5):
        return
    api_key = ENVIRONMENTS[session.env]["firebase_api_key"]
    res = requests.post(
        f"{FIREBASE_REFRESH}?key={api_key}",
        data={"grant_type": "refresh_token", "refresh_token": session.refresh_token},
        timeout=15,
    )
    if res.status_code != 200:
        raise SmallworldError(
            f"Token refresh failed ({res.status_code})",
            status=res.status_code,
            body=res.text,
        )
    data = res.json()
    session.id_token = data["id_token"]
    session.refresh_token = data.get("refresh_token", session.refresh_token)
    session.expires_at = datetime.now(timezone.utc) + timedelta(
        seconds=int(data.get("expires_in", 3600)) - 60
    )


def _auth_headers(session: SmallworldSession) -> Dict[str, str]:
    refresh_if_needed(session)
    return {
        "Authorization": f"Bearer {session.id_token}",
        "x-client-platform": "web",
        "Content-Type": "application/json",
    }


# ---- Topics ---------------------------------------------------------------


def fetch_event_topics(session: SmallworldSession) -> Dict[str, int]:
    """GET /v1/topics?type=EVENT. Returns a dict of topic name -> id."""
    res = requests.get(
        f"{session.api_base()}/v1/topics",
        params={"type": "EVENT"},
        headers=_auth_headers(session),
        timeout=15,
    )
    if res.status_code != 200:
        raise SmallworldError(
            f"fetch_topics failed ({res.status_code})",
            status=res.status_code,
            body=res.text,
        )
    body = res.json()
    # Response shape varies slightly — handle both {data: [...]} and
    # {data: {items: [...]}} defensively.
    data = body.get("data", body)
    items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(items, list):
        items = []
    topics: Dict[str, int] = {}
    for it in items:
        if isinstance(it, dict) and "id" in it and "name" in it:
            topics[it["name"]] = it["id"]
    session.topics = topics
    return topics


# ---- Image upload (presigned URL -> S3) ----------------------------------


def upload_image(
    session: SmallworldSession,
    image_bytes: bytes,
    mime_type: str = "image/jpeg",
    entity: str = "users",
) -> str:
    """
    Two-step upload: ask the API for a presigned S3 URL, then PUT the bytes.
    Returns the S3 key (used as thumbnailPath on the event).

    `entity` is the StorageEntity. The admin CMS itself uses "users" even for
    event thumbnails, so we default to that to mirror the CMS exactly.
    """
    # The FE has a quirk where it converts "image/jpeg" to "image/jpg" when
    # asking for the presigned URL (but the S3 upload still goes as image/jpeg).
    presign_mime = "image/jpg" if mime_type == "image/jpeg" else mime_type

    res = requests.post(
        f"{session.api_base()}/v1/storage/presign-url",
        headers=_auth_headers(session),
        json={"mimeType": presign_mime, "entity": entity},
        timeout=15,
    )
    if res.status_code not in (200, 201):
        raise SmallworldError(
            f"presign failed ({res.status_code})",
            status=res.status_code,
            body=res.text,
        )
    body = res.json()
    url = body["data"]["url"]
    key = body["data"]["key"]

    # IMPORTANT: do NOT send our Authorization header to S3. The presigned URL
    # is self-authenticating; extra headers will cause SignatureDoesNotMatch.
    put = requests.put(
        url,
        data=image_bytes,
        headers={"Content-Type": mime_type},
        timeout=60,
    )
    if put.status_code not in (200, 204):
        raise SmallworldError(
            f"S3 upload failed ({put.status_code})",
            status=put.status_code,
            body=put.text,
        )
    return key


# ---- Event creation -------------------------------------------------------


def _iso_utc(dt: datetime) -> str:
    """Millisecond-precision UTC ISO string: 2026-04-17T12:34:56.000Z."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


@dataclass
class EventDraft:
    """
    All the fields needed to compose a POST /v1/admin/events body.
    Names mirror the wire format (camelCase) for easy debugging against the
    captured CMS traffic.
    """

    title: str
    description: str
    topic_id: int
    start_at: datetime
    end_at: datetime
    location: str = ""
    hosted_by: str = ""
    special_instructions: str = ""
    thumbnail_path: str = ""  # S3 key returned by upload_image
    details_url: str = ""
    file_description: str = ""
    layout_hierarchy: str = "MEDIUM"
    tickets_available: bool = False
    file_uploaded_paths: List[str] = field(default_factory=list)
    order_index: Optional[int] = None


def build_admin_event_payload(draft: EventDraft, *, publish: bool) -> Dict[str, Any]:
    """Assemble the exact POST /v1/admin/events body shape observed in the CMS."""
    publish_iso = _iso_utc(datetime.now(timezone.utc)) if publish else None
    return {
        "content": {
            "title": draft.title,
            "topicId": draft.topic_id,
            "thumbnailPath": draft.thumbnail_path,
            "layoutHierarchy": draft.layout_hierarchy,
        },
        "status": "PUBLISHED" if publish else "DRAFT",
        "publishedAt": publish_iso,
        "orderIndex": draft.order_index,
        "description": draft.description,
        "startAt": _iso_utc(draft.start_at),
        "endAt": _iso_utc(draft.end_at),
        "location": draft.location,
        "specialInstructions": draft.special_instructions,
        "fileDescription": draft.file_description,
        "ticketsAvailable": draft.tickets_available,
        "detailsUrl": draft.details_url,
        "fileUploadedPaths": draft.file_uploaded_paths,
        "hostedBy": draft.hosted_by,
    }


def create_event(
    session: SmallworldSession,
    draft: EventDraft,
    *,
    publish: bool = True,
) -> Dict[str, Any]:
    """POST /v1/admin/events. Returns the parsed response JSON on success."""
    payload = build_admin_event_payload(draft, publish=publish)
    res = requests.post(
        f"{session.api_base()}/v1/admin/events",
        headers=_auth_headers(session),
        json=payload,
        timeout=30,
    )
    if res.status_code not in (200, 201):
        raise SmallworldError(
            f"create_event failed ({res.status_code})",
            status=res.status_code,
            body=res.text,
        )
    return res.json()


def list_admin_events(
    session: SmallworldSession,
    *,
    page: int = 1,
    page_size: int = 100,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    GET /v1/admin/events — return one page of admin events.

    `status` filters by event status (e.g. "PUBLISHED", "DRAFT"). The admin
    endpoint defaults to published-only, so callers that want drafts must
    pass status="DRAFT" explicitly (see list_all_admin_events which fetches
    both statuses and merges).

    Path is inferred by symmetry with the POST endpoint and matches typical
    REST conventions; if the CMS uses a different path, this will need to be
    adjusted after capturing the real request. Response shape is handled
    defensively ({data: [...]} or {data: {items: [...]}}).
    """
    # Try a few common pagination param names; the API may ignore unknowns.
    params: Dict[str, Any] = {
        "page": page,
        "pageSize": page_size,
        "limit": page_size,
        "offset": (page - 1) * page_size,
    }
    if status:
        params["status"] = status
    res = requests.get(
        f"{session.api_base()}/v1/admin/events",
        headers=_auth_headers(session),
        params=params,
        timeout=20,
    )
    if res.status_code != 200:
        raise SmallworldError(
            f"list_admin_events failed ({res.status_code})",
            status=res.status_code,
            body=res.text,
        )
    body = res.json()
    data = body.get("data", body) if isinstance(body, dict) else body
    if isinstance(data, dict):
        items = data.get("items") or data.get("events") or data.get("results") or []
    elif isinstance(data, list):
        items = data
    else:
        items = []
    return items if isinstance(items, list) else []


def list_all_admin_events(
    session: SmallworldSession,
    *,
    max_pages: int = 10,
    page_size: int = 100,
    statuses: tuple = ("PUBLISHED", "DRAFT"),
) -> List[Dict[str, Any]]:
    """
    Fetch every event in `statuses` and merge the results.

    The admin endpoint filters to PUBLISHED by default (confirmed empirically
    against stg — drafts never appeared in the response). So to surface
    drafts in the 'Already on Smallworld' column, we have to request each
    status separately and combine. Deduped by id in case any event appears
    in more than one call (shouldn't happen, but cheap insurance).
    """
    out: List[Dict[str, Any]] = []
    seen_ids: set = set()
    for status in statuses:
        for page in range(1, max_pages + 1):
            items = list_admin_events(
                session, page=page, page_size=page_size, status=status
            )
            if not items:
                break
            # Guard against endpoints that ignore paging — dedupe by id and
            # stop once we stop gaining new events.
            new_count = 0
            for it in items:
                ident = it.get("id") if isinstance(it, dict) else None
                if ident is None:
                    # No stable id — just append, accept possible duplicates
                    out.append(it)
                    new_count += 1
                    continue
                if ident in seen_ids:
                    continue
                seen_ids.add(ident)
                out.append(it)
                new_count += 1
            if new_count == 0 or len(items) < page_size:
                break
    return out


def delete_event(session: SmallworldSession, event_id: str | int) -> None:
    """
    DELETE /v1/admin/events/{id}. Used for push-log rollback.
    NOTE: endpoint shape is inferred from REST conventions; if the CMS uses a
    different path, this will need adjustment after testing.
    """
    res = requests.delete(
        f"{session.api_base()}/v1/admin/events/{event_id}",
        headers=_auth_headers(session),
        timeout=15,
    )
    if res.status_code not in (200, 204):
        raise SmallworldError(
            f"delete_event failed ({res.status_code})",
            status=res.status_code,
            body=res.text,
        )
