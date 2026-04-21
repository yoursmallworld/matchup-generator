"""
PulsePoint incident sweep for the Concord News tab.

PulsePoint (pulsepoint.org) is the public-safety app that surfaces live
CAD/dispatch data for participating fire/EMS agencies. Contra Costa
County Fire Protection District (ConFire) is agency code 07090, which
covers all of Concord. This is where local reporters (including Concord
Patch) learn about fires and major incidents — usually before any press
release or @ContraCostaFire tweet exists.

Data flow
---------
1. GET https://api.pulsepoint.org/v1/webapp?resource=incidents&agencyid=07090
   (The older public endpoint was https://web.pulsepoint.org/DB/giba.php.
   PulsePoint migrated to api.pulsepoint.org in 2024-ish. Same encrypted
   payload shape, same decryption recipe — just a different URL.)
2. Response is an AES-256-CBC encrypted JSON blob ({ct, iv, s}) — no
   official public API, so this endpoint is effectively the one the
   PulsePoint web app itself uses.
3. Decrypt with the documented key-derivation recipe (MD5 EVP_BytesToKey,
   password = permuted substrings of "CommonIncidents" + hardcoded bits).
4. Parse `incidents.active` and `incidents.recent` arrays.
5. Filter to:
     a. Newsworthy call-type codes (fires, hazmats, explosions, major
        rescues, major traffic — see NEWSWORTHY_CODES below).
     b. Concord-area addresses (substring match on "concord" in the
        FullDisplayAddress, case-insensitive).
6. Write findings to cache/concord_news_pulsepoint.json using the same
   shape the Streamlit tab consumes.

Runs from GitHub Actions alongside the RSS and X sweeps. No auth, no
cookies — PulsePoint is reachable from datacenter IPs at the time of
writing. If decryption ever breaks (the key recipe has changed once or
twice historically), the scraper will fail loud and the workflow will
email Seth.

Caveats
-------
- TOS: PulsePoint has no public API; community convention is to keep
  request volume modest (we run once a day). We never republish their
  feed — we just surface relevant items inside a private dashboard.
- Per-incident deep links don't exist; all findings link to the live
  agency map. Good enough for situational awareness.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except ImportError:  # pragma: no cover
    print("requests is not installed. Run: pip install requests", file=sys.stderr)
    sys.exit(2)

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
except ImportError:  # pragma: no cover
    print(
        "cryptography is not installed. Run: pip install cryptography",
        file=sys.stderr,
    )
    sys.exit(2)

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE = REPO_ROOT / "cache" / "concord_news_pulsepoint.json"
TMP = CACHE.with_suffix(".json.tmp")

AGENCY_ID = "07090"  # Contra Costa County Fire Protection District
AGENCY_NAME = "ConFire Dispatch (PulsePoint)"
SOURCE_KEY = "pulsepoint_confire"

ENDPOINT = f"https://api.pulsepoint.org/v1/webapp?resource=incidents&agencyid={AGENCY_ID}"
AGENCY_PAGE = f"https://web.pulsepoint.org/?agencies={AGENCY_ID}"

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://web.pulsepoint.org/",
    "Accept": "application/json, text/plain, */*",
}

REQUEST_TIMEOUT_S = 20
MAX_FINDINGS = 200

# Full call-type map pulled from Davnit's reverse-engineered gist.
# Kept here (not fetched at runtime) so the sweep works offline and so
# new codes don't silently get tagged with stale labels.
CALL_TYPES: Dict[str, str] = {
    # Mutual aid / task force
    "AA": "Auto Aid", "MU": "Mutual Aid", "ST": "Strike Team/Task Force",
    # Aircraft
    "AC": "Aircraft Crash", "AE": "Aircraft Emergency",
    "AES": "Aircraft Emergency Standby", "LZ": "Landing Zone",
    # Alarms
    "AED": "AED Alarm", "OA": "Alarm", "CMA": "Carbon Monoxide",
    "FA": "Fire Alarm", "MA": "Manual Alarm", "SD": "Smoke Detector",
    "TRBL": "Trouble Alarm", "WFA": "Waterflow Alarm",
    # Service calls
    "FL": "Flooding", "LR": "Ladder Request", "LA": "Lift Assist",
    "PA": "Police Assist", "PS": "Public Service", "SH": "Sheared Hydrant",
    # Explosions
    "EX": "Explosion", "PE": "Pipeline Emergency", "TE": "Transformer Explosion",
    # Fires
    "AF": "Appliance Fire", "CHIM": "Chimney Fire", "CF": "Commercial Fire",
    "WSF": "Confirmed Structure Fire", "WVEG": "Confirmed Vegetation Fire",
    "CB": "Controlled Burn/Prescribed Fire", "ELF": "Electrical Fire",
    "EF": "Extinguished Fire", "FIRE": "Fire", "FULL": "Full Assignment",
    "IF": "Illegal Fire", "MF": "Marine Fire", "OF": "Outside Fire",
    "PF": "Pole Fire", "GF": "Refuse/Garbage Fire", "RF": "Residential Fire",
    "SF": "Structure Fire", "VEG": "Vegetation Fire", "VF": "Vehicle Fire",
    "WCF": "Working Commercial Fire", "WRF": "Working Residential Fire",
    # Other emergencies
    "BT": "Bomb Threat", "EE": "Electrical Emergency", "EM": "Emergency",
    "ER": "Emergency Response", "GAS": "Gas Leak",
    "HC": "Hazardous Condition", "HMR": "Hazmat Response",
    "TD": "Tree Down", "WE": "Water Emergency",
    # Investigations
    "AI": "Arson Investigation", "HMI": "Hazmat Investigation",
    "INV": "Investigation", "OI": "Odor Investigation", "SI": "Smoke Investigation",
    # Lockouts
    "LO": "Lockout", "CL": "Commercial Lockout",
    "RL": "Residential Lockout", "VL": "Vehicle Lockout",
    # Medical
    "IFT": "Interfacility Transfer", "ME": "Medical Emergency",
    "MCI": "Multi Casualty",
    # Natural disasters
    "EQ": "Earthquake", "FLW": "Flood Warning",
    "TOW": "Tornado Warning", "TSW": "Tsunami Warning",
    # Notifications
    "CA": "Community Activity", "FW": "Fire Watch", "NO": "Notification",
    "STBY": "Standby", "TEST": "Test", "TRNG": "Training", "UNK": "Unknown",
    # Rescues
    "AR": "Animal Rescue", "CR": "Cliff Rescue", "CSR": "Confined Space",
    "ELR": "Elevator Rescue", "RES": "Rescue", "RR": "Rope Rescue",
    "TR": "Technical Rescue", "TNR": "Trench Rescue",
    "USAR": "Urban Search and Rescue", "VS": "Vessel Sinking", "WR": "Water Rescue",
    # Traffic
    "TCE": "Expanded Traffic Collision", "RTE": "Railroad/Train Emergency",
    "TC": "Traffic Collision", "TCS": "Traffic Collision Involving Structure",
    "TCT": "Traffic Collision Involving Train",
    # Utilities
    "WA": "Wires Arcing", "WD": "Wires Down",
}

# Codes we actually surface. Tuned to catch anything a local reporter
# would write about, while skipping the high-volume medical and routine
# service calls. Tweak this set without touching the rest of the code.
NEWSWORTHY_CODES = frozenset({
    # Fires
    "AF", "CHIM", "CF", "WSF", "WVEG", "ELF", "FIRE", "IF", "MF", "OF",
    "PF", "GF", "RF", "SF", "VEG", "VF", "WCF", "WRF",
    # Explosions / transformer / pipeline
    "EX", "PE", "TE",
    # Hazmat / gas / significant hazard
    "HMR", "GAS", "HC",
    # Major rescues (operational — not lockouts, lift assists)
    "AR", "CR", "CSR", "ELR", "RES", "RR", "TR", "TNR", "USAR", "VS", "WR",
    # Serious traffic only (not every fender bender)
    "TCE", "TCS", "TCT", "RTE",
    # Aircraft
    "AC", "AE",
    # Bomb / multi-casualty / natural disaster
    "BT", "MCI", "EQ", "FLW", "TOW", "TSW", "FL",
    # Arson investigations (fire-adjacent news)
    "AI",
})

CITY_MATCH = "concord"  # lowercase substring match on FullDisplayAddress


# ---- decryption ---------------------------------------------------------


def _derive_password() -> str:
    """Reproduce the password the PulsePoint web app uses. Kept in a
    function (rather than a literal) so it's obvious this isn't a secret
    we invented — it's reverse-engineered from their client bundle and
    documented in Davnit's gist."""
    e = "CommonIncidents"
    return e[13] + e[1] + e[2] + "brady" + "5" + "r" + e.lower()[6] + e[5] + "gs"


def _derive_key(password: str, salt: bytes, key_len: int = 32) -> bytes:
    """OpenSSL EVP_BytesToKey with MD5. Loop until we have enough bytes."""
    key = b""
    block = b""
    pw = password.encode()
    while len(key) < key_len:
        hasher = hashlib.md5()
        if block:
            hasher.update(block)
        hasher.update(pw)
        hasher.update(salt)
        block = hasher.digest()
        key += block
    return key[:key_len]


def _decrypt(blob: Dict[str, str]) -> Any:
    ct = base64.b64decode(blob["ct"])
    iv = bytes.fromhex(blob["iv"])
    salt = bytes.fromhex(blob["s"])
    key = _derive_key(_derive_password(), salt)

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    plaintext = decryptor.update(ct) + decryptor.finalize()

    # The inner payload is a JSON *string* double-quoted inside another
    # layer, with escaped inner quotes. Slice off the wrapping quote
    # characters and unescape.
    try:
        trimmed = plaintext[1 : plaintext.rindex(b'"')].decode("utf-8")
    except ValueError as exc:
        raise RuntimeError("decryption produced non-string output") from exc
    inner = trimmed.replace(r'\"', '"')
    return json.loads(inner)


# ---- filtering + shaping ------------------------------------------------


@dataclass
class Incident:
    id: str
    call_type: str
    address: str
    received_iso: Optional[str]
    closed_iso: Optional[str]
    units: List[str]
    status: str  # "active" or "recent"


def _iso(ts: Optional[str]) -> Optional[str]:
    """PulsePoint timestamps are already ISO-8601 UTC ('2026-04-21T15:32:10Z').
    Normalize to the same format our other caches use."""
    if not ts:
        return None
    try:
        # '2026-04-21T15:32:10Z' → datetime
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    except ValueError:
        return ts  # let downstream sorting treat it as a string


def _parse_incidents(payload: Dict[str, Any]) -> List[Incident]:
    out: List[Incident] = []
    incidents = (payload or {}).get("incidents") or {}
    for status in ("active", "recent"):
        for raw in incidents.get(status) or []:
            unit_list = raw.get("Unit") or []
            units = sorted({
                (u.get("UnitID") or "").strip()
                for u in unit_list
                if u.get("UnitID")
            })
            out.append(Incident(
                id=str(raw.get("ID") or ""),
                call_type=(raw.get("PulsePointIncidentCallType") or "").strip().upper(),
                address=(raw.get("FullDisplayAddress") or "").strip(),
                received_iso=_iso(raw.get("CallReceivedDateTime")),
                closed_iso=_iso(raw.get("ClosedDateTime")),
                units=list(units),
                status=status,
            ))
    return out


def _is_relevant(inc: Incident) -> bool:
    if not inc.id or not inc.call_type:
        return False
    if inc.call_type not in NEWSWORTHY_CODES:
        return False
    if CITY_MATCH not in inc.address.lower():
        return False
    return True


def _format_address(addr: str) -> str:
    """Trim state/country tails for a tighter title — ', CA USA' etc."""
    out = addr
    for tail in (", USA", ", CA USA", ", CA"):
        if out.endswith(tail):
            out = out[: -len(tail)]
    return out.strip()


def _finding_id(inc: Incident) -> str:
    """Stable across active→recent transitions (same ID from PulsePoint)."""
    key = f"{SOURCE_KEY}|{inc.id}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def _build_finding(inc: Incident, fetched_at: str) -> Dict[str, Any]:
    label = CALL_TYPES.get(inc.call_type, inc.call_type)
    where = _format_address(inc.address)
    title = f"{label}: {where}" if where else label
    if inc.status == "active":
        title = f"[ACTIVE] {title}"

    summary_bits: List[str] = []
    if inc.units:
        unit_str = ", ".join(inc.units[:12])
        if len(inc.units) > 12:
            unit_str += f" (+{len(inc.units) - 12} more)"
        summary_bits.append(f"Units: {unit_str}")
    if inc.closed_iso:
        summary_bits.append(f"Cleared {inc.closed_iso}")
    summary = " · ".join(summary_bits) if summary_bits else "(no additional detail)"

    # Per-incident deep link. PulsePoint's web app reads `?agencies=` and
    # `?incident=` and opens the incident detail card directly — much more
    # useful than landing on the generic agency map.
    incident_url = f"https://web.pulsepoint.org/?agencies={AGENCY_ID}&incident={inc.id}"

    return {
        "id": _finding_id(inc),
        "title": title,
        "summary": summary,
        "url": incident_url,
        "source_key": SOURCE_KEY,
        "source_name": AGENCY_NAME,
        "published_at": inc.received_iso,
        "fetched_at": fetched_at,
        "raw_pub": None,
        "call_type": inc.call_type,
        "address": inc.address,
        "status": inc.status,
        "incident_id": inc.id,  # raw PulsePoint ID, kept for URL reconstruction
    }


# ---- I/O ----------------------------------------------------------------


def _load_existing() -> Dict[str, Any]:
    if not CACHE.exists():
        return {"fetched_at": None, "sources": {}, "findings": []}
    try:
        return json.loads(CACHE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"fetched_at": None, "sources": {}, "findings": []}


def _atomic_write(payload: Dict[str, Any]) -> None:
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    TMP.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(TMP, CACHE)


# ---- main ---------------------------------------------------------------


def _fetch_incidents() -> Tuple[List[Incident], Optional[str]]:
    """Returns (incidents, error_string_or_None)."""
    try:
        resp = requests.get(ENDPOINT, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT_S)
    except requests.RequestException as exc:
        return [], f"network: {exc}"

    if resp.status_code != 200:
        return [], f"http {resp.status_code}"

    try:
        blob = resp.json()
    except ValueError as exc:
        return [], f"non-json response: {exc}"

    try:
        payload = _decrypt(blob)
    except Exception as exc:  # noqa: BLE001 — surface the raw failure
        return [], f"decrypt failed: {type(exc).__name__}: {exc}"

    return _parse_incidents(payload), None


def main() -> int:
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    incidents, err = _fetch_incidents()
    sources_status: Dict[str, Any] = {
        SOURCE_KEY: {
            "name": AGENCY_NAME,
            "ok": err is None,
            "count": 0,
            "error": err,
        }
    }

    if err:
        # Persist the error status so the Streamlit Source Status
        # expander shows it, but don't wipe previously-cached findings.
        payload = _load_existing()
        payload["fetched_at"] = now_iso
        payload.setdefault("sources", {}).update(sources_status)
        _atomic_write(payload)
        print(f"[pulsepoint-sweep] ERROR {err}", file=sys.stderr)
        return 1

    relevant = [i for i in incidents if _is_relevant(i)]
    new_findings = [_build_finding(i, now_iso) for i in relevant]
    sources_status[SOURCE_KEY]["count"] = len(new_findings)

    payload = _load_existing()
    existing = {f.get("id"): f for f in payload.get("findings", [])}

    added = 0
    for f in new_findings:
        if f["id"] in existing:
            # Update in place so active→recent transitions refresh status,
            # units, closed_iso, and the incident URL (in case we're
            # backfilling older findings that were cached with a stale
            # URL schema) without creating duplicates.
            existing[f["id"]].update({
                "title": f["title"],
                "summary": f["summary"],
                "status": f.get("status"),
                "url": f.get("url"),
                "incident_id": f.get("incident_id"),
                # Keep the original fetched_at so age filtering in the UI
                # stays stable across runs.
            })
        else:
            existing[f["id"]] = f
            added += 1

    payload["findings"] = list(existing.values())
    payload["fetched_at"] = now_iso
    payload.setdefault("sources", {}).update(sources_status)

    # Newest first, by CallReceivedDateTime.
    payload["findings"].sort(
        key=lambda f: (f.get("published_at") or "", 0 if f.get("published_at") else 1),
        reverse=True,
    )
    payload["findings"] = payload["findings"][:MAX_FINDINGS]

    _atomic_write(payload)

    print(
        f"[pulsepoint-sweep] added={added} relevant={len(relevant)} "
        f"total={len(payload['findings'])} raw_incidents={len(incidents)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
