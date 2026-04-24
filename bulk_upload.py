"""
Bulk-upload extraction module.

Takes an event screenshot (bytes) and uses the Anthropic API to produce a
structured event record that can be pushed to Smallworld. A two-pass design
provides a fact-check loop — the second pass independently reviews the first
pass's output against the same image and emits concerns.

Public entry point:
    extract_and_factcheck(image_bytes, *, mime_type, upload_date, api_key) -> dict

Returned shape:
    {
        "title": str,
        "description": str,
        "start_date": str | None,   # "YYYY-MM-DD"
        "start_time": str | None,   # "HH:MM" 24h local
        "end_date":   str | None,
        "end_time":   str | None,
        "location":   str,
        "concerns":   List[str],    # QA notes from the fact-check pass
        "raw_extract": dict,        # pass-1 raw output, kept for debugging
    }

All string fields default to "" (never None) so the downstream Streamlit
grid doesn't have to special-case missing values. Date/time fields may be
None when the flyer doesn't supply them.

Cost: ~2 Claude Sonnet vision calls per image (~$0.01-0.03 per upload at
current pricing). Light enough that we don't bother rate-limiting here.
"""

from __future__ import annotations

import base64
import json
from datetime import date
from typing import Any, Dict, List, Optional

import anthropic


# Anthropic model — pinned to Claude Sonnet 4.6, the current flagship vision
# model. Bumping to opus-4-6 would roughly 3x the cost for marginal gains on
# this kind of structured extraction.
_MODEL = "claude-sonnet-4-6"

# Tool-use schema for the extract pass. Using a tool (rather than asking
# for JSON in prose) guarantees structured output — Claude is forced to
# call the tool with valid JSON matching the schema.
_EXTRACT_TOOL = {
    "name": "record_event",
    "description": (
        "Record the extracted event details from a flyer/screenshot. "
        "Leave a field blank if the image does not state it clearly — "
        "do not guess or hallucinate. Dates must be ISO YYYY-MM-DD and "
        "times must be 24-hour HH:MM."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "title": {
                "type": "string",
                "description": (
                    "Short, event-list-appropriate title (~80 chars max). "
                    "If the flyer has a headline or event name, use that verbatim."
                ),
            },
            "description": {
                "type": "string",
                "description": (
                    "1-3 sentence description of the event in plain prose. "
                    "Summarize the key details a reader would want: what, "
                    "who's hosting, cost/ticketing if mentioned, and any "
                    "notable context. Do not include the date/time/location "
                    "here — those have their own fields."
                ),
            },
            "start_date": {
                "type": "string",
                "description": (
                    "ISO date of the event start, YYYY-MM-DD. Leave blank "
                    "if the flyer doesn't state a specific date. If the "
                    "flyer only states a weekday (\"Saturday\"), leave blank "
                    "and note the ambiguity in a concern."
                ),
            },
            "start_time": {
                "type": "string",
                "description": (
                    "24-hour start time, HH:MM. Leave blank if not stated."
                ),
            },
            "end_date": {
                "type": "string",
                "description": "ISO end date, YYYY-MM-DD. Leave blank if not stated.",
            },
            "end_time": {
                "type": "string",
                "description": "24-hour end time, HH:MM. Leave blank if not stated.",
            },
            "location": {
                "type": "string",
                "description": (
                    "Venue name and/or address as stated on the flyer. "
                    "Include both when the flyer has both (e.g. "
                    "\"Todos Santos Plaza, 2161 Salvio St, Concord\")."
                ),
            },
        },
        "required": [
            "title",
            "description",
            "start_date",
            "start_time",
            "end_date",
            "end_time",
            "location",
        ],
    },
}

# Tool-use schema for the fact-check pass.
_FACTCHECK_TOOL = {
    "name": "report_concerns",
    "description": (
        "Report any inaccuracies, ambiguities, or missing information you "
        "notice when comparing the extracted event record against the "
        "source image. Return an empty list if the extraction looks clean."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "concerns": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "One short sentence per concern. Examples: "
                    "\"Year was not specified on the flyer — assumed current year.\", "
                    "\"Start time reads 5pm on the flyer but was extracted as 17:30.\", "
                    "\"No end time on the flyer.\"."
                ),
            }
        },
        "required": ["concerns"],
    },
}


# ---- Prompts -------------------------------------------------------------


def _extract_system_prompt(upload_date: date) -> str:
    return (
        "You are an event-details extractor. Given an event flyer or "
        "screenshot, call the `record_event` tool with the details. "
        "Rules:\n"
        "  1. Copy text from the image verbatim where possible — do not "
        "paraphrase the title.\n"
        "  2. Do not hallucinate missing fields. Leave them blank and "
        "the reviewer will fill them in.\n"
        "  3. Dates that are ambiguous (e.g. only a weekday, or no year) "
        "should be LEFT BLANK — a downstream fact-check pass will note the "
        "ambiguity.\n"
        f"  4. Today is {upload_date.isoformat()}; if a year is obvious "
        "from context but not stated (e.g. \"Nov 14\" in January and the "
        "event is clearly upcoming), you may infer the next occurrence, "
        "but only when you are confident.\n"
        "  5. Normalize times to 24-hour HH:MM. \"7 PM\" -> \"19:00\", "
        "\"noon\" -> \"12:00\".\n"
        "  6. Call the tool exactly once."
    )


def _factcheck_system_prompt() -> str:
    return (
        "You are an independent fact-checker. You'll see an event flyer "
        "and a JSON record of event details that another model extracted "
        "from it. Review the record against the image and call the "
        "`report_concerns` tool with any issues you find.\n\n"
        "Flag things like:\n"
        "  * Dates or times that don't match the flyer\n"
        "  * Locations that are too vague or inaccurate\n"
        "  * Titles that paraphrase rather than quote the flyer\n"
        "  * Missing end time / end date (note as a concern if absent)\n"
        "  * Missing year on date fields\n"
        "  * Any text that appears to have been hallucinated\n\n"
        "Be concise — one sentence per concern. Return an empty list if "
        "the extraction is accurate."
    )


# ---- Image helpers -------------------------------------------------------


def _b64(image_bytes: bytes) -> str:
    return base64.standard_b64encode(image_bytes).decode("ascii")


def _image_block(image_bytes: bytes, mime_type: str) -> Dict[str, Any]:
    return {
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": mime_type,
            "data": _b64(image_bytes),
        },
    }


def _tool_call_input(response: anthropic.types.Message, tool_name: str) -> Dict[str, Any]:
    """Pull the input dict out of the first tool_use block matching `tool_name`."""
    for block in response.content:
        if getattr(block, "type", None) == "tool_use" and block.name == tool_name:
            # block.input is already a dict per the SDK.
            return dict(block.input)
    # Fall through: try to parse any text block as JSON (defensive).
    for block in response.content:
        if getattr(block, "type", None) == "text":
            try:
                return json.loads(block.text)
            except (ValueError, AttributeError):
                pass
    raise RuntimeError(
        f"Claude did not call the {tool_name!r} tool. "
        f"Raw stop_reason={getattr(response, 'stop_reason', '?')}."
    )


# ---- Public API ----------------------------------------------------------


def extract_and_factcheck(
    image_bytes: bytes,
    *,
    mime_type: str = "image/png",
    upload_date: Optional[date] = None,
    api_key: str,
    user_instructions: str = "",
) -> Dict[str, Any]:
    """
    Run extract → fact-check. See module docstring for the return shape.
    Raises on API failure so the caller can surface a clean error per row.

    `user_instructions` is free-text guidance from the uploader (e.g.
    "Focus on the May 1 reception but mention the broader exhibit in the
    description.") It's injected into both the extract and fact-check
    passes so Claude follows the uploader's intent and the fact-checker
    flags deviations.
    """
    client = anthropic.Anthropic(api_key=api_key)
    upload_date = upload_date or date.today()
    instructions = (user_instructions or "").strip()

    extract_user_text = (
        "Extract the event details from this image by "
        "calling the record_event tool."
    )
    if instructions:
        extract_user_text += (
            "\n\n---\nAdditional instructions from the uploader "
            "(follow these carefully — they describe which event to focus "
            "on, what to emphasize in the description, etc.):\n"
            + instructions
        )

    # ---- Pass 1: extract -------------------------------------------------
    extract_resp = client.messages.create(
        model=_MODEL,
        max_tokens=1024,
        system=_extract_system_prompt(upload_date),
        tools=[_EXTRACT_TOOL],
        tool_choice={"type": "tool", "name": "record_event"},
        messages=[
            {
                "role": "user",
                "content": [
                    _image_block(image_bytes, mime_type),
                    {
                        "type": "text",
                        "text": extract_user_text,
                    },
                ],
            }
        ],
    )
    extracted = _tool_call_input(extract_resp, "record_event")

    factcheck_user_text = (
        "Here is the extracted event record. Compare it "
        "against the image and report any concerns.\n\n"
        "```json\n"
        + json.dumps(extracted, indent=2)
        + "\n```"
    )
    if instructions:
        factcheck_user_text += (
            "\n\n---\nThe uploader also provided these instructions for "
            "the extractor. Flag as a concern if the extraction does not "
            "appear to have followed them:\n"
            + instructions
        )

    # ---- Pass 2: fact-check (independent read of the image) --------------
    factcheck_resp = client.messages.create(
        model=_MODEL,
        max_tokens=512,
        system=_factcheck_system_prompt(),
        tools=[_FACTCHECK_TOOL],
        tool_choice={"type": "tool", "name": "report_concerns"},
        messages=[
            {
                "role": "user",
                "content": [
                    _image_block(image_bytes, mime_type),
                    {
                        "type": "text",
                        "text": factcheck_user_text,
                    },
                ],
            }
        ],
    )
    factcheck = _tool_call_input(factcheck_resp, "report_concerns")
    concerns: List[str] = [str(c).strip() for c in factcheck.get("concerns", []) if str(c).strip()]

    # ---- Merge -----------------------------------------------------------
    return {
        "title":       str(extracted.get("title") or "").strip(),
        "description": str(extracted.get("description") or "").strip(),
        "start_date":  (str(extracted.get("start_date") or "").strip() or None),
        "start_time":  (str(extracted.get("start_time") or "").strip() or None),
        "end_date":    (str(extracted.get("end_date") or "").strip() or None),
        "end_time":    (str(extracted.get("end_time") or "").strip() or None),
        "location":    str(extracted.get("location") or "").strip(),
        "concerns":    concerns,
        "raw_extract": extracted,
    }
