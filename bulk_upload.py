"""
Bulk-upload extraction module.

Takes an event screenshot (bytes) and uses the Anthropic API to produce a
structured event record that can be pushed to Smallworld. A three-pass
design provides a self-correcting loop: pass 1 extracts, pass 2
independently fact-checks against the same source and emits concerns,
and pass 3 (conditional) feeds those concerns back to the extractor so
the output record is actually corrected. If pass 2 is clean, pass 3 is
skipped.

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

Cost: 2-3 Claude Sonnet vision calls per image (pass 3 only runs on
flagged inputs). ~$0.01-0.04 per upload at current pricing. Light enough
that we don't bother rate-limiting here.
"""

from __future__ import annotations

import base64
import copy
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
            "details_url": {
                "type": "string",
                "description": (
                    "A URL the reader can visit for more info — event "
                    "page, RSVP form, Eventbrite link, Instagram post, "
                    "venue page, etc. Copy it verbatim. Leave blank if "
                    "the source doesn't show a clear link."
                ),
            },
            "hosted_by": {
                "type": "string",
                "description": (
                    "Who is hosting or presenting the event — the "
                    "organization, venue, brand, or individual listed "
                    "as host/presenter/organizer. Examples: "
                    "\"City of Concord\", \"Concord Art Association\", "
                    "\"Todos Santos Plaza\". Leave blank ONLY if there "
                    "is truly no identifiable host — most events should "
                    "have one (often the venue if nothing more specific "
                    "is listed)."
                ),
            },
            "special_instructions": {
                "type": "string",
                "description": (
                    "Anything a reader needs to know to attend that "
                    "doesn't fit elsewhere: RSVP requirements, age "
                    "restrictions, dress code, BYOB, parking notes, "
                    "rain plan, ticket price, \"free for members\", "
                    "\"registration required\", etc. Leave blank if "
                    "nothing notable. Do NOT repeat the date/time/"
                    "location — those have their own fields."
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
            "details_url",
            "hosted_by",
            "special_instructions",
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
        "  * A URL, host, or important instruction that the flyer shows "
        "but the extraction left blank\n"
        "  * A hosted_by field that is empty when the flyer or source "
        "clearly names a host, venue, or organizer\n"
        "  * A details_url that was hallucinated or doesn't match the "
        "source\n"
        "  * Any text that appears to have been hallucinated\n\n"
        "Be concise — one sentence per concern. Return an empty list if "
        "the extraction is accurate."
    )


def _revise_system_prompt(upload_date: date) -> str:
    return (
        "You are revising an event-details extraction. A first pass "
        "produced a `record_event` call from the source, and a fact-"
        "checker then flagged specific concerns with it. Your job is to "
        "emit a corrected `record_event` call that addresses those "
        "concerns using the original source as the source of truth.\n\n"
        "Rules:\n"
        "  1. Re-examine the source for each flagged field. Fix fields "
        "that are wrong; leave them blank if the source genuinely "
        "doesn't state them. Never guess or hallucinate.\n"
        "  2. Fields the fact-checker did NOT flag should be kept "
        "verbatim from the original extract unless you can see that "
        "they are clearly wrong too.\n"
        f"  3. Today is {upload_date.isoformat()}; same date/time "
        "formatting rules as the original extractor (ISO YYYY-MM-DD, "
        "24-hour HH:MM).\n"
        "  4. If a concern is about ambiguity that the source truly "
        "doesn't resolve (e.g. \"no year stated\"), leave the field "
        "blank rather than assuming.\n"
        "  5. Call the tool exactly once."
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


def _extract_tool_with_topics(topic_options: List[str]) -> Dict[str, Any]:
    """
    Return a copy of _EXTRACT_TOOL with a `topic_name` enum field appended.

    Using an enum (rather than a freeform string) guarantees Claude picks
    a value from the list — the tool call won't validate otherwise. This
    removes an entire class of "Claude returned a topic we can't resolve
    to a topic_id" failures.
    """
    tool = copy.deepcopy(_EXTRACT_TOOL)
    tool["input_schema"]["properties"]["topic_name"] = {
        "type": "string",
        "enum": list(topic_options),
        "description": (
            "The single best-matching topic for this event from the "
            "provided list. Pick the option that most accurately "
            "categorizes the event's primary purpose — e.g. an art "
            "reception is Culture, a job fair is Community, a road race "
            "is Sports. Choose the closest match even if none is a "
            "perfect fit."
        ),
    }
    tool["input_schema"]["required"].append("topic_name")
    return tool


def _run_pipeline(
    client: anthropic.Anthropic,
    source_blocks: List[Dict[str, Any]],
    *,
    source_word: str,
    upload_date: date,
    user_instructions: str,
    topic_options: Optional[List[str]],
) -> Dict[str, Any]:
    """
    Full extract → fact-check → revise pipeline.

    Pass 1 (`record_event`): extract event fields from the source.
    Pass 2 (`report_concerns`): independently review the extract against
        the source and flag any inaccuracies/ambiguities/missing info.
    Pass 3 (`record_event`, conditional): if pass 2 found concerns,
        re-run the extractor with the concerns attached so it can
        correct the record. Skipped entirely when the first extract
        looks clean, so clean inputs stay at the old 2-call cost.

    `source_blocks` is the content list that represents the event
    source — an image block for screenshots, a text block for pasted
    text. `source_word` is the noun Claude sees in the user-facing
    prompts ("image", "pasted event text") so the instructions read
    naturally either way.
    """
    instructions = (user_instructions or "").strip()

    extract_user_text = (
        f"Extract the event details from this {source_word} by "
        "calling the record_event tool."
    )
    if instructions:
        extract_user_text += (
            "\n\n---\nAdditional instructions from the uploader "
            "(follow these carefully — they describe which event to focus "
            "on, what to emphasize in the description, etc.):\n"
            + instructions
        )

    # Build the extract tool, optionally with a topic_name enum so Claude
    # auto-picks a topic from the caller-supplied list.
    extract_tool = (
        _extract_tool_with_topics(topic_options) if topic_options else _EXTRACT_TOOL
    )

    # ---- Pass 1: extract -------------------------------------------------
    extract_resp = client.messages.create(
        model=_MODEL,
        max_tokens=1024,
        system=_extract_system_prompt(upload_date),
        tools=[extract_tool],
        tool_choice={"type": "tool", "name": "record_event"},
        messages=[
            {
                "role": "user",
                "content": [
                    *source_blocks,
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
        f"against the original {source_word} and report any concerns.\n\n"
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

    # ---- Pass 2: fact-check (independent read of the source) -------------
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
                    *source_blocks,
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

    # ---- Pass 3: revise (only if pass 2 flagged anything) ---------------
    # When the fact-checker returns concerns, we feed them back to the
    # extractor along with the original source so it can produce a
    # corrected record. We preserve the original concerns list so the
    # UI can still surface what was flagged (now "what was addressed").
    if concerns:
        revise_user_parts: List[str] = [
            "A prior extraction of this event produced the following "
            "record:\n\n```json\n"
            + json.dumps(extracted, indent=2)
            + "\n```",
            "An independent fact-checker flagged these concerns about "
            "the extraction:\n"
            + "\n".join(f"  - {c}" for c in concerns),
            (
                "Please look at the original "
                f"{source_word} again and call the record_event tool "
                "with a corrected record that addresses those concerns. "
                "Keep the fields that weren't flagged unchanged."
            ),
        ]
        if instructions:
            revise_user_parts.append(
                "Uploader's original instructions (still in effect):\n"
                + instructions
            )
        revise_user_text = "\n\n---\n".join(revise_user_parts)

        revise_resp = client.messages.create(
            model=_MODEL,
            max_tokens=1024,
            system=_revise_system_prompt(upload_date),
            tools=[extract_tool],
            tool_choice={"type": "tool", "name": "record_event"},
            messages=[
                {
                    "role": "user",
                    "content": [
                        *source_blocks,
                        {
                            "type": "text",
                            "text": revise_user_text,
                        },
                    ],
                }
            ],
        )
        revised = _tool_call_input(revise_resp, "record_event")
        # Replace in place so the merge step below sees the corrected
        # record. `concerns` stays unchanged so the UI still shows what
        # was flagged (labeled as addressed).
        extracted = revised

    return {
        "title":                str(extracted.get("title") or "").strip(),
        "description":          str(extracted.get("description") or "").strip(),
        "start_date":           (str(extracted.get("start_date") or "").strip() or None),
        "start_time":           (str(extracted.get("start_time") or "").strip() or None),
        "end_date":             (str(extracted.get("end_date") or "").strip() or None),
        "end_time":             (str(extracted.get("end_time") or "").strip() or None),
        "location":             str(extracted.get("location") or "").strip(),
        "details_url":          str(extracted.get("details_url") or "").strip(),
        "hosted_by":            str(extracted.get("hosted_by") or "").strip(),
        "special_instructions": str(extracted.get("special_instructions") or "").strip(),
        "topic_name":           (str(extracted.get("topic_name") or "").strip() or None),
        "concerns":             concerns,
        "raw_extract":          extracted,
    }


def extract_and_factcheck(
    image_bytes: bytes,
    *,
    mime_type: str = "image/png",
    upload_date: Optional[date] = None,
    api_key: str,
    user_instructions: str = "",
    topic_options: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Image variant. Run extract → fact-check on a flyer/screenshot. See
    module docstring for the return shape. Raises on API failure so the
    caller can surface a clean error per row.

    `user_instructions` is free-text guidance from the uploader (e.g.
    "Focus on the May 1 reception but mention the broader exhibit in the
    description.") It's injected into both the extract and fact-check
    passes so Claude follows the uploader's intent and the fact-checker
    flags deviations.
    """
    client = anthropic.Anthropic(api_key=api_key)
    return _run_pipeline(
        client,
        [_image_block(image_bytes, mime_type)],
        source_word="image",
        upload_date=upload_date or date.today(),
        user_instructions=user_instructions,
        topic_options=topic_options,
    )


def extract_and_factcheck_text(
    text: str,
    *,
    upload_date: Optional[date] = None,
    api_key: str,
    user_instructions: str = "",
    topic_options: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Text variant. Same two-pass extract → fact-check, but the source is
    pasted event text (from an email, social post, webpage copy, etc.)
    instead of an image. Returns the same shape as `extract_and_factcheck`.
    """
    body = (text or "").strip()
    if not body:
        raise ValueError("extract_and_factcheck_text: no text provided.")

    # Wrap the paste in a small fence so Claude can unambiguously see
    # its boundaries — otherwise the instruction text and the event
    # text could blur together when the paste itself contains prose.
    source_block = {
        "type": "text",
        "text": (
            "The following is event information that was pasted by the "
            "user (from an email, webpage, social post, etc.). Treat it "
            "as the authoritative source:\n\n"
            "<<<EVENT TEXT>>>\n"
            + body
            + "\n<<<END EVENT TEXT>>>"
        ),
    }

    client = anthropic.Anthropic(api_key=api_key)
    return _run_pipeline(
        client,
        [source_block],
        source_word="pasted event text",
        upload_date=upload_date or date.today(),
        user_instructions=user_instructions,
        topic_options=topic_options,
    )
