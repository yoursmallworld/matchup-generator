"""
Streamlit tab: upload audio/video files and get transcripts via AssemblyAI.

Speaker diarization + timestamps included. Uses the same AssemblyAI key
stored in Streamlit secrets as `ASSEMBLYAI_API_KEY`.

Entry point: `render()`. Called from app.py.
"""

from __future__ import annotations

import io
import time
from typing import Any, Dict, List, Optional

import requests
import streamlit as st

ASSEMBLYAI_BASE = "https://api.assemblyai.com/v2"

# Supported upload types. AssemblyAI handles most common audio/video formats.
SUPPORTED_TYPES = ["mp3", "mp4", "m4a", "wav", "flac", "ogg", "webm", "aac", "wma", "mov", "avi"]

# Session-state keys (namespaced with "tx_" to avoid collisions).
SS_TRANSCRIPT = "tx_transcript"
SS_STATUS = "tx_status"


def _ms_to_timestamp(ms: int) -> str:
    """Convert milliseconds to HH:MM:SS."""
    s = ms // 1000
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{h}:{m:02d}:{s:02d}"


def _upload_audio(api_key: str, file_bytes: bytes) -> str:
    """Upload audio bytes to AssemblyAI and return the upload_url."""
    resp = requests.post(
        f"{ASSEMBLYAI_BASE}/upload",
        headers={"authorization": api_key},
        data=file_bytes,
        timeout=600,
    )
    resp.raise_for_status()
    return resp.json()["upload_url"]


def _submit_transcription(api_key: str, upload_url: str) -> str:
    """Submit a transcription request and return the transcript ID."""
    resp = requests.post(
        f"{ASSEMBLYAI_BASE}/transcript",
        headers={
            "authorization": api_key,
            "content-type": "application/json",
        },
        json={
            "audio_url": upload_url,
            "speech_models": ["universal-3-pro", "universal-2"],
            "language_detection": True,
            "speaker_labels": True,
            "auto_chapters": True,
        },
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"Transcription submit failed ({resp.status_code}): {resp.text[:300]}")
    return resp.json()["id"]


def _poll_transcript(api_key: str, transcript_id: str, status_container) -> Dict[str, Any]:
    """Poll until the transcript is ready. Updates the status_container with progress."""
    poll_url = f"{ASSEMBLYAI_BASE}/transcript/{transcript_id}"
    headers = {"authorization": api_key}
    start = time.time()

    for attempt in range(360):  # up to 30 minutes
        resp = requests.get(poll_url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status")

        if status == "completed":
            return data
        elif status == "error":
            raise RuntimeError(f"Transcription failed: {data.get('error', 'unknown error')}")

        elapsed = int(time.time() - start)
        status_container.info(f"Transcribing... ({elapsed}s elapsed)")
        time.sleep(5)

    raise RuntimeError("Transcription timed out after 30 minutes.")


def _format_transcript(data: Dict[str, Any]) -> str:
    """Format the AssemblyAI response into a readable transcript string."""
    lines: List[str] = []

    # Header
    duration_ms = data.get("audio_duration", 0) * 1000 if data.get("audio_duration") else 0
    if duration_ms:
        duration_str = _ms_to_timestamp(int(duration_ms))
        lines.append(f"Duration: {duration_str}")
        lines.append("")

    # Chapter summary (if available)
    chapters = data.get("chapters") or []
    if chapters:
        lines.append("CHAPTER SUMMARY")
        lines.append("-" * 40)
        for i, ch in enumerate(chapters, 1):
            start = _ms_to_timestamp(ch.get("start", 0))
            lines.append(f"\n{i}. [{start}] {ch.get('headline', '')}")
            lines.append(f"   {ch.get('summary', '')}")
        lines.append("")
        lines.append("=" * 60)
        lines.append("")

    # Full transcript with speaker labels
    utterances = data.get("utterances") or []
    if utterances:
        lines.append("FULL TRANSCRIPT")
        lines.append("-" * 40)
        lines.append("")
        for utt in utterances:
            speaker = utt.get("speaker", "?")
            start = _ms_to_timestamp(utt.get("start", 0))
            text = utt.get("text", "")
            lines.append(f"[{start}] Speaker {speaker}: {text}")
            lines.append("")
    else:
        # Fallback: just the full text without speaker labels
        text = data.get("text", "")
        if text:
            lines.append("FULL TRANSCRIPT")
            lines.append("-" * 40)
            lines.append("")
            lines.append(text)

    return "\n".join(lines)


# ---- Main render --------------------------------------------------------


def render() -> None:
    st.subheader("Transcribe")
    st.caption(
        "Upload an audio or video file to get a transcript with speaker "
        "labels and timestamps via AssemblyAI. Supports MP3, MP4, WAV, "
        "M4A, FLAC, and more."
    )

    api_key = None
    if hasattr(st, "secrets"):
        api_key = st.secrets.get("ASSEMBLYAI_API_KEY")
    if not api_key:
        st.error(
            "Set `ASSEMBLYAI_API_KEY` in Streamlit Cloud → Settings → Secrets "
            "(or `.streamlit/secrets.toml` locally) to use this tab."
        )
        return

    # ---- Upload ---------------------------------------------------------
    uploaded = st.file_uploader(
        "Drop an audio or video file",
        type=SUPPORTED_TYPES,
        key="tx_uploader",
    )

    col1, col2 = st.columns([1, 1])
    with col1:
        transcribe_clicked = st.button(
            "Transcribe",
            disabled=uploaded is None,
            type="primary",
            use_container_width=True,
        )
    with col2:
        if st.button(
            "Clear",
            disabled=not st.session_state.get(SS_TRANSCRIPT),
            use_container_width=True,
        ):
            st.session_state.pop(SS_TRANSCRIPT, None)
            st.session_state.pop(SS_STATUS, None)
            st.rerun()

    # ---- Transcription pipeline -----------------------------------------
    if transcribe_clicked and uploaded is not None:
        file_bytes = uploaded.getvalue()
        file_size_mb = len(file_bytes) / (1024 * 1024)

        st.info(f"File: **{uploaded.name}** ({file_size_mb:.1f} MB)")

        status_container = st.empty()

        try:
            # Step 1: Upload
            status_container.info("Uploading to AssemblyAI...")
            upload_url = _upload_audio(api_key, file_bytes)

            # Step 2: Submit
            status_container.info("Submitting transcription request...")
            transcript_id = _submit_transcription(api_key, upload_url)

            # Step 3: Poll
            data = _poll_transcript(api_key, transcript_id, status_container)

            # Step 4: Format
            transcript_text = _format_transcript(data)
            st.session_state[SS_TRANSCRIPT] = transcript_text
            st.session_state[SS_STATUS] = "done"
            status_container.success("Transcription complete!")

        except Exception as e:
            status_container.error(f"Transcription failed: {e}")
            st.session_state[SS_STATUS] = "error"

    # ---- Display result -------------------------------------------------
    transcript = st.session_state.get(SS_TRANSCRIPT)
    if transcript:
        st.divider()
        st.markdown("**Transcript**")
        st.text_area(
            "transcript_output",
            value=transcript,
            height=500,
            label_visibility="collapsed",
        )

        # Download button
        st.download_button(
            label="Download transcript (.txt)",
            data=transcript,
            file_name="transcript.txt",
            mime="text/plain",
            use_container_width=True,
        )
