"""Transcribe clips using faster-whisper + apply quality gates."""
import asyncio
import json
from pathlib import Path
from src.db.database import get_db
from src.models.schemas import ClipMeta, ClipStatus, ProfileRules
from src.config import settings
from src.utils.log import log

# Lazy-load whisper model (heavy import)
_model = None


def _get_model():
    """Lazy-load faster-whisper model."""
    global _model
    if _model is None:
        from faster_whisper import WhisperModel
        log.info("Loading whisper model (base.en) — first run downloads ~150MB...")
        _model = WhisperModel(
            "base.en",
            device="cpu",           # faster-whisper uses CPU on macOS (no CUDA)
            compute_type="int8",    # fast on Apple Silicon
        )
        log.info("Whisper model loaded")
    return _model


def transcribe_audio(audio_path: str) -> dict:
    """
    Transcribe an audio/video file.
    Returns {segments: [{start, end, text}...], language, duration, full_text}
    """
    model = _get_model()

    segments_raw, info = model.transcribe(
        audio_path,
        beam_size=5,
        language="en",
        vad_filter=True,          # Voice Activity Detection — filters silence
        vad_parameters=dict(
            min_silence_duration_ms=500,
        ),
    )

    segments = []
    full_text_parts = []
    for seg in segments_raw:
        segments.append({
            "start": round(seg.start, 3),
            "end": round(seg.end, 3),
            "text": seg.text.strip(),
        })
        full_text_parts.append(seg.text.strip())

    return {
        "segments": segments,
        "language": info.language,
        "language_probability": round(info.language_probability, 3),
        "duration": round(info.duration, 3),
        "full_text": " ".join(full_text_parts),
    }


# ── Quality Gates ──

def gate_hook(transcript: dict, rules: ProfileRules) -> tuple[bool, str]:
    """
    Hook gate: reject if first spoken word starts after hook_max_delay_sec.
    Rationale: viewers scroll away if nothing happens in first 2 seconds.
    """
    if not transcript["segments"]:
        return False, "no_speech_detected"

    first_start = transcript["segments"][0]["start"]
    if first_start > rules.hook_max_delay_sec:
        return False, f"hook_too_late:{first_start:.1f}s>(max {rules.hook_max_delay_sec}s)"

    return True, ""


def gate_silence(transcript: dict, rules: ProfileRules) -> tuple[bool, str]:
    """
    Silence gate: reject if silence_ratio > threshold.
    silence_ratio = 1 - (total_speech_duration / total_clip_duration)
    """
    total_duration = transcript["duration"]
    if total_duration <= 0:
        return False, "zero_duration"

    speech_duration = sum(
        seg["end"] - seg["start"] for seg in transcript["segments"]
    )
    silence_ratio = 1.0 - (speech_duration / total_duration)

    if silence_ratio > rules.silence_ratio_max + 0.001:  # small epsilon for float comparison
        return False, f"too_silent:{silence_ratio:.0%}>(max {rules.silence_ratio_max:.0%})"

    return True, ""


def gate_length(transcript: dict, rules: ProfileRules) -> tuple[bool, str]:
    """
    Length gate: reject if clip duration is outside the profile's length band.
    """
    dur = transcript["duration"]
    min_len, max_len = rules.length_band_sec

    if dur < min_len:
        return False, f"too_short:{dur:.0f}s<(min {min_len}s)"
    if dur > max_len:
        # Don't reject long clips — we'll trim them in the edit decision.
        # But flag if way too long (>3x max)
        if dur > max_len * 3:
            return False, f"way_too_long:{dur:.0f}s>(max {max_len * 3}s)"

    return True, ""


def run_quality_gates(transcript: dict, rules: ProfileRules) -> tuple[bool, str]:
    """Run all quality gates. Returns (passed, reason)."""
    gates = [
        ("hook", gate_hook),
        ("silence", gate_silence),
        ("length", gate_length),
    ]

    for name, gate_fn in gates:
        passed, reason = gate_fn(transcript, rules)
        if not passed:
            return False, reason

    return True, ""


# ── Orchestrator ──

async def transcribe_clip(clip_row_id: int) -> bool:
    """Transcribe a DOWNLOADED clip, apply quality gates, update DB."""
    db = get_db()
    row = db.execute("""
        SELECT cl.*, p.rules_json
        FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        WHERE cl.id = ? AND cl.status = ?
    """, (clip_row_id, ClipStatus.DOWNLOADED.value)).fetchone()

    if not row:
        log.warning(f"Clip {clip_row_id} not found or not DOWNLOADED")
        return False

    paths = json.loads(row["paths_json"])
    source_path = paths.get("source")
    if not source_path or not Path(source_path).exists():
        log.error(f"Source file missing for clip {clip_row_id}: {source_path}")
        db.execute("""
            UPDATE clips SET status = ?, fail_reason = 'source_missing', updated_at = datetime('now')
            WHERE id = ?
        """, (ClipStatus.FAILED.value, clip_row_id))
        db.commit()
        db.close()
        return False

    clip_meta = ClipMeta.model_validate_json(row["metadata_json"])
    rules = ProfileRules.model_validate_json(row["rules_json"])

    log.info(f"Transcribing: {clip_meta.title} ({row['platform']}/{row['clip_id']})")

    # Run transcription in executor (CPU-bound)
    loop = asyncio.get_event_loop()
    try:
        transcript = await loop.run_in_executor(None, transcribe_audio, source_path)
    except Exception as e:
        log.error(f"Transcription failed: {e}")
        db.execute("""
            UPDATE clips SET status = ?, fail_reason = ?, updated_at = datetime('now')
            WHERE id = ?
        """, (ClipStatus.FAILED.value, f"transcription_error:{e}", clip_row_id))
        db.commit()
        db.close()
        return False

    # Save transcript to disk
    transcript_path = Path(source_path).parent / "transcript.json"
    with open(transcript_path, "w") as f:
        json.dump(transcript, f, indent=2)

    # Update paths
    paths["transcript"] = str(transcript_path)

    # Run quality gates
    passed, fail_reason = run_quality_gates(transcript, rules)

    if passed:
        db.execute("""
            UPDATE clips SET
                status = ?,
                paths_json = ?,
                updated_at = datetime('now')
            WHERE id = ?
        """, (ClipStatus.TRANSCRIBED.value, json.dumps(paths), clip_row_id))
        db.commit()
        db.close()
        log.info(f"  ✅ Transcribed ({len(transcript['segments'])} segments, {transcript['duration']:.0f}s)")
        log.info(f"  Text: {transcript['full_text'][:100]}...")
        return True
    else:
        db.execute("""
            UPDATE clips SET
                status = ?,
                fail_reason = ?,
                paths_json = ?,
                updated_at = datetime('now')
            WHERE id = ?
        """, (ClipStatus.FAILED.value, fail_reason, json.dumps(paths), clip_row_id))
        db.commit()
        db.close()
        log.warning(f"  ❌ Quality gate failed: {fail_reason}")
        return False


async def transcribe_downloaded_clips(profile_slug: str, limit: int = 10) -> dict:
    """Transcribe all DOWNLOADED clips for a profile. Returns stats."""
    db = get_db()
    rows = db.execute("""
        SELECT cl.id FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        WHERE p.slug = ? AND cl.status = ?
        ORDER BY cl.created_at ASC
        LIMIT ?
    """, (profile_slug, ClipStatus.DOWNLOADED.value, limit)).fetchall()
    db.close()

    stats = {"total": len(rows), "passed": 0, "failed": 0}

    for row in rows:
        ok = await transcribe_clip(row["id"])
        if ok:
            stats["passed"] += 1
        else:
            stats["failed"] += 1

    return stats