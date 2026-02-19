"""
Milestone 4: Render shorts from edit decisions.

Uses ffmpeg to:
  1. Extract the chosen segment (start → end)
  2. Crop to 9:16 (center crop from 16:9 source)
  3. Burn in word-by-word captions from transcript
  4. Normalize audio (loudnorm)
  5. Output final MP4
"""
import asyncio
import json
import re
from pathlib import Path
from src.db.database import get_db
from src.models.schemas import ClipMeta, ClipStatus, EditDecision, Segment
from src.config import settings
from src.utils.log import log


def _escape_ass(text: str) -> str:
    """Escape special characters for ASS subtitle format."""
    # ASS uses { } for override tags, so escape them
    text = text.replace("\\", "\\\\")
    text = text.replace("{", "\\{")
    text = text.replace("}", "\\}")
    return text


def _secs_to_ass_time(secs: float) -> str:
    """Convert seconds to ASS timestamp format: H:MM:SS.cc"""
    h = int(secs // 3600)
    m = int((secs % 3600) // 60)
    s = secs % 60
    return f"{h}:{m:02d}:{s:05.2f}"


def _build_ass_subtitles(
    transcript: dict,
    segment: Segment,
    max_words: int = 3,
    style: str = "bold_white",
) -> str:
    """
    Build ASS subtitle file content with word-by-word captions.

    Groups words into chunks of max_words, times them evenly across each segment.
    """
    # ASS header with styling
    if style == "bold_white":
        font_name = "Arial"
        font_size = 16
        primary_color = "&H00FFFFFF"   # white
        outline_color = "&H00000000"   # black outline
        bold = -1
        outline = 3
        shadow = 1
    else:
        font_name = "Arial"
        font_size = 16
        primary_color = "&H00FFFFFF"
        outline_color = "&H00000000"
        bold = -1
        outline = 3
        shadow = 1

    ass_header = f"""[Script Info]
Title: ClipForge Captions
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,{font_name},{font_size},{primary_color},&H000000FF,{outline_color},&H80000000,{bold},0,0,0,100,100,0,0,1,{outline},{shadow},2,40,40,120,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""

    events = []

    for seg in transcript.get("segments", []):
        seg_start = seg["start"]
        seg_end = seg["end"]

        # Skip segments outside our chosen segment
        if seg_end <= segment.start or seg_start >= segment.end:
            continue

        # Clamp to segment bounds
        seg_start = max(seg_start, segment.start)
        seg_end = min(seg_end, segment.end)

        # Offset to make times relative to the extracted segment
        rel_start = seg_start - segment.start
        rel_end = seg_end - segment.start

        text = seg["text"].strip()
        if not text:
            continue

        words = text.split()
        if not words:
            continue

        # Group words into chunks
        chunks = []
        for i in range(0, len(words), max_words):
            chunks.append(" ".join(words[i:i + max_words]))

        # Distribute time evenly across chunks
        chunk_duration = (rel_end - rel_start) / len(chunks)

        for i, chunk in enumerate(chunks):
            c_start = rel_start + i * chunk_duration
            c_end = rel_start + (i + 1) * chunk_duration

            ass_start = _secs_to_ass_time(c_start)
            ass_end = _secs_to_ass_time(c_end)
            escaped = _escape_ass(chunk.upper())

            events.append(
                f"Dialogue: 0,{ass_start},{ass_end},Default,,0,0,0,,{escaped}"
            )

    return ass_header + "\n".join(events) + "\n"


async def probe_video(source_path: str) -> dict:
    """Get video dimensions and codec info via ffprobe."""
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        source_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        return {}

    data = json.loads(stdout)
    for stream in data.get("streams", []):
        if stream.get("codec_type") == "video":
            return {
                "width": int(stream.get("width", 0)),
                "height": int(stream.get("height", 0)),
                "codec": stream.get("codec_name", ""),
                "duration": float(stream.get("duration", 0)),
            }
    return {}


def _build_crop_filter(src_w: int, src_h: int) -> str:
    """
    Build ffmpeg crop filter for 9:16 from source dimensions.
    Center crop: take the tallest 9:16 slice from the center.
    """
    target_ratio = 9 / 16  # width / height

    if src_w <= 0 or src_h <= 0:
        # Fallback: assume 1920x1080 source
        src_w, src_h = 1920, 1080

    src_ratio = src_w / src_h

    if src_ratio > target_ratio:
        # Source is wider than 9:16 — crop width, keep full height
        out_h = src_h
        out_w = int(out_h * target_ratio)
    else:
        # Source is taller than 9:16 — crop height, keep full width
        out_w = src_w
        out_h = int(out_w / target_ratio)

    # Center the crop
    x = (src_w - out_w) // 2
    y = (src_h - out_h) // 2

    return f"crop={out_w}:{out_h}:{x}:{y}"


async def render_clip(clip_row_id: int) -> bool:
    """Render a DECIDED clip into a 9:16 short with captions."""
    db = get_db()
    row = db.execute("""
        SELECT cl.*, p.rules_json, p.slug as profile_slug
        FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        WHERE cl.id = ? AND cl.status = ?
    """, (clip_row_id, ClipStatus.DECIDED.value)).fetchone()

    if not row:
        log.warning(f"Clip {clip_row_id} not found or not DECIDED")
        db.close()
        return False

    paths = json.loads(row["paths_json"])
    source_path = paths.get("source")
    decision_path = paths.get("edit_decision")
    transcript_path = paths.get("transcript")

    if not source_path or not Path(source_path).exists():
        log.error(f"Source missing for clip {clip_row_id}")
        db.close()
        return False

    if not decision_path or not Path(decision_path).exists():
        log.error(f"Edit decision missing for clip {clip_row_id}")
        db.close()
        return False

    # Load edit decision
    with open(decision_path) as f:
        ed = EditDecision.model_validate_json(f.read())

    # Load transcript
    with open(transcript_path) as f:
        transcript = json.load(f)

    log.info(f"Rendering: clip {clip_row_id} ({row['platform']}/{row['clip_id'][:30]}...)")
    log.info(f"  Segment: {ed.segment.start:.1f}s → {ed.segment.end:.1f}s")

    # Probe source video
    probe = await probe_video(source_path)
    src_w = probe.get("width", 1920)
    src_h = probe.get("height", 1080)
    log.info(f"  Source: {src_w}x{src_h}")

    # Build ASS subtitle file
    clip_dir = Path(source_path).parent
    ass_path = clip_dir / "captions.ass"
    ass_content = _build_ass_subtitles(
        transcript,
        ed.segment,
        max_words=ed.captions.max_words,
        style=ed.captions.style,
    )
    with open(ass_path, "w") as f:
        f.write(ass_content)

    # Build ffmpeg command
    output_path = clip_dir / "rendered.mp4"
    segment_duration = ed.segment.end - ed.segment.start

    # Video filter chain:
    # 1. Crop to 9:16 from center
    # 2. Scale to 1080x1920
    # 3. Burn in ASS subtitles
    crop_filter = _build_crop_filter(src_w, src_h)

    # For ffmpeg filter graphs, special chars in paths need escaping:
    # 1. Backslash, colon, semicolon, single-quote, brackets need \ prefix
    # 2. No wrapping quotes — subprocess passes the string directly
    ass_path_str = str(ass_path)
    ass_escaped = ass_path_str
    for ch in ["\\", ":", "'", "[", "]", ";", ","]:
        ass_escaped = ass_escaped.replace(ch, f"\\{ch}")

    vf_with_subs = f"{crop_filter},scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,ass={ass_escaped}"

    cmd = [
        "ffmpeg", "-y",
        "-ss", str(ed.segment.start),
        "-i", source_path,
        "-t", str(segment_duration),
        "-vf", vf_with_subs,
        "-af", "loudnorm=I=-14:TP=-1:LRA=11",
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "44100",
        "-movflags", "+faststart",
        str(output_path),
    ]

    log.info(f"  Running ffmpeg...")

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        err_text = stderr.decode()[-800:]
        log.error(f"  ffmpeg failed:\n{err_text}")

        # Common fix: ASS path issue — try without subtitles
        log.info("  Retrying without subtitles...")
        vf_nosub = f"{crop_filter},scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2"
        cmd_nosub = [
            "ffmpeg", "-y",
            "-ss", str(ed.segment.start),
            "-i", source_path,
            "-t", str(segment_duration),
            "-vf", vf_nosub,
            "-af", "loudnorm=I=-14:TP=-1:LRA=11",
            "-c:v", "libx264",
            "-preset", "medium",
            "-crf", "23",
            "-c:a", "aac",
            "-b:a", "128k",
            "-ar", "44100",
            "-movflags", "+faststart",
            str(output_path),
        ]
        proc2 = await asyncio.create_subprocess_exec(
            *cmd_nosub,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr2 = await proc2.communicate()
        if proc2.returncode != 0:
            log.error(f"  ffmpeg retry also failed:\n{stderr2.decode()[-500:]}")
            db.execute("""
                UPDATE clips SET status = ?, fail_reason = 'render_failed', updated_at = datetime('now')
                WHERE id = ?
            """, (ClipStatus.FAILED.value, clip_row_id))
            db.commit()
            db.close()
            return False
        else:
            log.warning("  ⚠ Rendered WITHOUT captions (subtitle burn-in failed)")

    # Verify output
    if not output_path.exists() or output_path.stat().st_size < 1000:
        log.error(f"  Output file missing or too small")
        db.execute("""
            UPDATE clips SET status = ?, fail_reason = 'render_output_invalid', updated_at = datetime('now')
            WHERE id = ?
        """, (ClipStatus.FAILED.value, clip_row_id))
        db.commit()
        db.close()
        return False

    # Probe output
    out_probe = await probe_video(str(output_path))
    out_w = out_probe.get("width", 0)
    out_h = out_probe.get("height", 0)
    file_size_mb = output_path.stat().st_size / 1024 / 1024

    # Update DB
    paths["rendered"] = str(output_path)
    paths["captions_ass"] = str(ass_path)

    db.execute("""
        UPDATE clips SET
            status = ?,
            paths_json = ?,
            updated_at = datetime('now')
        WHERE id = ?
    """, (ClipStatus.RENDERED.value, json.dumps(paths), clip_row_id))
    db.commit()
    db.close()

    log.info(f"  ✅ Rendered: {out_w}x{out_h}, {file_size_mb:.1f} MB, {segment_duration:.1f}s")
    return True


async def render_decided_clips(profile_slug: str, limit: int = 10) -> dict:
    """Render all DECIDED clips for a profile."""
    db = get_db()
    rows = db.execute("""
        SELECT cl.id FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        WHERE p.slug = ? AND cl.status = ?
        ORDER BY cl.created_at ASC
        LIMIT ?
    """, (profile_slug, ClipStatus.DECIDED.value, limit)).fetchall()
    db.close()

    stats = {"total": len(rows), "rendered": 0, "failed": 0}

    for row in rows:
        ok = await render_clip(row["id"])
        if ok:
            stats["rendered"] += 1
        else:
            stats["failed"] += 1

    return stats