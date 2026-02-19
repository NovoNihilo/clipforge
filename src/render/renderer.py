"""
Milestone 4: Render shorts from edit decisions.

Uses ffmpeg to:
  1. Extract the chosen segment (start -> end)
  2. Crop to 9:16 (center crop from 16:9 source)
  3. Burn in word-by-word captions via drawtext filter
  4. Normalize audio (loudnorm)
  5. Output final MP4
"""
import asyncio
import json
import re
import tempfile
from pathlib import Path
from src.db.database import get_db
from src.models.schemas import ClipMeta, ClipStatus, EditDecision, Segment
from src.config import settings
from src.utils.log import log


async def probe_video(source_path: str) -> dict:
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams",
        source_path, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
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
    target_ratio = 9 / 16
    if src_w <= 0 or src_h <= 0:
        src_w, src_h = 1920, 1080
    src_ratio = src_w / src_h
    if src_ratio > target_ratio:
        out_h = src_h
        out_w = int(out_h * target_ratio)
    else:
        out_w = src_w
        out_h = int(out_w / target_ratio)
    x = (src_w - out_w) // 2
    y = (src_h - out_h) // 2
    return f"crop={out_w}:{out_h}:{x}:{y}"


def _build_drawtext_filters(
    transcript: dict,
    segment: Segment,
    max_words: int = 3,
) -> str:
    """
    Build a chain of drawtext filters for word-by-word captions.
    Each chunk appears/disappears at the right time using enable='between(t,start,end)'.
    """
    filters = []

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

            # Escape text for drawtext: ' -> \\', : -> \\:, \ -> \\\\
            escaped = chunk.upper()
            escaped = escaped.replace("\\", "\\\\\\\\")
            escaped = escaped.replace("'", "\u2019")  # replace apostrophe with unicode
            escaped = escaped.replace(":", "\\:")
            escaped = escaped.replace("%", "%%")

            filters.append(
                f"drawtext=text='{escaped}'"
                f":fontsize=64"
                f":fontcolor=white"
                f":borderw=4"
                f":bordercolor=black"
                f":fontfile=/System/Library/Fonts/Helvetica.ttc"
                f":x=(w-text_w)/2"
                f":y=h*0.75"
                f":enable='between(t\\,{c_start:.3f}\\,{c_end:.3f})'"
            )

    return ",".join(filters) if filters else ""


async def render_clip(clip_row_id: int) -> bool:
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

    with open(decision_path) as f:
        ed = EditDecision.model_validate_json(f.read())

    with open(transcript_path) as f:
        transcript = json.load(f)

    log.info(f"Rendering: clip {clip_row_id} ({row['platform']}/{row['clip_id'][:30]}...)")
    log.info(f"  Segment: {ed.segment.start:.1f}s -> {ed.segment.end:.1f}s")

    probe = await probe_video(source_path)
    src_w = probe.get("width", 1920)
    src_h = probe.get("height", 1080)
    log.info(f"  Source: {src_w}x{src_h}")

    clip_dir = Path(source_path).parent
    output_path = clip_dir / "rendered.mp4"
    segment_duration = ed.segment.end - ed.segment.start

    crop_filter = _build_crop_filter(src_w, src_h)
    scale_pad = "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2"
    drawtext_chain = _build_drawtext_filters(transcript, ed.segment, max_words=ed.captions.max_words)

    if drawtext_chain:
        vf = f"{crop_filter},{scale_pad},{drawtext_chain}"
    else:
        vf = f"{crop_filter},{scale_pad}"

    # Write the complex filter to a script file to avoid shell escaping issues
    filter_script = clip_dir / "filter_script.txt"
    with open(filter_script, "w") as f:
        f.write(vf)

    cmd = [
        "ffmpeg", "-y",
        "-ss", str(ed.segment.start),
        "-i", source_path,
        "-t", str(segment_duration),
        "-filter_script:v", str(filter_script),
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

    log.info(f"  Running ffmpeg (with drawtext captions)...")

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        err_text = stderr.decode()[-800:]
        log.error(f"  ffmpeg with captions failed:\n{err_text}")

        # Fallback: render without captions
        log.info("  Retrying without captions...")
        vf_nosub = f"{crop_filter},{scale_pad}"
        cmd_nosub = [
            "ffmpeg", "-y",
            "-ss", str(ed.segment.start),
            "-i", source_path,
            "-t", str(segment_duration),
            "-vf", vf_nosub,
            "-af", "loudnorm=I=-14:TP=-1:LRA=11",
            "-c:v", "libx264", "-preset", "medium", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
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
            log.warning("  Rendered WITHOUT captions (drawtext failed)")

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

    out_probe = await probe_video(str(output_path))
    out_w = out_probe.get("width", 0)
    out_h = out_probe.get("height", 0)
    file_size_mb = output_path.stat().st_size / 1024 / 1024

    paths["rendered"] = str(output_path)
    db.execute("""
        UPDATE clips SET status = ?, paths_json = ?, updated_at = datetime('now')
        WHERE id = ?
    """, (ClipStatus.RENDERED.value, json.dumps(paths), clip_row_id))
    db.commit()
    db.close()

    log.info(f"  ✅ Rendered: {out_w}x{out_h}, {file_size_mb:.1f} MB, {segment_duration:.1f}s")
    return True


async def render_decided_clips(profile_slug: str, limit: int = 10) -> dict:
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