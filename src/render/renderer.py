"""
Milestone 4 v6: Render shorts.

- Blurred background + centered overlay (shows more of original frame)
- Word-level caption timing (uses word timestamps from transcript)
- Bold white Impact captions
- Large persistent title overlay
- Optional background music mixing
- Fixed: captions no longer appear early during silence gaps
"""
import asyncio
import json
import re
import textwrap
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


def _build_video_filter(src_w: int, src_h: int) -> str:
    """
    Blurred background + centered overlay layout.

    Instead of cropping (which loses most of the frame), we:
    1. Scale + blur the full frame as a 1080x1920 background
    2. Scale the original to fit width (1080px wide), maintaining aspect ratio
    3. Overlay the sharp original centered on the blurred background

    For 16:9 source (1920x1080):
      - Background: blurred, scaled to fill 1080x1920
      - Overlay: 1080x608, centered vertically (with ~656px of blur visible top+bottom)
    """
    if src_w <= 0 or src_h <= 0:
        src_w, src_h = 1920, 1080

    vf = (
        "[0:v]split[bg][fg];"
        "[bg]scale=1080:1920:force_original_aspect_ratio=increase,"
        "crop=1080:1920,"
        "boxblur=20:5[blurred];"
        "[fg]scale=1080:-2[sharp];"
        "[blurred][sharp]overlay=(W-w)/2:(H-h)/2"
    )
    return vf


def _escape_drawtext(text: str) -> str:
    escaped = text
    escaped = escaped.replace("\\", "\\\\\\\\")
    escaped = escaped.replace("'", "\u2019")
    escaped = escaped.replace(":", "\\:")
    escaped = escaped.replace("%", "%%")
    escaped = escaped.replace('"', '\\"')
    return escaped


def _build_caption_filters(
    transcript: dict,
    segment: Segment,
    max_words: int = 3,
) -> str:
    """
    Build caption filters using WORD-LEVEL timestamps when available.
    Falls back to per-segment timestamps (not even distribution) if word
    timestamps are missing.

    Key fix: each caption chunk is tightly bound to its actual speech timing.
    Silence gaps between chunks produce no visible captions.
    """
    filters = []
    FONT = "/System/Library/Fonts/Supplemental/Impact.ttf"

    # Small padding so captions don't vanish mid-syllable
    TAIL_PAD = 0.15  # seconds to hold caption after last word ends

    has_word_timestamps = bool(transcript.get("words"))

    if has_word_timestamps:
        # Use precise word timestamps — group into chunks of max_words
        words = transcript["words"]

        # Filter words within our segment
        seg_words = [
            w for w in words
            if w["end"] > segment.start and w["start"] < segment.end
        ]

        # Group into chunks
        for i in range(0, len(seg_words), max_words):
            chunk_words = seg_words[i:i + max_words]
            if not chunk_words:
                continue

            # Tight timing: first word start → last word end + small pad
            c_start = chunk_words[0]["start"] - segment.start
            c_end = chunk_words[-1]["end"] - segment.start + TAIL_PAD

            # Clamp to valid range
            c_start = max(0, c_start)
            c_end = max(c_start + 0.1, c_end)

            # Don't let this chunk bleed into the next chunk's start
            if i + max_words < len(seg_words):
                next_start = seg_words[i + max_words]["start"] - segment.start
                c_end = min(c_end, next_start)

            chunk_text = " ".join(w["word"] for w in chunk_words)
            escaped = _escape_drawtext(chunk_text.upper())

            filters.append(
                f"drawtext=text='{escaped}'"
                f":fontsize=80"
                f":fontcolor=white"
                f":fontfile={FONT}"
                f":borderw=4"
                f":bordercolor=black"
                f":x=(w-text_w)/2"
                f":y=h*0.78"
                f":enable='between(t\\,{c_start:.3f}\\,{c_end:.3f})'"
            )
    else:
        # Fallback: use ACTUAL segment timestamps from Whisper (not even distribution).
        # This ensures captions only appear when speech was detected.
        for seg in transcript.get("segments", []):
            seg_start = seg["start"]
            seg_end = seg["end"]

            # Skip segments outside our edit window
            if seg_end <= segment.start or seg_start >= segment.end:
                continue

            # Clamp to edit window
            seg_start = max(seg_start, segment.start)
            seg_end = min(seg_end, segment.end)
            rel_start = seg_start - segment.start
            rel_end = seg_end - segment.start

            text = seg["text"].strip()
            if not text:
                continue

            words = text.split()
            if not words:
                continue

            chunks = []
            for i in range(0, len(words), max_words):
                chunks.append(" ".join(words[i:i + max_words]))

            if not chunks:
                continue

            # Distribute chunks proportionally within this SEGMENT's timespan only.
            # Crucially, this only covers the time when speech is happening,
            # not the silence between segments.
            chunk_duration = (rel_end - rel_start) / len(chunks)

            for i, chunk in enumerate(chunks):
                c_start = rel_start + i * chunk_duration
                c_end = rel_start + (i + 1) * chunk_duration
                escaped = _escape_drawtext(chunk.upper())

                filters.append(
                    f"drawtext=text='{escaped}'"
                    f":fontsize=80"
                    f":fontcolor=white"
                    f":fontfile={FONT}"
                    f":borderw=4"
                    f":bordercolor=black"
                    f":x=(w-text_w)/2"
                    f":y=h*0.78"
                    f":enable='between(t\\,{c_start:.3f}\\,{c_end:.3f})'"
                )

    return ",".join(filters) if filters else ""


def _strip_emojis(text: str) -> str:
    return re.sub(
        r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF'
        r'\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U000024C2-\U0001F251'
        r'\U0001f900-\U0001f9FF\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF'
        r'\U00002600-\U000026FF\U0000FE0F\U0000200D]+',
        '', text
    ).strip()


def _get_title(ed: EditDecision, clip_meta: ClipMeta) -> str:
    if ed.post_copy:
        for pk in ["shorts", "youtube_shorts", "tiktok", "reels"]:
            pc = ed.post_copy.get(pk)
            if pc and pc.title and pc.title.strip():
                return pc.title.strip()
    if clip_meta and clip_meta.title:
        return clip_meta.title.strip()
    return ""


def _build_title_filters(title: str, duration: float) -> str:
    if not title:
        return ""

    title = _strip_emojis(title.strip())
    if not title:
        return ""
    lines = textwrap.wrap(title, width=25)
    if not lines:
        return ""
    lines = lines[:3]

    filters = []
    FONT = "/System/Library/Fonts/Supplemental/Impact.ttf"
    FONTSIZE = 72
    line_height = 90
    base_y = 100

    for i, line in enumerate(lines):
        escaped = _escape_drawtext(line.upper())
        y_pos = base_y + i * line_height

        filters.append(
            f"drawtext=text='{escaped}'"
            f":fontsize={FONTSIZE}"
            f":fontcolor=white"
            f":fontfile={FONT}"
            f":borderw=4"
            f":bordercolor=black"
            f":x=(w-text_w)/2"
            f":y={y_pos}"
            f":box=1"
            f":boxcolor=black@0.55"
            f":boxborderw=12"
            f":enable='between(t\\,0.0\\,{duration:.1f})'"
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

    clip_meta = ClipMeta.model_validate_json(row["metadata_json"])
    clip_title = _get_title(ed, clip_meta)

    log.info(f"Rendering: clip {clip_row_id} ({row['platform']}/{row['clip_id'][:30]}...)")
    log.info(f"  Segment: {ed.segment.start:.1f}s -> {ed.segment.end:.1f}s")
    if clip_title:
        log.info(f"  Title: {clip_title[:60]}")

    probe = await probe_video(source_path)
    src_w = probe.get("width", 1920)
    src_h = probe.get("height", 1080)
    log.info(f"  Source: {src_w}x{src_h}")

    clip_dir = Path(source_path).parent
    output_path = clip_dir / "rendered.mp4"
    segment_duration = ed.segment.end - ed.segment.start

    # Build blurred background + overlay filter
    video_layout = _build_video_filter(src_w, src_h)

    caption_chain = _build_caption_filters(
        transcript, ed.segment,
        max_words=ed.captions.max_words,
    )

    title_filters = _build_title_filters(clip_title, duration=segment_duration)

    # Build the full filter_complex:
    # video_layout produces the base 1080x1920 frame
    # then chain drawtext filters for captions and title
    drawtext_chain = ""
    if caption_chain:
        drawtext_chain += "," + caption_chain
    if title_filters:
        drawtext_chain += "," + title_filters

    # Music handling
    music_path = None
    try:
        from src.render.music_mixer import get_music_track
        music_path = get_music_track(mood="funny")
    except ImportError:
        pass

    # Video chain: blur + overlay + captions + title -> [vout]
    video_chain = video_layout + drawtext_chain + "[vout]"

    # Audio chain
    fade_start = max(0, segment_duration - 2.0)
    if music_path:
        audio_chain = (
            f"[0:a]loudnorm=I=-14:TP=-1:LRA=11[speech];"
            f"[1:a]atrim=0:{segment_duration:.1f},"
            f"afade=t=in:st=0:d=1.0,"
            f"afade=t=out:st={fade_start:.1f}:d=2.0,"
            f"volume=0.10[music];"
            f"[speech][music]amix=inputs=2:duration=first:dropout_transition=2[aout]"
        )
    else:
        audio_chain = "[0:a]loudnorm=I=-14:TP=-1:LRA=11[aout]"

    # Combine into single filter_complex
    full_filter = video_chain + ";" + audio_chain

    filter_script = clip_dir / "filter_script.txt"
    with open(filter_script, "w") as f:
        f.write(full_filter)

    # Build command
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(ed.segment.start),
        "-i", source_path,
    ]
    if music_path:
        cmd += ["-i", music_path]
    cmd += [
        "-t", str(segment_duration),
        "-filter_complex_script", str(filter_script),
        "-map", "[vout]",
        "-map", "[aout]",
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "44100",
        "-movflags", "+faststart",
        str(output_path),
    ]

    has_music = " + music" if music_path else ""
    has_word_ts = " + word-sync" if transcript.get("words") else ""
    log.info(f"  Running ffmpeg (blur layout + captions{has_word_ts}{has_music})...")

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        err_text = stderr.decode()[-800:]
        log.error(f"  ffmpeg failed:\n{err_text}")

        # Fallback: simple scale + pad (no blur, no split = safe with -vf/-af)
        log.info("  Retrying with simple layout...")
        vf_simple = "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2"
        if caption_chain:
            vf_simple += "," + caption_chain
        if title_filters:
            vf_simple += "," + title_filters

        fallback_script = clip_dir / "filter_fallback.txt"
        with open(fallback_script, "w") as f:
            f.write(vf_simple)

        cmd_simple = [
            "ffmpeg", "-y",
            "-ss", str(ed.segment.start),
            "-i", source_path,
            "-t", str(segment_duration),
            "-filter_script:v", str(fallback_script),
            "-af", "loudnorm=I=-14:TP=-1:LRA=11",
            "-c:v", "libx264", "-preset", "medium", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
            "-movflags", "+faststart",
            str(output_path),
        ]
        proc2 = await asyncio.create_subprocess_exec(
            *cmd_simple,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr2 = await proc2.communicate()
        if proc2.returncode != 0:
            log.error(f"  Simple layout also failed:\n{stderr2.decode()[-500:]}")

            # Last resort: no captions at all
            log.info("  Retrying without captions...")
            cmd_bare = [
                "ffmpeg", "-y",
                "-ss", str(ed.segment.start),
                "-i", source_path,
                "-t", str(segment_duration),
                "-vf", "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2",
                "-af", "loudnorm=I=-14:TP=-1:LRA=11",
                "-c:v", "libx264", "-preset", "medium", "-crf", "23",
                "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
                "-movflags", "+faststart",
                str(output_path),
            ]
            proc3 = await asyncio.create_subprocess_exec(
                *cmd_bare,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr3 = await proc3.communicate()
            if proc3.returncode != 0:
                log.error(f"  All render attempts failed:\n{stderr3.decode()[-500:]}")
                db.execute("""
                    UPDATE clips SET status = ?, fail_reason = 'render_failed', updated_at = datetime('now')
                    WHERE id = ?
                """, (ClipStatus.FAILED.value, clip_row_id))
                db.commit()
                db.close()
                return False
            else:
                log.warning("  Rendered WITHOUT captions or blur (bare fallback)")
        else:
            log.warning("  Rendered with simple layout (blur failed)")

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