"""
Milestone 4 v8: Render shorts.

Changes from v7:
  - 2-word caption chunks (configurable via caption_max_words)
  - Speaker diarization: different caption colors per speaker
  - Default caption color: yellow (was white)
  - Speaker color rotation: yellow → cyan → pink → green
  - Graceful fallback: if diarization unavailable, all captions use yellow
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
from src.moderation.content_mod import get_bleep_map, BLEEP_WORDS

from pathlib import Path as _Path

def _get_font() -> str:
    """Find Impact font with Linux/Docker fallback."""
    for candidate in [
        "/System/Library/Fonts/Supplemental/Impact.ttf",        # macOS
        "/usr/share/fonts/truetype/msttcorefonts/Impact.ttf",   # Linux (ttf-mscorefonts)
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", # Linux fallback
    ]:
        if _Path(candidate).exists():
            return candidate
    return "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"  # best guess

FONT_PATH = _get_font()


# ── Speaker color palette ─────────────────────────────────────────────────────
# ffmpeg drawtext fontcolor values
SPEAKER_COLORS = [
    "yellow",       # SPEAKER_00 (default / primary speaker)
    "cyan",         # SPEAKER_01
    "#FF69B4",      # SPEAKER_02 (pink)
    "#00FF7F",      # SPEAKER_03 (green)
]

def _speaker_color(speaker_id: str) -> str:
    """Map speaker label to color. Defaults to yellow."""
    try:
        idx = int(speaker_id.split("_")[-1])
        return SPEAKER_COLORS[idx % len(SPEAKER_COLORS)]
    except (ValueError, IndexError):
        return SPEAKER_COLORS[0]


def _clean_word(word: str) -> str:
    return re.sub(r'[^a-z]', '', word.lower())


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


def _censor_word(word: str) -> str:
    """Replace profanity with [BLEEP] for caption display."""
    if _clean_word(word) in BLEEP_WORDS:
        return "[BLEEP]"
    return word


def _build_caption_filters(
    transcript: dict,
    segment: Segment,
    max_words: int = 2,
    speaker_words: list[dict] | None = None,
) -> str:
    """
    Build caption filters with:
      - Configurable words-per-chunk (default 2)
      - Per-speaker colors when diarization is available
      - Profanity replaced by [BLEEP]
    """
    filters = []
    FONT = FONT_PATH
    TAIL_PAD = 0.15

    has_word_timestamps = bool(transcript.get("words"))

    if has_word_timestamps:
        # Use speaker-annotated words if available, else plain transcript words
        words = speaker_words if speaker_words else transcript["words"]
        seg_words = [
            w for w in words
            if w["end"] > segment.start and w["start"] < segment.end
        ]

        for i in range(0, len(seg_words), max_words):
            chunk_words = seg_words[i:i + max_words]
            if not chunk_words:
                continue

            c_start = chunk_words[0]["start"] - segment.start
            c_end = chunk_words[-1]["end"] - segment.start + TAIL_PAD

            c_start = max(0, c_start)
            c_end = max(c_start + 0.1, c_end)

            # Clamp to next chunk start
            if i + max_words < len(seg_words):
                next_start = seg_words[i + max_words]["start"] - segment.start
                c_end = min(c_end, next_start)

            # Determine color from the first word's speaker in this chunk
            speaker = chunk_words[0].get("speaker", "SPEAKER_00")
            color = _speaker_color(speaker)

            # Censor profanity in caption text
            chunk_text = " ".join(_censor_word(w["word"]) for w in chunk_words)
            escaped = _escape_drawtext(chunk_text.upper())

            filters.append(
                f"drawtext=text='{escaped}'"
                f":fontsize=80"
                f":fontcolor={color}"
                f":fontfile={FONT}"
                f":borderw=4"
                f":bordercolor=black"
                f":x=(w-text_w)/2"
                f":y=h*0.78"
                f":enable='between(t\\,{c_start:.3f}\\,{c_end:.3f})'"
            )
    else:
        # Fallback: no word timestamps — use segment-level timing, default yellow
        for seg in transcript.get("segments", []):
            seg_start = seg["start"]
            seg_end = seg["end"]

            if seg_end <= segment.start or seg_start >= segment.end:
                continue

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

            censored_words = [_censor_word(w) for w in words]

            chunks = []
            for ci in range(0, len(censored_words), max_words):
                chunks.append(" ".join(censored_words[ci:ci + max_words]))

            if not chunks:
                continue

            chunk_duration = (rel_end - rel_start) / len(chunks)

            for ci, chunk in enumerate(chunks):
                c_start = rel_start + ci * chunk_duration
                c_end = rel_start + (ci + 1) * chunk_duration
                escaped = _escape_drawtext(chunk.upper())

                filters.append(
                    f"drawtext=text='{escaped}'"
                    f":fontsize=80"
                    f":fontcolor=yellow"
                    f":fontfile={FONT}"
                    f":borderw=4"
                    f":bordercolor=black"
                    f":x=(w-text_w)/2"
                    f":y=h*0.78"
                    f":enable='between(t\\,{c_start:.3f}\\,{c_end:.3f})'"
                )

    return ",".join(filters) if filters else ""


def _build_bleep_audio_filter(
    bleep_map: list[dict],
    segment_start: float,
) -> str:
    """Build ffmpeg volume filter that mutes audio at bleep timestamps."""
    if not bleep_map:
        return ""

    conditions = []
    for b in bleep_map:
        rel_start = b["start"] - segment_start
        rel_end = b["end"] - segment_start
        rel_start = max(0, rel_start - 0.05)
        rel_end = rel_end + 0.05
        conditions.append(f"between(t\\,{rel_start:.3f}\\,{rel_end:.3f})")

    enable_expr = "+".join(conditions)
    return f"volume=0:enable='{enable_expr}'"


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
    FONT = FONT_PATH
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

    # Get bleep map for this segment
    bleep_map = get_bleep_map(transcript, ed.segment.start, ed.segment.end)

    # ── Speaker diarization (optional) ────────────────────────────────────
    speaker_words = None
    try:
        from src.render.diarize import diarize_speakers, assign_speakers_to_words

        diarization_segments = diarize_speakers(
            source_path,
            segment_start=ed.segment.start,
            segment_end=ed.segment.end,
            min_speakers=1,
            max_speakers=4,
        )
        if diarization_segments:
            # Deep copy words to avoid mutating transcript
            import copy
            words_copy = copy.deepcopy(transcript.get("words", []))
            speaker_words = assign_speakers_to_words(
                words_copy,
                diarization_segments,
                segment_start=ed.segment.start,
                segment_end=ed.segment.end,
            )
            unique = set(w.get("speaker", "SPEAKER_00") for w in speaker_words)
            if len(unique) > 1:
                log.info(f"  🎨 Speaker colors: {len(unique)} speakers → multi-color captions")
            else:
                log.info(f"  🎨 Single speaker → yellow captions")
    except ImportError:
        log.info("  Speaker diarization not available (pyannote not installed) → yellow captions")
    except Exception as e:
        log.warning(f"  Speaker diarization failed: {e} → yellow captions")

    # Build video filters
    video_layout = _build_video_filter(src_w, src_h)

    caption_chain = _build_caption_filters(
        transcript, ed.segment,
        max_words=ed.captions.max_words,
        speaker_words=speaker_words,
    )

    title_filters = _build_title_filters(clip_title, duration=segment_duration)

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

    # Audio chain with bleeping
    fade_start = max(0, segment_duration - 2.0)
    bleep_filter = _build_bleep_audio_filter(bleep_map, ed.segment.start)

    if music_path:
        if bleep_filter:
            audio_chain = (
                f"[0:a]loudnorm=I=-14:TP=-1:LRA=11,{bleep_filter}[speech];"
                f"[1:a]atrim=0:{segment_duration:.1f},"
                f"afade=t=in:st=0:d=1.0,"
                f"afade=t=out:st={fade_start:.1f}:d=2.0,"
                f"volume=0.10[music];"
                f"[speech][music]amix=inputs=2:duration=first:dropout_transition=2[aout]"
            )
        else:
            audio_chain = (
                f"[0:a]loudnorm=I=-14:TP=-1:LRA=11[speech];"
                f"[1:a]atrim=0:{segment_duration:.1f},"
                f"afade=t=in:st=0:d=1.0,"
                f"afade=t=out:st={fade_start:.1f}:d=2.0,"
                f"volume=0.10[music];"
                f"[speech][music]amix=inputs=2:duration=first:dropout_transition=2[aout]"
            )
    else:
        if bleep_filter:
            audio_chain = f"[0:a]loudnorm=I=-14:TP=-1:LRA=11,{bleep_filter}[aout]"
        else:
            audio_chain = "[0:a]loudnorm=I=-14:TP=-1:LRA=11[aout]"

    full_filter = video_chain + ";" + audio_chain

    filter_script = clip_dir / "filter_script.txt"
    with open(filter_script, "w") as f:
        f.write(full_filter)

    # Build command — BUG 4 FIX: -t is now BEFORE -i source_path
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(ed.segment.start),
        "-t", str(segment_duration),
        "-i", source_path,
    ]
    if music_path:
        cmd += ["-i", music_path]
    cmd += [
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
    has_bleeps = f" + {len(bleep_map)} bleeps" if bleep_map else ""
    has_speakers = f" + {len(set(w.get('speaker','') for w in (speaker_words or [])))} speakers" if speaker_words else ""
    log.info(f"  Running ffmpeg (blur + captions{has_word_ts}{has_bleeps}{has_speakers}{has_music})...")

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        err_text = stderr.decode()[-800:]
        log.error(f"  ffmpeg failed:\n{err_text}")

        # Fallback: simple layout (no blur)
        log.info("  Retrying with simple layout...")
        vf_simple = "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2"
        if caption_chain:
            vf_simple += "," + caption_chain
        if title_filters:
            vf_simple += "," + title_filters

        fallback_script = clip_dir / "filter_fallback.txt"
        with open(fallback_script, "w") as f:
            f.write(vf_simple)

        af_simple = "loudnorm=I=-14:TP=-1:LRA=11"
        if bleep_filter:
            af_simple += f",{bleep_filter}"

        cmd_simple = [
            "ffmpeg", "-y",
            "-ss", str(ed.segment.start),
            "-t", str(segment_duration),
            "-i", source_path,
            "-filter_script:v", str(fallback_script),
            "-af", af_simple,
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

            log.info("  Retrying without captions...")
            cmd_bare = [
                "ffmpeg", "-y",
                "-ss", str(ed.segment.start),
                "-t", str(segment_duration),
                "-i", source_path,
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
                log.warning("  Rendered WITHOUT captions or bleeps (bare fallback)")
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