"""Download clip media files to local assets dir."""
import asyncio
import json
import os
import subprocess
from pathlib import Path
from src.db.database import get_db
from src.models.schemas import ClipMeta, ClipStatus
from src.config import settings
from src.utils.http import download_file
from src.utils.log import log


def _asset_dir_for_clip(platform: str, clip_id: str) -> Path:
    """Get/create asset directory for a clip."""
    d = settings.assets_path / platform / clip_id
    d.mkdir(parents=True, exist_ok=True)
    return d


async def download_twitch_clip(clip_meta: ClipMeta, dest_dir: Path) -> str | None:
    """Download a Twitch clip MP4."""
    if not clip_meta.download_url:
        log.error(f"No download URL for Twitch clip {clip_meta.clip_id}")
        return None

    dest = dest_dir / "source.mp4"
    ok = await download_file(clip_meta.download_url, str(dest))
    if ok and dest.exists() and dest.stat().st_size > 0:
        return str(dest)
    return None


async def download_kick_clip(clip_meta: ClipMeta, dest_dir: Path) -> str | None:
    """
    Download a Kick clip. Kick serves HLS (.m3u8), so we use ffmpeg to convert.
    Falls back to direct download if it's an MP4 URL.
    """
    url = clip_meta.download_url
    if not url:
        log.error(f"No download URL for Kick clip {clip_meta.clip_id}")
        return None

    dest = dest_dir / "source.mp4"

    # If it's already an MP4, direct download
    if url.endswith(".mp4"):
        ok = await download_file(url, str(dest))
        if ok and dest.exists() and dest.stat().st_size > 0:
            return str(dest)
        return None

    # HLS → MP4 via ffmpeg
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y",
            "-i", url,
            "-c", "copy",
            "-bsf:a", "aac_adtstoasc",
            str(dest),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode == 0 and dest.exists() and dest.stat().st_size > 0:
            return str(dest)
        else:
            log.error(f"ffmpeg failed for {url}: {stderr.decode()[-500:]}")
            return None
    except FileNotFoundError:
        log.error("ffmpeg not found! Install with: brew install ffmpeg")
        return None


async def download_clip(clip_row_id: int) -> bool:
    """
    Download a single DISCOVERED clip. Updates DB status.
    Returns True on success.
    """
    db = get_db()
    row = db.execute("SELECT * FROM clips WHERE id = ? AND status = ?",
                     (clip_row_id, ClipStatus.DISCOVERED.value)).fetchone()
    if not row:
        log.warning(f"Clip {clip_row_id} not found or not in DISCOVERED state")
        return False

    clip_meta = ClipMeta.model_validate_json(row["metadata_json"])
    dest_dir = _asset_dir_for_clip(row["platform"], row["clip_id"])

    log.info(f"Downloading: {clip_meta.title} ({row['platform']}/{row['clip_id']})")

    path: str | None = None
    if row["platform"] == "twitch":
        path = await download_twitch_clip(clip_meta, dest_dir)
    elif row["platform"] == "kick":
        path = await download_kick_clip(clip_meta, dest_dir)

    if path:
        paths = json.loads(row["paths_json"]) if row["paths_json"] != '{}' else {}
        paths["source"] = path
        db.execute("""
            UPDATE clips SET
                status = ?,
                paths_json = ?,
                updated_at = datetime('now')
            WHERE id = ?
        """, (ClipStatus.DOWNLOADED.value, json.dumps(paths), clip_row_id))
        db.commit()
        db.close()
        log.info(f"  ✅ Downloaded to {path}")
        return True
    else:
        db.execute("""
            UPDATE clips SET
                status = ?,
                fail_reason = 'download_failed',
                updated_at = datetime('now')
            WHERE id = ?
        """, (ClipStatus.FAILED.value, clip_row_id))
        db.commit()
        db.close()
        log.error(f"  ❌ Download failed for clip {clip_row_id}")
        return False


async def download_discovered_clips(profile_slug: str, limit: int = 5) -> int:
    """Download all DISCOVERED clips for a profile, up to limit."""
    db = get_db()
    rows = db.execute("""
        SELECT cl.id FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        WHERE p.slug = ? AND cl.status = ?
        ORDER BY cl.created_at ASC
        LIMIT ?
    """, (profile_slug, ClipStatus.DISCOVERED.value, limit)).fetchall()
    db.close()

    count = 0
    for row in rows:
        ok = await download_clip(row["id"])
        if ok:
            count += 1
        await asyncio.sleep(settings.request_delay_sec)

    return count
