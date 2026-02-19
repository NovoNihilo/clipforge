"""Kick clip discovery using KickApi package."""
import asyncio
from datetime import datetime
from kickapi import KickAPI
from src.config import settings
from src.utils.log import log
from src.models.schemas import ClipMeta

kick = KickAPI()


def _fetch_channel_clips_sync(channel_slug: str) -> list[ClipMeta]:
    """Synchronous fetch using KickApi package."""
    try:
        channel = kick.channel(channel_slug)
    except Exception as e:
        log.warning(f"Kick channel not found: {channel_slug} — {e}")
        return []

    clips = []
    try:
        for c in channel.clips:
            clip_url = getattr(c, 'stream', '') or getattr(c, 'clip_url', '') or ''
            thumbnail = getattr(c, 'thumbnail', '') or getattr(c, 'thumbnail_url', '') or ''
            creator_name = channel_slug
            if hasattr(c, 'creator') and c.creator:
                creator_name = getattr(c.creator, 'username', channel_slug)

            clips.append(ClipMeta(
                clip_id=str(c.id),
                platform="kick",
                title=getattr(c, 'title', '') or '',
                creator_name=creator_name,
                duration_sec=float(getattr(c, 'duration', 0) or 0),
                view_count=int(getattr(c, 'views', 0) or getattr(c, 'view_count', 0) or 0),
                created_at=getattr(c, 'created_at', '') or '',
                thumbnail_url=thumbnail,
                download_url=clip_url,
                language='en',
                game_name=getattr(c.category, 'name', '') if hasattr(c, 'category') and c.category else '',
                raw={},
            ))
    except Exception as e:
        log.warning(f"Error fetching clips for {channel_slug}: {e}")

    return clips


async def discover_clips_for_creator(
    channel_slug: str,
    last_fetched_at: str | None = None,
    max_clips: int = 10,
) -> list[ClipMeta]:
    """Get recent clips, filter by last_fetched_at if set."""
    # Run sync KickApi in executor to not block event loop
    loop = asyncio.get_event_loop()
    all_clips = await loop.run_in_executor(None, _fetch_channel_clips_sync, channel_slug)

    # Filter by cursor
    if last_fetched_at:
        filtered = []
        for clip in all_clips:
            if clip.created_at:
                try:
                    clip_time = datetime.fromisoformat(clip.created_at.replace("Z", "+00:00"))
                    cursor_time = datetime.fromisoformat(last_fetched_at.replace("Z", "+00:00"))
                    if clip_time > cursor_time:
                        filtered.append(clip)
                except (ValueError, TypeError):
                    filtered.append(clip)
            else:
                filtered.append(clip)
        all_clips = filtered

    return all_clips[:max_clips]