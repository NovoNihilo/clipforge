"""Kick clip discovery via unofficial public API."""
import asyncio
from datetime import datetime, timedelta, timezone
from src.config import settings
from src.utils.http import fetch_json
from src.utils.log import log
from src.models.schemas import ClipMeta

KICK_API_BASE = "https://kick.com/api/v2"


async def fetch_channel_info(slug: str) -> dict | None:
    """Get channel info to verify slug exists."""
    data = await fetch_json(
        f"{KICK_API_BASE}/channels/{slug}",
        headers={"Accept": "application/json"},
    )
    return data


async def fetch_clips(
    channel_slug: str,
    sort: str = "recent",  # 'recent' | 'popular'
    page: int = 1,
    limit: int = 20,
) -> list[ClipMeta]:
    """
    Fetch clips for a Kick channel.
    Uses the unofficial /api/v2/channels/{slug}/clips endpoint.
    """
    # Try the known unofficial endpoint
    data = await fetch_json(
        f"{KICK_API_BASE}/channels/{channel_slug}/clips",
        headers={"Accept": "application/json"},
        params={"sort": sort, "page": str(page), "limit": str(limit)},
    )

    if not data:
        # Fallback: try the /api/v1 endpoint
        data = await fetch_json(
            f"https://kick.com/api/v1/channels/{channel_slug}/clips",
            headers={"Accept": "application/json"},
            params={"sort": sort, "page": str(page), "limit": str(limit)},
        )

    if not data:
        log.warning(f"No clip data returned for Kick channel: {channel_slug}")
        return []

    # Handle different response shapes
    clip_list = []
    if isinstance(data, dict):
        clip_list = data.get("clips", data.get("data", []))
    elif isinstance(data, list):
        clip_list = data

    clips = []
    for c in clip_list:
        clip_id = str(c.get("id", ""))
        if not clip_id:
            continue

        # Kick clip video URLs
        clip_url = c.get("clip_url", "")  # Usually HLS .m3u8
        thumbnail = c.get("thumbnail_url", c.get("thumbnail", ""))

        clips.append(ClipMeta(
            clip_id=clip_id,
            platform="kick",
            title=c.get("title", ""),
            creator_name=channel_slug,
            duration_sec=float(c.get("duration", 0)),
            view_count=int(c.get("views", c.get("view_count", 0))),
            created_at=c.get("created_at", ""),
            thumbnail_url=thumbnail,
            download_url=clip_url,
            language=c.get("language", "en"),
            game_name=c.get("category", {}).get("name", "") if isinstance(c.get("category"), dict) else "",
            raw=c,
        ))

    return clips


async def discover_clips_for_creator(
    channel_slug: str,
    last_fetched_at: str | None = None,
    max_clips: int = 10,
) -> list[ClipMeta]:
    """
    Get recent clips, filter by last_fetched_at if set.
    Kick doesn't support started_at param, so we fetch recent + filter client-side.
    """
    all_clips: list[ClipMeta] = []
    page = 1

    while len(all_clips) < max_clips and page <= 3:  # Max 3 pages to be safe
        batch = await fetch_clips(channel_slug, sort="recent", page=page)
        if not batch:
            break

        for clip in batch:
            # Filter by cursor time
            if last_fetched_at and clip.created_at:
                try:
                    clip_time = datetime.fromisoformat(clip.created_at.replace("Z", "+00:00"))
                    cursor_time = datetime.fromisoformat(last_fetched_at.replace("Z", "+00:00"))
                    if clip_time <= cursor_time:
                        continue
                except (ValueError, TypeError):
                    pass
            all_clips.append(clip)

        page += 1
        await asyncio.sleep(settings.request_delay_sec)

    return all_clips[:max_clips]
