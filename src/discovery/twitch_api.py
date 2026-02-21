"""Twitch Helix API: auth + clip fetching."""
import asyncio
from datetime import datetime, timedelta, timezone
from src.config import settings
from src.utils.http import fetch_json
from src.utils.log import log
from src.models.schemas import ClipMeta

# Twitch OAuth2 app access token
_token_cache: dict = {"token": None, "expires_at": 0}

HELIX_BASE = "https://api.twitch.tv/helix"


async def get_app_token() -> str | None:
    """Get/refresh Twitch app access token (client credentials flow)."""
    import time
    if _token_cache["token"] and time.time() < _token_cache["expires_at"] - 60:
        return _token_cache["token"]

    if not settings.twitch_client_id or not settings.twitch_client_secret:
        log.error("TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET required in .env")
        return None

    import httpx
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://id.twitch.tv/oauth2/token",
            data={
                "client_id": settings.twitch_client_id,
                "client_secret": settings.twitch_client_secret,
                "grant_type": "client_credentials",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        _token_cache["token"] = data["access_token"]
        _token_cache["expires_at"] = time.time() + data.get("expires_in", 3600)
        log.info("Twitch app token acquired")
        return _token_cache["token"]


def _helix_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Client-Id": settings.twitch_client_id,
    }


async def get_broadcaster_id(login: str) -> str | None:
    """Look up Twitch user ID from login name."""
    token = await get_app_token()
    if not token:
        return None
    data = await fetch_json(
        f"{HELIX_BASE}/users",
        headers=_helix_headers(token),
        params={"login": login},
    )
    if data and data.get("data"):
        return data["data"][0]["id"]
    log.warning(f"Twitch user not found: {login}")
    return None


async def fetch_clips(
    broadcaster_id: str,
    started_at: str | None = None,
    first: int = 20,
    after: str | None = None,
) -> tuple[list[ClipMeta], str | None]:
    """
    Fetch clips for a broadcaster.
    Returns (clips, pagination_cursor).
    started_at: ISO datetime string — only get clips after this time.
    """
    token = await get_app_token()
    if not token:
        return [], None

    params: dict = {"broadcaster_id": broadcaster_id, "first": min(first, 100)}
    if started_at:
        params["started_at"] = started_at
    if after:
        params["after"] = after

    data = await fetch_json(
        f"{HELIX_BASE}/clips",
        headers=_helix_headers(token),
        params=params,
    )
    if not data:
        return [], None

    clips = []
    for c in data.get("data", []):
        # Twitch clip download URL hack: thumbnail URL contains the clip video URL
        thumb = c.get("thumbnail_url", "")
        # thumbnail format: https://clips-media-assets2.twitch.tv/xxx-preview-480x272.jpg
        # video URL: everything before -preview + .mp4
        # Extract download URL from thumbnail
        # Format: https://clips-media-assets2.twitch.tv/AT-cm%7Cxxx-preview-480x272.jpg
        download_url = ""
        if "-preview-" in thumb:
            download_url = thumb.split("-preview-")[0] + ".mp4"
        elif thumb:
            # Fallback: try removing the file extension portion
            base = thumb.rsplit("/", 1)
            if len(base) == 2:
                download_url = base[0] + "/" + base[1].split("-")[0] + ".mp4"

        clips.append(ClipMeta(
            clip_id=c["id"],
            platform="twitch",
            title=c.get("title", ""),
            creator_name=c.get("broadcaster_name", ""),
            duration_sec=c.get("duration", 0),
            view_count=c.get("view_count", 0),
            created_at=c.get("created_at", ""),
            thumbnail_url=thumb,
            download_url=download_url,
            language=c.get("language", "en"),
            game_name=c.get("game_id", ""),
            raw=c,
        ))

    cursor = None
    if data.get("pagination", {}).get("cursor"):
        cursor = data["pagination"]["cursor"]

    return clips, cursor


async def discover_clips_for_creator(
    broadcaster_id: str,
    last_fetched_at: str | None = None,
    max_clips: int = 10,
) -> list[ClipMeta]:
    """
    Get new clips since last_fetched_at.
    If no cursor, defaults to last 24 hours.
    """
    if not last_fetched_at:
        # Default: clips from last 24 hours
        cutoff = datetime.now(timezone.utc) - timedelta(days=1)
        last_fetched_at = cutoff.isoformat()

    all_clips: list[ClipMeta] = []
    cursor = None

    while len(all_clips) < max_clips:
        batch, cursor = await fetch_clips(
            broadcaster_id=broadcaster_id,
            started_at=last_fetched_at,
            first=min(20, max_clips - len(all_clips)),
            after=cursor,
        )
        all_clips.extend(batch)
        if not cursor or not batch:
            break
        await asyncio.sleep(settings.request_delay_sec)

    return all_clips[:max_clips]