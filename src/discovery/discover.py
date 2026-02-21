"""Discovery orchestrator: discover clips for all tracked creators in a profile."""
import asyncio
import json
from datetime import datetime, timezone
from src.db.database import get_db
from src.discovery import twitch_api, kick_api
from src.models.schemas import ClipMeta, ClipStatus, ProfileRules
from src.config import settings
from src.utils.log import log


async def discover_for_profile(profile_slug: str, max_per_creator: int | None = None) -> list[dict]:
    """
    Discover new clips for all enabled creators in a profile.
    Returns list of newly inserted clip rows.
    """
    db = get_db()

    # Load profile
    row = db.execute("SELECT * FROM profiles WHERE slug = ?", (profile_slug,)).fetchone()
    if not row:
        log.error(f"Profile not found: {profile_slug}")
        return []

    profile_id = row["id"]
    rules = ProfileRules.model_validate_json(row["rules_json"])
    per_creator_max = max_per_creator or rules.max_clips_per_creator_per_run

    # Get enabled creators for this profile
    creators = db.execute("""
        SELECT c.*, pc.is_enabled
        FROM creators c
        JOIN profile_creators pc ON pc.creator_id = c.id
        WHERE pc.profile_id = ? AND pc.is_enabled = 1
    """, (profile_id,)).fetchall()

    if not creators:
        log.warning(f"No enabled creators for profile: {profile_slug}")
        return []

    new_clips = []

    for creator in creators:
        creator_id = creator["id"]
        platform = creator["platform"]
        platform_user_id = creator["platform_user_id"]
        display_name = creator["display_name"]

        # Load cursor
        cursor_row = db.execute(
            "SELECT * FROM cursors WHERE creator_id = ?", (creator_id,)
        ).fetchone()
        last_fetched = cursor_row["last_fetched_at"] if cursor_row else None

        log.info(f"Discovering clips: {display_name} ({platform}) since {last_fetched or 'never'}")

        # Fetch clips based on platform
        clips: list[ClipMeta] = []
        try:
            if platform == "twitch":
                clips = await twitch_api.discover_clips_for_creator(
                    broadcaster_id=platform_user_id,
                    last_fetched_at=last_fetched,
                    max_clips=per_creator_max,
                )
            elif platform == "kick":
                clips = await kick_api.discover_clips_for_creator(
                    channel_slug=platform_user_id,
                    last_fetched_at=last_fetched,
                    max_clips=per_creator_max,
                )
            else:
                log.warning(f"Unknown platform: {platform}")
                continue
        except Exception as e:
            log.error(f"Discovery failed for {display_name}: {e}")
            continue

        if not clips:
            log.info(f"  No new clips for {display_name}")
            continue

        # Insert clips (dedupe via UNIQUE constraint)
        newest_time = last_fetched
        for clip in clips:
            try:
                db.execute("""
                    INSERT OR IGNORE INTO clips
                    (platform, clip_id, creator_id, profile_id, status, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    clip.platform,
                    clip.clip_id,
                    creator_id,
                    profile_id,
                    ClipStatus.DISCOVERED.value,
                    clip.to_json(),
                ))
                cursor = db.execute("INSERT OR IGNORE INTO clips ...")
                if cursor.rowcount > 0:
                    new_clips.append(...)({
                        "clip_id": clip.clip_id,
                        "platform": clip.platform,
                        "title": clip.title,
                        "creator": display_name,
                        "views": clip.view_count,
                        "duration": clip.duration_sec,
                    })

                # Track newest clip time for cursor
                if clip.created_at:
                    if not newest_time or clip.created_at > newest_time:
                        newest_time = clip.created_at

            except Exception as e:
                log.warning(f"  Skipped clip {clip.clip_id}: {e}")

        # Update cursor
        if newest_time:
            db.execute("""
                INSERT INTO cursors (creator_id, last_fetched_at, updated_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(creator_id) DO UPDATE SET
                    last_fetched_at = excluded.last_fetched_at,
                    updated_at = datetime('now')
            """, (creator_id, newest_time))

        db.commit()
        log.info(f"  Found {len(clips)} clips, {len(new_clips)} new for {display_name}")

        # Rate limit between creators
        await asyncio.sleep(settings.request_delay_sec)

    db.close()
    return new_clips
