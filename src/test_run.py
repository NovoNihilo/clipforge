"""
ClipForge — Quick test run with minimal creators.

Usage:
    python -m src.test_run

Resets DB, seeds 2 creators, runs full pipeline with limit=2, top=2.
"""
import asyncio
import os
from pathlib import Path
from rich import print as rprint

from src.db.database import init_db, get_db
from src.models.schemas import ProfileRules
from src.discovery.twitch_api import get_broadcaster_id
from src.run import run_pipeline


# Just 2 creators for a quick test
TEST_CREATORS = [
    ("twitch", "botezlive", "BotezLive"),
    ("kick", "xqc", "xQc"),
]

PROFILE_SLUG = "funny-streamers"


async def seed_test_db():
    """Seed DB with profile + 2 test creators."""
    # Wipe old DB
    db_path = Path("clipforge.db")
    if db_path.exists():
        db_path.unlink()
        rprint("[yellow]Deleted old clipforge.db[/yellow]")

    db = init_db()

    rules = ProfileRules(
        niche="funny livestreamers",
        categories=["Just Chatting", "IRL", "Grand Theft Auto V", "Fortnite"],
        languages=["en"],
        length_band_sec=[12, 40],
        hook_max_delay_sec=2.0,
        silence_ratio_max=0.20,
        caption_style="bold_white",
        caption_max_words=2,
        max_clips_per_creator_per_run=10,
        hashtag_bank=["#shorts", "#funny", "#streamer", "#viral",
                      "#twitch", "#kick", "#livestream", "#clips",
                      "#gaming", "#lol"],
    )

    db.execute("""
        INSERT OR IGNORE INTO profiles (slug, name, rules_json)
        VALUES (?, ?, ?)
    """, (PROFILE_SLUG, "Funny Livestreamers", rules.model_dump_json()))
    db.commit()

    profile_id = db.execute(
        "SELECT id FROM profiles WHERE slug = ?", (PROFILE_SLUG,)
    ).fetchone()["id"]

    for platform, login, display_name in TEST_CREATORS:
        platform_user_id = login
        if platform == "twitch":
            bid = await get_broadcaster_id(login)
            if bid:
                platform_user_id = bid
                rprint(f"  Twitch {login} → {bid}")
            else:
                rprint(f"  [yellow]⚠ Could not resolve {login}, skipping[/yellow]")
                continue

        channel_url = (
            f"https://twitch.tv/{login}" if platform == "twitch"
            else f"https://kick.com/{login}"
        )

        db.execute("""
            INSERT OR IGNORE INTO creators (platform, platform_user_id, display_name, channel_url)
            VALUES (?, ?, ?, ?)
        """, (platform, platform_user_id, display_name, channel_url))
        db.commit()

        creator_row = db.execute(
            "SELECT id FROM creators WHERE platform = ? AND platform_user_id = ?",
            (platform, platform_user_id)
        ).fetchone()

        if creator_row:
            db.execute("""
                INSERT OR IGNORE INTO profile_creators (profile_id, creator_id, is_enabled)
                VALUES (?, ?, 1)
            """, (profile_id, creator_row["id"]))
            db.commit()

    creators = db.execute("""
        SELECT c.display_name, c.platform FROM creators c
        JOIN profile_creators pc ON pc.creator_id = c.id
        WHERE pc.profile_id = ? AND pc.is_enabled = 1
    """, (profile_id,)).fetchall()

    rprint(f"\n[green]✅ Seeded {len(creators)} test creators:[/green]")
    for c in creators:
        rprint(f"  {c['display_name']} ({c['platform']})")

    db.close()


async def main():
    rprint("\n[bold cyan]══ ClipForge Test Run ══[/bold cyan]\n")

    await seed_test_db()

    rprint("\n[bold]Running pipeline: limit=2, top=2[/bold]\n")
    await run_pipeline(
        profile_slug=PROFILE_SLUG,
        skip_discover=False,
        limit_per_creator=2,
        top_n=2,
    )


if __name__ == "__main__":
    asyncio.run(main())