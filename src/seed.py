"""
Milestone 0+1: Setup DB, create profile, add creators, discover & download clips.

Usage:
    python -m src.seed
"""
import asyncio
import json
import sys
from rich import print as rprint
from rich.table import Table
from rich.console import Console

from src.db.database import init_db, get_db
from src.models.schemas import ProfileRules
from src.discovery.discover import discover_for_profile
from src.discovery.twitch_api import get_broadcaster_id
from src.download.downloader import download_discovered_clips
from src.config import settings

console = Console()

# ── Profile + Creator definitions ──
PROFILE = {
    "slug": "funny-streamers",
    "name": "Funny Livestreamers",
    "rules": ProfileRules(
        niche="funny livestreamers",
        categories=["Just Chatting", "IRL", "Grand Theft Auto V", "Fortnite"],
        languages=["en"],
        length_band_sec=[12, 40],
        hook_max_delay_sec=2.0,
        silence_ratio_max=0.20,
        caption_style="bold_white",
        hashtag_bank=[
            "#shorts", "#funny", "#streamer", "#viral",
            "#twitch", "#kick", "#livestream", "#clips",
            "#gaming", "#lol",
        ],
    ),
}

# Creators: (platform, login/slug, display_name)
CREATORS = [
    # Twitch
    ("twitch", "braeden", "Braeden"),
    ("twitch", "agent00", "Agent00"),
    ("twitch", "2xrakai", "2xRaKai"),
    ("twitch", "xqc", "xQC"),
    ("twitch", "jasontheween", "JasonTheWeen"),
    ("twitch", "botezlive", "BotezLive"),
    ("twitch", "lacy", "Lacy"),
    ("twitch", "jinnytty", "Jinnytty"),
    ("twitch", "stableronaldo", "StableRonaldo"),
    ("twitch", "adapt", "Adapt"),

    # Kick
    ("kick", "xqc", "xQc"),
    ("kick", "adinross", "Adin Ross"),
    ("kick", "jackdoherty", "Jack Doherty"),
    ("kick", "n3on", "N3on"),
    ("kick", "clavicular", "Clavicular"),
    ("kick", "rampagejackson", "RampageJackson"),
    ("kick", "vitaly", "Vitaly"),
]


async def seed_profile_and_creators():
    """Create profile + creators in DB."""
    rprint("\n[bold cyan]═══ ClipForge: Milestone 0 — Setup ═══[/bold cyan]\n")

    # Init DB
    db = init_db()
    rprint("[green]✅ Database initialized[/green]")

    # Create profile
    rules_json = PROFILE["rules"].model_dump_json()
    db.execute("""
        INSERT OR IGNORE INTO profiles (slug, name, rules_json)
        VALUES (?, ?, ?)
    """, (PROFILE["slug"], PROFILE["name"], rules_json))
    db.commit()

    profile_row = db.execute("SELECT id FROM profiles WHERE slug = ?",
                             (PROFILE["slug"],)).fetchone()
    profile_id = profile_row["id"]
    rprint(f"[green]✅ Profile created: {PROFILE['slug']} (id={profile_id})[/green]")

    # Add creators
    for platform, login, display_name in CREATORS:
        platform_user_id = login  # default: use login as ID

        # For Twitch, resolve login → broadcaster_id
        if platform == "twitch":
            bid = await get_broadcaster_id(login)
            if bid:
                platform_user_id = bid
                rprint(f"  Twitch {login} → broadcaster_id={bid}")
            else:
                rprint(f"  [yellow]⚠ Could not resolve Twitch user: {login} — skipping[/yellow]")
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

    # Show summary
    creators = db.execute("""
        SELECT c.*, pc.is_enabled FROM creators c
        JOIN profile_creators pc ON pc.creator_id = c.id
        WHERE pc.profile_id = ?
    """, (profile_id,)).fetchall()

    table = Table(title="Tracked Creators")
    table.add_column("ID", style="cyan")
    table.add_column("Platform", style="green")
    table.add_column("User ID", style="yellow")
    table.add_column("Name")
    table.add_column("URL")
    for c in creators:
        table.add_row(str(c["id"]), c["platform"], c["platform_user_id"],
                      c["display_name"], c["channel_url"])
    console.print(table)
    db.close()
    rprint(f"\n[green]✅ {len(creators)} creators linked to profile '{PROFILE['slug']}'[/green]")


async def run_discovery_and_download():
    """Discover + download 1 clip."""
    rprint("\n[bold cyan]═══ ClipForge: Milestone 1 — Discover & Download ═══[/bold cyan]\n")

    # Discover
    new_clips = await discover_for_profile(PROFILE["slug"], max_per_creator=3)
    if not new_clips:
        rprint("[yellow]No new clips discovered. Try again later or check API keys.[/yellow]")
        return

    rprint(f"\n[green]Discovered {len(new_clips)} new clips:[/green]")
    table = Table(title="Discovered Clips")
    table.add_column("Platform")
    table.add_column("Clip ID")
    table.add_column("Creator")
    table.add_column("Title")
    table.add_column("Views", justify="right")
    table.add_column("Duration", justify="right")
    for c in new_clips[:10]:
        table.add_row(
            c["platform"], c["clip_id"][:20] + "...",
            c["creator"], c["title"][:40],
            str(c["views"]), f"{c['duration']:.0f}s"
        )
    console.print(table)

    # Download first clip
    rprint("\n[cyan]Downloading first discovered clip...[/cyan]")
    count = await download_discovered_clips(PROFILE["slug"], limit=1)
    rprint(f"[green]✅ Downloaded {count} clip(s)[/green]")

    # Show DB state
    db = get_db()
    downloaded = db.execute("""
        SELECT cl.*, c.display_name as creator_name FROM clips cl
        JOIN creators c ON c.id = cl.creator_id
        WHERE cl.status = 'DOWNLOADED'
        ORDER BY cl.updated_at DESC LIMIT 1
    """).fetchone()

    if downloaded:
        rprint(f"\n[bold green]── Downloaded Clip ──[/bold green]")
        rprint(f"  DB ID:    {downloaded['id']}")
        rprint(f"  Platform: {downloaded['platform']}")
        rprint(f"  Clip ID:  {downloaded['clip_id']}")
        rprint(f"  Creator:  {downloaded['creator_name']}")
        rprint(f"  Status:   {downloaded['status']}")
        paths = json.loads(downloaded["paths_json"])
        rprint(f"  File:     {paths.get('source', 'N/A')}")

        # Verify file exists
        src = paths.get("source")
        if src:
            import os
            size = os.path.getsize(src)
            rprint(f"  Size:     {size / 1024 / 1024:.1f} MB")
    else:
        rprint("[yellow]No downloaded clips found in DB.[/yellow]")

    db.close()


async def main():
    await seed_profile_and_creators()
    await run_discovery_and_download()
    rprint("\n[bold green]🎉 Milestone 0+1 complete![/bold green]")


if __name__ == "__main__":
    asyncio.run(main())
