"""
ClipForge — Full Pipeline Runner.

Runs the entire pipeline end-to-end:
  0. Archive previous outputs
  1. Discover new clips (last 24h, up to 3 per creator)
  2. Download discovered clips
  3. Transcribe + quality gates
  4. LLM edit decisions
  5. Render 9:16 shorts with captions
  6. Package publish packs

Usage:
    python -m src.run
    python -m src.run --profile funny-streamers
    python -m src.run --profile funny-streamers --skip-discover
"""
import asyncio
import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from rich import print as rprint
from rich.console import Console

from src.db.database import init_db, get_db
from src.discovery.discover import discover_for_profile
from src.download.downloader import download_discovered_clips
from src.transcribe.transcriber import transcribe_downloaded_clips
from src.decide.decider import decide_transcribed_clips
from src.render.renderer import render_decided_clips
from src.package.packager import package_rendered_clips
from src.utils.log import log

console = Console()


# ── Auto-archive ──────────────────────────────────────────────────────────────

def archive_existing_outputs(profile_slug: str) -> int:
    """
    Move existing output packs from outputs/{profile_slug}/ into
    archives/{profile_slug}/{YYYY-MM-DD}/

    Returns count of packs archived.
    """
    outputs_dir = Path("outputs") / profile_slug
    if not outputs_dir.exists():
        return 0

    # Only archive subdirectories (each pack is a folder)
    packs = [d for d in outputs_dir.iterdir() if d.is_dir()]
    if not packs:
        return 0

    today = datetime.now().strftime("%Y-%m-%d")
    archive_dir = Path("archives") / profile_slug / today

    # If archive for today already exists (ran twice in one day), append counter
    if archive_dir.exists():
        counter = 2
        while archive_dir.exists():
            archive_dir = Path("archives") / profile_slug / f"{today}-{counter}"
            counter += 1

    archive_dir.mkdir(parents=True, exist_ok=True)

    archived = 0
    for pack_dir in packs:
        dest = archive_dir / pack_dir.name
        shutil.move(str(pack_dir), str(dest))
        archived += 1

    log.info(f"📦 Archived {archived} output packs → {archive_dir}")
    return archived


# ── Pipeline status ───────────────────────────────────────────────────────────

def show_pipeline_status(profile_slug: str):
    """Show current clip counts by status."""
    db = get_db()
    rows = db.execute("""
        SELECT cl.status, COUNT(*) as cnt
        FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        WHERE p.slug = ?
        GROUP BY cl.status ORDER BY cl.status
    """, (profile_slug,)).fetchall()
    db.close()

    emoji_map = {
        "DISCOVERED": "🔍", "DOWNLOADED": "⬇️", "TRANSCRIBED": "📝",
        "DECIDED": "🧠", "RENDERED": "🎬", "PACKAGED": "📦", "FAILED": "❌",
    }
    rprint("\n[bold]Pipeline Status:[/bold]")
    for r in rows:
        e = emoji_map.get(r["status"], "?")
        rprint(f"  {e} {r['status']}: {r['cnt']}")
    rprint("")


# ── Main pipeline ─────────────────────────────────────────────────────────────

async def run_pipeline(profile_slug: str, skip_discover: bool = False, limit_per_creator: int = 3):
    """Run the full pipeline."""
    rprint(f"\n[bold cyan]══ ClipForge Pipeline: {profile_slug} ══[/bold cyan]\n")

    # Ensure DB exists
    init_db()

    # ── Step 0: Archive previous outputs ──
    archived = archive_existing_outputs(profile_slug)
    if archived:
        rprint(f"[bold]Step 0: Archived {archived} previous output packs[/bold]")
        rprint(f"  → Moved to archives/{profile_slug}/\n")
    else:
        rprint("[dim]Step 0: No previous outputs to archive[/dim]\n")

    # ── Step 1: Discover ──
    if not skip_discover:
        rprint("[bold]Step 1/6: Discovering new clips...[/bold]")
        new_clips = await discover_for_profile(profile_slug, max_per_creator=limit_per_creator)
        rprint(f"  → {len(new_clips)} new clips discovered\n")
    else:
        rprint("[dim]Step 1/6: Skipped discovery[/dim]\n")

    # ── Step 2: Download ──
    rprint("[bold]Step 2/6: Downloading clips...[/bold]")
    dl_count = await download_discovered_clips(profile_slug, limit=50)
    rprint(f"  → {dl_count} clips downloaded\n")

    if dl_count == 0:
        db = get_db()
        remaining = db.execute("""
            SELECT COUNT(*) as cnt FROM clips cl
            JOIN profiles p ON p.id = cl.profile_id
            WHERE p.slug = ? AND cl.status = 'DISCOVERED'
        """, (profile_slug,)).fetchone()["cnt"]
        db.close()
        if remaining == 0:
            rprint("[yellow]  No clips to process. All clips already handled or none discovered.[/yellow]\n")

    # ── Step 3: Transcribe ──
    rprint("[bold]Step 3/6: Transcribing + quality gates...[/bold]")
    t_stats = await transcribe_downloaded_clips(profile_slug, limit=50)
    rprint(f"  → {t_stats['passed']} passed, {t_stats['failed']} filtered out\n")

    # ── Step 4: LLM Decisions ──
    rprint("[bold]Step 4/6: LLM edit decisions...[/bold]")
    d_stats = await decide_transcribed_clips(profile_slug, limit=50)
    rprint(f"  → {d_stats['decided']} decisions made\n")

    # ── Step 5: Render ──
    rprint("[bold]Step 5/6: Rendering shorts...[/bold]")
    r_stats = await render_decided_clips(profile_slug, limit=50)
    rprint(f"  → {r_stats['rendered']} shorts rendered\n")

    # ── Step 6: Package ──
    rprint("[bold]Step 6/6: Packaging publish packs...[/bold]")
    p_stats = await package_rendered_clips(profile_slug, limit=50)
    rprint(f"  → {p_stats['packaged']} packs ready\n")

    # ── Summary ──
    show_pipeline_status(profile_slug)

    total_new = p_stats["packaged"]
    if total_new > 0:
        rprint(f"[bold green]✅ {total_new} new shorts ready in outputs/{profile_slug}/[/bold green]")
    else:
        rprint("[yellow]No new shorts produced this run.[/yellow]")

    rprint(f"[bold cyan]══ Pipeline complete ══[/bold cyan]\n")


def main():
    parser = argparse.ArgumentParser(description="ClipForge — Full Pipeline")
    parser.add_argument("--profile", default="funny-streamers", help="Profile slug to process")
    parser.add_argument("--skip-discover", action="store_true", help="Skip discovery step")
    parser.add_argument("--limit", type=int, default=3, help="Max clips per creator to discover")
    args = parser.parse_args()

    asyncio.run(run_pipeline(
        profile_slug=args.profile,
        skip_discover=args.skip_discover,
        limit_per_creator=args.limit,
    ))


if __name__ == "__main__":
    main()