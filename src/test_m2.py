"""
Milestone 2: Download more clips + transcribe + quality gates.

Usage:
    python -m src.test_m2
"""
import asyncio
import json
from rich import print as rprint
from rich.table import Table
from rich.console import Console

from src.db.database import get_db
from src.download.downloader import download_discovered_clips
from src.transcribe.transcriber import transcribe_downloaded_clips

console = Console()
PROFILE_SLUG = "funny-streamers"


async def main():
    rprint("\n[bold cyan]═══ ClipForge: Milestone 2 — Transcribe + Quality Gates ═══[/bold cyan]\n")

    # Step 1: Download a few more clips (we already have 27 discovered)
    rprint("[cyan]Downloading up to 3 more clips...[/cyan]")
    dl_count = await download_discovered_clips(PROFILE_SLUG, limit=3)
    rprint(f"[green]Downloaded {dl_count} clip(s)[/green]\n")

    # Step 2: Transcribe all DOWNLOADED clips
    rprint("[cyan]Transcribing downloaded clips...[/cyan]\n")
    stats = await transcribe_downloaded_clips(PROFILE_SLUG, limit=10)

    rprint(f"\n[bold]Transcription results:[/bold]")
    rprint(f"  Total:  {stats['total']}")
    rprint(f"  Passed: [green]{stats['passed']}[/green]")
    rprint(f"  Failed: [red]{stats['failed']}[/red]")

    # Step 3: Show DB state
    db = get_db()

    # Show transcribed clips
    transcribed = db.execute("""
        SELECT cl.id, cl.platform, cl.clip_id, cl.status, cl.paths_json,
               c.display_name as creator
        FROM clips cl
        JOIN creators c ON c.id = cl.creator_id
        WHERE cl.status = 'TRANSCRIBED'
        ORDER BY cl.updated_at DESC
    """).fetchall()

    if transcribed:
        table = Table(title="✅ Transcribed Clips")
        table.add_column("ID")
        table.add_column("Platform")
        table.add_column("Creator")
        table.add_column("Clip ID")
        table.add_column("Transcript Preview")
        for row in transcribed:
            paths = json.loads(row["paths_json"])
            preview = ""
            if paths.get("transcript"):
                try:
                    with open(paths["transcript"]) as f:
                        t = json.load(f)
                        preview = t.get("full_text", "")[:60] + "..."
                except:
                    preview = "(error reading)"
            table.add_row(
                str(row["id"]), row["platform"], row["creator"],
                row["clip_id"][:30] + "...", preview
            )
        console.print(table)

    # Show failed clips
    failed = db.execute("""
        SELECT cl.id, cl.platform, cl.clip_id, cl.fail_reason,
               c.display_name as creator
        FROM clips cl
        JOIN creators c ON c.id = cl.creator_id
        WHERE cl.status = 'FAILED'
        ORDER BY cl.updated_at DESC
    """).fetchall()

    if failed:
        table = Table(title="❌ Failed Clips")
        table.add_column("ID")
        table.add_column("Platform")
        table.add_column("Creator")
        table.add_column("Reason")
        for row in failed:
            table.add_row(
                str(row["id"]), row["platform"], row["creator"],
                row["fail_reason"] or "unknown"
            )
        console.print(table)

    db.close()
    rprint("\n[bold green]🎉 Milestone 2 complete![/bold green]")


if __name__ == "__main__":
    asyncio.run(main())