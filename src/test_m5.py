"""
Milestone 5: Package publish packs.

Usage:
    python -m src.test_m5
"""
import asyncio
import json
from pathlib import Path
from rich import print as rprint
from rich.table import Table
from rich.console import Console

from src.db.database import get_db
from src.package.packager import package_rendered_clips

console = Console()
PROFILE_SLUG = "funny-streamers"


async def main():
    rprint("\n[bold cyan]═══ ClipForge: Milestone 5 — Package Publish Packs ═══[/bold cyan]\n")

    db = get_db()
    count = db.execute("""
        SELECT COUNT(*) as cnt FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        WHERE p.slug = ? AND cl.status = 'RENDERED'
    """, (PROFILE_SLUG,)).fetchone()["cnt"]
    db.close()

    rprint(f"[cyan]Found {count} RENDERED clips ready to package[/cyan]\n")

    if count == 0:
        rprint("[yellow]No rendered clips. Run test_m4 first.[/yellow]")
        return

    stats = await package_rendered_clips(PROFILE_SLUG, limit=10)

    rprint(f"\n[bold]Package results:[/bold]")
    rprint(f"  Total:    {stats['total']}")
    rprint(f"  Packaged: [green]{stats['packaged']}[/green]")
    rprint(f"  Failed:   [red]{stats['failed']}[/red]")

    # Show packaged clips
    db = get_db()
    packaged = db.execute("""
        SELECT cl.id, cl.platform, cl.clip_id, cl.paths_json,
               c.display_name as creator
        FROM clips cl
        JOIN creators c ON c.id = cl.creator_id
        WHERE cl.status = 'PACKAGED'
        ORDER BY cl.updated_at DESC
    """).fetchall()

    if packaged:
        table = Table(title="📦 Publish Packs")
        table.add_column("ID")
        table.add_column("Creator")
        table.add_column("Platform")
        table.add_column("Pack Folder")
        table.add_column("Files")

        for row in packaged:
            paths = json.loads(row["paths_json"])
            pack_dir = paths.get("publish_pack", "")
            file_count = ""
            if pack_dir and Path(pack_dir).exists():
                file_count = str(len(list(Path(pack_dir).iterdir())))

            folder_name = Path(pack_dir).name if pack_dir else "N/A"
            table.add_row(
                str(row["id"]), row["creator"], row["platform"],
                folder_name, file_count,
            )
        console.print(table)

    # Show pipeline summary
    all_statuses = db.execute("""
        SELECT status, COUNT(*) as cnt FROM clips GROUP BY status ORDER BY status
    """).fetchall()

    rprint("\n[bold]Full Pipeline Status:[/bold]")
    for s in all_statuses:
        emoji = {"DISCOVERED": "🔍", "DOWNLOADED": "⬇️", "TRANSCRIBED": "📝",
                 "DECIDED": "🧠", "RENDERED": "🎬", "PACKAGED": "📦", "FAILED": "❌"}.get(s["status"], "?")
        rprint(f"  {emoji} {s['status']}: {s['cnt']}")

    db.close()

    rprint(f"\n[bold green]🎉 Milestone 5 complete! MVP pipeline finished![/bold green]")
    rprint(f"[dim]Check outputs/{PROFILE_SLUG}/ for your publish-ready packs![/dim]")


if __name__ == "__main__":
    asyncio.run(main())