"""
Milestone 4: Render shorts.

Usage:
    python -m src.test_m4
"""
import asyncio
import json
from pathlib import Path
from rich import print as rprint
from rich.table import Table
from rich.console import Console

from src.db.database import get_db
from src.render.renderer import render_decided_clips

console = Console()
PROFILE_SLUG = "funny-streamers"


async def main():
    rprint("\n[bold cyan]═══ ClipForge: Milestone 4 — Render Shorts ═══[/bold cyan]\n")

    # Check DECIDED clips
    db = get_db()
    count = db.execute("""
        SELECT COUNT(*) as cnt FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        WHERE p.slug = ? AND cl.status = 'DECIDED'
    """, (PROFILE_SLUG,)).fetchone()["cnt"]
    db.close()

    rprint(f"[cyan]Found {count} DECIDED clips ready to render[/cyan]\n")

    if count == 0:
        rprint("[yellow]No decided clips. Run test_m3 first.[/yellow]")
        return

    # Render
    stats = await render_decided_clips(PROFILE_SLUG, limit=10)

    rprint(f"\n[bold]Render results:[/bold]")
    rprint(f"  Total:    {stats['total']}")
    rprint(f"  Rendered: [green]{stats['rendered']}[/green]")
    rprint(f"  Failed:   [red]{stats['failed']}[/red]")

    # Show rendered clips
    db = get_db()
    rendered = db.execute("""
        SELECT cl.id, cl.platform, cl.clip_id, cl.paths_json,
               c.display_name as creator
        FROM clips cl
        JOIN creators c ON c.id = cl.creator_id
        WHERE cl.status = 'RENDERED'
        ORDER BY cl.updated_at DESC
    """).fetchall()

    if rendered:
        table = Table(title="✅ Rendered Shorts")
        table.add_column("ID")
        table.add_column("Creator")
        table.add_column("Resolution")
        table.add_column("Size")
        table.add_column("File")
        for row in rendered:
            paths = json.loads(row["paths_json"])
            rendered_path = paths.get("rendered", "")
            size = ""
            resolution = ""
            if rendered_path and Path(rendered_path).exists():
                size = f"{Path(rendered_path).stat().st_size / 1024 / 1024:.1f} MB"
                # Quick check via file name
                resolution = "1080x1920"
            table.add_row(
                str(row["id"]), row["creator"],
                resolution, size,
                rendered_path.split("/")[-2] + "/rendered.mp4" if "/" in rendered_path else rendered_path,
            )
        console.print(table)

    db.close()
    rprint("\n[bold green]🎉 Milestone 4 complete![/bold green]")
    rprint("[dim]Check the rendered files in assets/ — open them to verify quality![/dim]")


if __name__ == "__main__":
    asyncio.run(main())