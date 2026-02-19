"""
Milestone 3: LLM Edit Decision.

Usage:
    python -m src.test_m3
"""
import asyncio
import json
from rich import print as rprint
from rich.table import Table
from rich.console import Console

from src.db.database import get_db
from src.decide.decider import decide_transcribed_clips

console = Console()
PROFILE_SLUG = "funny-streamers"


async def main():
    rprint("\n[bold cyan]═══ ClipForge: Milestone 3 — LLM Edit Decisions ═══[/bold cyan]\n")

    # Check how many TRANSCRIBED clips we have
    db = get_db()
    count = db.execute("""
        SELECT COUNT(*) as cnt FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        WHERE p.slug = ? AND cl.status = 'TRANSCRIBED'
    """, (PROFILE_SLUG,)).fetchone()["cnt"]
    db.close()

    rprint(f"[cyan]Found {count} TRANSCRIBED clips ready for LLM decisions[/cyan]\n")

    if count == 0:
        rprint("[yellow]No transcribed clips. Run test_m2 first.[/yellow]")
        return

    # Run LLM decisions
    stats = await decide_transcribed_clips(PROFILE_SLUG, limit=10)

    rprint(f"\n[bold]Decision results:[/bold]")
    rprint(f"  Total:   {stats['total']}")
    rprint(f"  Decided: [green]{stats['decided']}[/green]")
    rprint(f"  Failed:  [red]{stats['failed']}[/red]")

    # Show decided clips
    db = get_db()
    decided = db.execute("""
        SELECT cl.id, cl.platform, cl.clip_id, cl.paths_json,
               c.display_name as creator
        FROM clips cl
        JOIN creators c ON c.id = cl.creator_id
        WHERE cl.status = 'DECIDED'
        ORDER BY cl.updated_at DESC
    """).fetchall()

    if decided:
        for row in decided:
            paths = json.loads(row["paths_json"])
            decision_path = paths.get("edit_decision")
            if not decision_path:
                continue

            try:
                with open(decision_path) as f:
                    ed = json.load(f)
            except:
                continue

            rprint(f"\n[bold green]── Clip {row['id']}: {row['creator']} ({row['platform']}) ──[/bold green]")
            rprint(f"  Clip ID:  {row['clip_id'][:40]}...")
            rprint(f"  Segment:  {ed['segment']['start']:.1f}s → {ed['segment']['end']:.1f}s")

            # Show post copy for each platform
            for platform, copy in ed.get("post_copy", {}).items():
                rprint(f"  [{platform}] {copy.get('title', 'N/A')}")
                if copy.get("hashtags"):
                    rprint(f"           {' '.join(copy['hashtags'][:5])}")

    db.close()
    rprint("\n[bold green]🎉 Milestone 3 complete![/bold green]")


if __name__ == "__main__":
    asyncio.run(main())