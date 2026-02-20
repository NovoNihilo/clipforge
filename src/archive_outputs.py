"""
Auto-archive: move existing output packs to archives/ before a new pipeline run.

Usage (standalone):
    python -m src.archive_outputs

Or import and call from run.py:
    from src.archive_outputs import archive_existing_outputs
    archive_existing_outputs("funny-streamers")
"""
import shutil
from datetime import datetime
from pathlib import Path
from src.utils.log import log


def archive_existing_outputs(profile_slug: str) -> int:
    """
    Move existing output packs from outputs/{profile_slug}/ into
    archives/{profile_slug}/{YYYY-MM-DD}/

    Returns count of packs archived.
    """
    outputs_dir = Path("outputs") / profile_slug
    if not outputs_dir.exists():
        return 0

    # Check if there are any subdirectories (packs) to archive
    packs = [d for d in outputs_dir.iterdir() if d.is_dir()]
    if not packs:
        return 0

    # Create archive directory with today's date
    # If we're archiving packs from a previous run, label with today's date
    today = datetime.now().strftime("%Y-%m-%d")
    archive_dir = Path("archives") / profile_slug / today

    # If archive for today already exists, append a counter
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


if __name__ == "__main__":
    count = archive_existing_outputs("funny-streamers")
    if count:
        print(f"Archived {count} packs")
    else:
        print("Nothing to archive")