"""
ClipForge cleanup utility.

Usage:
    python -m src.cleanup --help
    python -m src.cleanup --status          # show disk usage
    python -m src.cleanup --purge-failed    # delete FAILED clip files
    python -m src.cleanup --archive         # compress PACKAGED clips into zip, delete originals
    python -m src.cleanup --purge-old 7     # delete source files for clips older than N days
"""
import argparse
import json
import os
import shutil
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from src.db.database import get_db
from src.utils.log import log


def get_disk_usage():
    """Show disk usage breakdown."""
    db = get_db()

    statuses = db.execute("""
        SELECT status, COUNT(*) as cnt FROM clips GROUP BY status
    """).fetchall()

    print("\n📊 Clip counts by status:")
    for row in statuses:
        print(f"  {row['status']:12s}: {row['cnt']}")

    # Calculate disk usage
    total_source = 0
    total_rendered = 0
    total_output = 0

    clips = db.execute("SELECT paths_json FROM clips").fetchall()
    for clip in clips:
        paths = json.loads(clip["paths_json"])
        for key, path in paths.items():
            if path and Path(path).exists():
                size = Path(path).stat().st_size
                if key == "source":
                    total_source += size
                elif key == "rendered":
                    total_rendered += size

    # Output packs
    outputs_dir = Path("outputs")
    if outputs_dir.exists():
        for f in outputs_dir.rglob("*"):
            if f.is_file():
                total_output += f.stat().st_size

    assets_dir = Path("assets")
    total_assets = 0
    if assets_dir.exists():
        for f in assets_dir.rglob("*"):
            if f.is_file():
                total_assets += f.stat().st_size

    print(f"\n💾 Disk usage:")
    print(f"  assets/ (sources + renders): {total_assets / 1024 / 1024:.0f} MB")
    print(f"    - source videos:           {total_source / 1024 / 1024:.0f} MB")
    print(f"    - rendered videos:         {total_rendered / 1024 / 1024:.0f} MB")
    print(f"  outputs/ (publish packs):    {total_output / 1024 / 1024:.0f} MB")
    print(f"  TOTAL:                       {(total_assets + total_output) / 1024 / 1024:.0f} MB")

    db.close()


def purge_failed():
    """Delete files for FAILED clips and remove DB entries."""
    db = get_db()
    failed = db.execute("""
        SELECT id, paths_json, clip_id FROM clips WHERE status = 'FAILED'
    """).fetchall()

    if not failed:
        print("No FAILED clips to purge.")
        return

    freed = 0
    for row in failed:
        paths = json.loads(row["paths_json"])
        source = paths.get("source")
        if source:
            clip_dir = Path(source).parent
            if clip_dir.exists():
                for f in clip_dir.rglob("*"):
                    if f.is_file():
                        freed += f.stat().st_size
                shutil.rmtree(clip_dir)

    # Delete DB entries
    db.execute("DELETE FROM clips WHERE status = 'FAILED'")
    db.commit()
    db.close()

    print(f"🗑️  Purged {len(failed)} failed clips, freed {freed / 1024 / 1024:.1f} MB")


def archive_packaged():
    """Compress PACKAGED clip source files into a zip, delete originals."""
    db = get_db()
    packaged = db.execute("""
        SELECT id, paths_json, clip_id, platform FROM clips WHERE status = 'PACKAGED'
    """).fetchall()

    if not packaged:
        print("No PACKAGED clips to archive.")
        return

    archive_dir = Path("archives")
    archive_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path = archive_dir / f"clips_{timestamp}.zip"

    freed = 0
    archived = 0

    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for row in packaged:
            paths = json.loads(row["paths_json"])
            source = paths.get("source")
            if source and Path(source).exists():
                # Add source video to archive
                arcname = f"{row['platform']}/{row['clip_id']}/{Path(source).name}"
                zf.write(source, arcname)
                source_size = Path(source).stat().st_size
                freed += source_size
                archived += 1

                # Delete the source video (keep rendered + other metadata)
                Path(source).unlink()

    archive_size = archive_path.stat().st_size
    print(f"📦 Archived {archived} source videos → {archive_path}")
    print(f"   Archive size: {archive_size / 1024 / 1024:.1f} MB")
    print(f"   Freed: {freed / 1024 / 1024:.1f} MB (sources deleted)")

    db.close()


def purge_old(days: int):
    """Delete source files for clips older than N days."""
    db = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    old_clips = db.execute("""
        SELECT id, paths_json, clip_id FROM clips
        WHERE status = 'PACKAGED' AND updated_at < ?
    """, (cutoff,)).fetchall()

    if not old_clips:
        print(f"No PACKAGED clips older than {days} days.")
        return

    freed = 0
    for row in old_clips:
        paths = json.loads(row["paths_json"])
        source = paths.get("source")
        if source and Path(source).exists():
            freed += Path(source).stat().st_size
            Path(source).unlink()

    print(f"🗑️  Deleted source files for {len(old_clips)} clips older than {days} days")
    print(f"   Freed: {freed / 1024 / 1024:.1f} MB")

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ClipForge disk cleanup")
    parser.add_argument("--status", action="store_true", help="Show disk usage")
    parser.add_argument("--purge-failed", action="store_true", help="Delete FAILED clips")
    parser.add_argument("--archive", action="store_true", help="Zip PACKAGED source files")
    parser.add_argument("--purge-old", type=int, metavar="DAYS", help="Delete sources older than N days")

    args = parser.parse_args()

    if not any([args.status, args.purge_failed, args.archive, args.purge_old]):
        get_disk_usage()
    else:
        if args.status:
            get_disk_usage()
        if args.purge_failed:
            purge_failed()
        if args.archive:
            archive_packaged()
        if args.purge_old:
            purge_old(args.purge_old)