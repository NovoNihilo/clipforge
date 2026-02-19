"""
Milestone 5: Package publish packs.

For each RENDERED clip, creates a publish-ready folder in outputs/ containing:
  - rendered.mp4 (copied)
  - post_copy.json (platform titles, captions, hashtags)
  - thumbnail.jpg (extracted frame)
  - metadata.json (clip info)
  - README.md (human-readable summary)
"""
import asyncio
import json
import shutil
from pathlib import Path
from src.db.database import get_db
from src.models.schemas import ClipStatus, EditDecision, ClipMeta
from src.config import settings
from src.utils.log import log


async def extract_thumbnail(video_path: str, output_path: str, timestamp: float = 1.0) -> bool:
    cmd = [
        "ffmpeg", "-y", "-ss", str(timestamp),
        "-i", video_path, "-vframes", "1", "-q:v", "2", output_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()
    return proc.returncode == 0 and Path(output_path).exists()


async def package_clip(clip_row_id: int) -> bool:
    db = get_db()
    row = db.execute("""
        SELECT cl.*, p.slug as profile_slug,
               c.display_name as creator_name,
               c.channel_url as creator_url
        FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        JOIN creators c ON c.id = cl.creator_id
        WHERE cl.id = ? AND cl.status = ?
    """, (clip_row_id, ClipStatus.RENDERED.value)).fetchone()

    if not row:
        log.warning(f"Clip {clip_row_id} not found or not RENDERED")
        db.close()
        return False

    paths = json.loads(row["paths_json"])
    rendered_path = paths.get("rendered")
    decision_path = paths.get("edit_decision")

    if not rendered_path or not Path(rendered_path).exists():
        log.error(f"Rendered file missing for clip {clip_row_id}")
        db.close()
        return False

    clip_meta = ClipMeta.model_validate_json(row["metadata_json"])

    # Load edit decision
    ed = None
    if decision_path and Path(decision_path).exists():
        with open(decision_path) as f:
            ed = EditDecision.model_validate_json(f.read())

    # Create output folder
    safe_id = row["clip_id"][:50].replace("/", "_")
    pack_dir = settings.outputs_path / row["profile_slug"] / f"{row['platform']}_{safe_id}"
    pack_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Packaging: clip {clip_row_id} → {pack_dir.name}/")

    # 1. Copy rendered video
    shutil.copy2(rendered_path, pack_dir / "rendered.mp4")

    # 2. Extract thumbnail
    thumb_ok = await extract_thumbnail(
        str(pack_dir / "rendered.mp4"),
        str(pack_dir / "thumbnail.jpg"),
        timestamp=1.0,
    )
    if not thumb_ok:
        log.warning("  Thumbnail extraction failed")

    # 3. Post copy JSON
    post_copy = {}
    if ed and ed.post_copy:
        for platform_key, pc in ed.post_copy.items():
            hashtag_str = " ".join(pc.hashtags) if pc.hashtags else ""
            post_copy[platform_key] = {
                "title": pc.title,
                "caption": pc.caption,
                "hashtags": pc.hashtags,
                "ready_to_paste": f"{pc.title}\n\n{pc.caption}\n\n{hashtag_str}".strip(),
            }
    with open(pack_dir / "post_copy.json", "w") as f:
        json.dump(post_copy, f, indent=2)

    # 4. Metadata JSON
    segment_info = {}
    if ed:
        segment_info = {"start": ed.segment.start, "end": ed.segment.end}

    metadata = {
        "clip_id": row["clip_id"],
        "platform": row["platform"],
        "creator": row["creator_name"],
        "creator_url": row["creator_url"],
        "title": clip_meta.title,
        "original_duration_sec": clip_meta.duration_sec,
        "view_count": clip_meta.view_count,
        "created_at": clip_meta.created_at,
        "profile": row["profile_slug"],
        "segment": segment_info,
        "files": {
            "video": "rendered.mp4",
            "thumbnail": "thumbnail.jpg" if thumb_ok else None,
        },
    }
    with open(pack_dir / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    # 5. README
    lines = [
        f"# {clip_meta.title}",
        f"**Creator:** [{row['creator_name']}]({row['creator_url']}) ({row['platform']})",
        f"**Views:** {clip_meta.view_count:,}",
        f"**Segment:** {segment_info.get('start', 0):.1f}s → {segment_info.get('end', 0):.1f}s",
        "",
    ]
    for pk, pc in post_copy.items():
        lines.append(f"## {pk.upper()}")
        lines.append(f"```")
        lines.append(pc["ready_to_paste"])
        lines.append(f"```")
        lines.append("")

    with open(pack_dir / "README.md", "w") as f:
        f.write("\n".join(lines))

    # Update DB
    paths["publish_pack"] = str(pack_dir)
    db.execute("""
        UPDATE clips SET status = ?, paths_json = ?, updated_at = datetime('now')
        WHERE id = ?
    """, (ClipStatus.PACKAGED.value, json.dumps(paths), clip_row_id))
    db.commit()
    db.close()

    file_count = len(list(pack_dir.iterdir()))
    log.info(f"  ✅ Packaged: {file_count} files → outputs/{row['profile_slug']}/{pack_dir.name}/")
    return True


async def package_rendered_clips(profile_slug: str, limit: int = 10) -> dict:
    db = get_db()
    rows = db.execute("""
        SELECT cl.id FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        WHERE p.slug = ? AND cl.status = ?
        ORDER BY cl.created_at ASC
        LIMIT ?
    """, (profile_slug, ClipStatus.RENDERED.value, limit)).fetchall()
    db.close()

    stats = {"total": len(rows), "packaged": 0, "failed": 0}
    for row in rows:
        ok = await package_clip(row["id"])
        if ok:
            stats["packaged"] += 1
        else:
            stats["failed"] += 1
    return stats