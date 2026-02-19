"""
Milestone 3: LLM Edit Decision Maker (OpenAI GPT-4.1).

Sends transcript + clip metadata to OpenAI API.
Returns a structured EditDecision with:
  - Best segment to extract (start/end)
  - Viral score (1-10)
  - Platform-specific post copy (title, caption, hashtags)
  - Layout + caption config
"""
import asyncio
import json
import httpx
from pathlib import Path
from src.db.database import get_db
from src.models.schemas import (
    ClipMeta, ClipStatus, ProfileRules, EditDecision,
    Segment, Layout, CaptionConfig, AudioConfig, OutputSpec, PlatformCopy,
)
from src.config import settings
from src.utils.log import log


OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = "gpt-4.1"

SYSTEM_PROMPT = """You are an expert short-form video editor for social media.
You analyze clip transcripts and metadata to make edit decisions for viral shorts.

Your job:
1. Pick the BEST segment (start_sec, end_sec) that would make the most engaging short
2. Score its viral potential (1-10)
3. Write platform-specific post copy

Rules:
- The segment MUST start with a strong hook (funny moment, shocking statement, or energy shift)
- Prefer segments where speech starts within the first 1-2 seconds
- Target length: {min_len}-{max_len} seconds (the segment you pick must be within this range)
- If the entire clip is good, use the full duration
- If the clip is longer than {max_len}s, find the best {max_len}s window
- Post copy should be short, punchy, and use the creator's voice/energy
- Hashtags should mix niche tags with broad viral tags

You MUST respond with ONLY a JSON object (no markdown, no backticks, no explanation).
The JSON must have exactly this structure:

{{
  "segment_start": <float>,
  "segment_end": <float>,
  "viral_score": <int 1-10>,
  "viral_reason": "<1 sentence why this would go viral>",
  "hook_description": "<what happens in the first 2 seconds>",
  "post_copy": {{
    "shorts": {{
      "title": "<YouTube Shorts title, max 100 chars>",
      "caption": "<YouTube description, 1-2 sentences>",
      "hashtags": ["#tag1", "#tag2", "#tag3", "#tag4", "#tag5"]
    }},
    "tiktok": {{
      "title": "<TikTok caption, max 150 chars with hashtags inline>",
      "caption": "",
      "hashtags": ["#tag1", "#tag2", "#tag3"]
    }},
    "reels": {{
      "title": "<Instagram Reels caption, max 125 chars>",
      "caption": "",
      "hashtags": ["#tag1", "#tag2", "#tag3", "#tag4"]
    }}
  }}
}}"""


def _build_user_prompt(clip_meta: ClipMeta, transcript: dict, rules: ProfileRules) -> str:
    segments_text = ""
    for seg in transcript.get("segments", []):
        segments_text += f"  [{seg['start']:.1f}s - {seg['end']:.1f}s] {seg['text']}\n"

    return f"""Analyze this clip and make an edit decision.

CLIP INFO:
- Title: {clip_meta.title}
- Creator: {clip_meta.creator_name}
- Platform: {clip_meta.platform}
- Duration: {transcript.get('duration', 0):.1f}s
- Views: {clip_meta.view_count:,}
- Category: {clip_meta.game_name or 'Just Chatting'}

PROFILE NICHE: {rules.niche}
TARGET LENGTH: {rules.length_band_sec[0]}-{rules.length_band_sec[1]} seconds

TRANSCRIPT (with timestamps):
{segments_text}

FULL TEXT: {transcript.get('full_text', '')}

Pick the best segment and generate post copy. Respond with ONLY JSON."""


async def call_openai_api(system: str, user_msg: str) -> dict | None:
    api_key = settings.openai_api_key
    if not api_key:
        log.error("OPENAI_API_KEY not set in .env")
        return None

    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            resp = await client.post(
                OPENAI_API_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user_msg},
                    ],
                    "temperature": 0.7,
                    "max_tokens": 1024,
                },
            )
            resp.raise_for_status()
            data = resp.json()

            text = data["choices"][0]["message"]["content"].strip()

            # Strip markdown fences if present
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                if text.endswith("```"):
                    text = text[:-3]
                text = text.strip()

            return json.loads(text)

        except httpx.HTTPStatusError as e:
            log.error(f"OpenAI API error {e.response.status_code}: {e.response.text[:500]}")
            return None
        except json.JSONDecodeError as e:
            log.error(f"Failed to parse OpenAI response as JSON: {e}\nRaw: {text[:500]}")
            return None
        except Exception as e:
            log.error(f"OpenAI API call failed: {e}")
            return None


def _llm_response_to_edit_decision(
    llm_resp: dict,
    clip_meta: ClipMeta,
    rules: ProfileRules,
    profile_slug: str,
) -> EditDecision:
    post_copy = {}
    for platform_key in ["shorts", "tiktok", "reels"]:
        pc_data = llm_resp.get("post_copy", {}).get(platform_key, {})
        post_copy[platform_key] = PlatformCopy(
            title=pc_data.get("title", clip_meta.title),
            caption=pc_data.get("caption", ""),
            hashtags=pc_data.get("hashtags", rules.hashtag_bank[:5]),
        )

    return EditDecision(
        profile_slug=profile_slug,
        clip_id=clip_meta.clip_id,
        segment=Segment(
            start=float(llm_resp.get("segment_start", 0)),
            end=float(llm_resp.get("segment_end", clip_meta.duration_sec)),
        ),
        layout=Layout(mode="center_crop", target="9:16"),
        captions=CaptionConfig(
            enabled=True,
            style=rules.caption_style,
            position=rules.caption_position,
            max_words=rules.caption_max_words,
        ),
        audio=AudioConfig(normalize=True),
        outputs={
            "shorts": OutputSpec(max_len_sec=60),
            "tiktok": OutputSpec(max_len_sec=60),
            "reels": OutputSpec(max_len_sec=90),
        },
        post_copy=post_copy,
    )


async def decide_clip(clip_row_id: int) -> bool:
    db = get_db()
    row = db.execute("""
        SELECT cl.*, p.rules_json, p.slug as profile_slug
        FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        WHERE cl.id = ? AND cl.status = ?
    """, (clip_row_id, ClipStatus.TRANSCRIBED.value)).fetchone()

    if not row:
        log.warning(f"Clip {clip_row_id} not found or not TRANSCRIBED")
        db.close()
        return False

    clip_meta = ClipMeta.model_validate_json(row["metadata_json"])
    rules = ProfileRules.model_validate_json(row["rules_json"])
    paths = json.loads(row["paths_json"])
    profile_slug = row["profile_slug"]

    transcript_path = paths.get("transcript")
    if not transcript_path or not Path(transcript_path).exists():
        log.error(f"Transcript missing for clip {clip_row_id}")
        db.execute("""
            UPDATE clips SET status = ?, fail_reason = 'transcript_missing', updated_at = datetime('now')
            WHERE id = ?
        """, (ClipStatus.FAILED.value, clip_row_id))
        db.commit()
        db.close()
        return False

    with open(transcript_path) as f:
        transcript = json.load(f)

    log.info(f"Deciding: {clip_meta.title} ({row['platform']}/{row['clip_id'][:30]}...)")

    system = SYSTEM_PROMPT.format(
        min_len=rules.length_band_sec[0],
        max_len=rules.length_band_sec[1],
    )
    user_msg = _build_user_prompt(clip_meta, transcript, rules)

    llm_resp = await call_openai_api(system, user_msg)

    if not llm_resp:
        db.execute("""
            UPDATE clips SET status = ?, fail_reason = 'llm_call_failed', updated_at = datetime('now')
            WHERE id = ?
        """, (ClipStatus.FAILED.value, clip_row_id))
        db.commit()
        db.close()
        return False

    try:
        edit_decision = _llm_response_to_edit_decision(llm_resp, clip_meta, rules, profile_slug)
    except Exception as e:
        log.error(f"Failed to build EditDecision: {e}")
        db.execute("""
            UPDATE clips SET status = ?, fail_reason = ?, updated_at = datetime('now')
            WHERE id = ?
        """, (ClipStatus.FAILED.value, f"edit_decision_invalid:{e}", clip_row_id))
        db.commit()
        db.close()
        return False

    decision_path = Path(transcript_path).parent / "edit_decision.json"
    with open(decision_path, "w") as f:
        f.write(edit_decision.model_dump_json(indent=2))

    paths["edit_decision"] = str(decision_path)

    db.execute("""
        UPDATE clips SET
            status = ?,
            paths_json = ?,
            updated_at = datetime('now')
        WHERE id = ?
    """, (ClipStatus.DECIDED.value, json.dumps(paths), clip_row_id))
    db.commit()
    db.close()

    viral_score = llm_resp.get("viral_score", "?")
    viral_reason = llm_resp.get("viral_reason", "")
    seg = edit_decision.segment
    log.info(f"  ✅ Decided: score={viral_score}/10, segment={seg.start:.1f}-{seg.end:.1f}s")
    log.info(f"  Reason: {viral_reason}")
    log.info(f"  YT Title: {edit_decision.post_copy.get('shorts', PlatformCopy(title='',caption='',hashtags=[])).title}")

    return True


async def decide_transcribed_clips(profile_slug: str, limit: int = 10) -> dict:
    db = get_db()
    rows = db.execute("""
        SELECT cl.id FROM clips cl
        JOIN profiles p ON p.id = cl.profile_id
        WHERE p.slug = ? AND cl.status = ?
        ORDER BY cl.created_at ASC
        LIMIT ?
    """, (profile_slug, ClipStatus.TRANSCRIBED.value, limit)).fetchall()
    db.close()

    stats = {"total": len(rows), "decided": 0, "failed": 0}

    for row in rows:
        ok = await decide_clip(row["id"])
        if ok:
            stats["decided"] += 1
        else:
            stats["failed"] += 1
        await asyncio.sleep(1.0)

    return stats