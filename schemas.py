"""Pydantic models for data validation."""
from __future__ import annotations
from pydantic import BaseModel, Field
from enum import Enum
from typing import Optional
import json


# ── Clip State Machine ──
class ClipStatus(str, Enum):
    DISCOVERED = "DISCOVERED"
    DOWNLOADED = "DOWNLOADED"
    TRANSCRIBED = "TRANSCRIBED"
    DECIDED = "DECIDED"
    RENDERED = "RENDERED"
    PACKAGED = "PACKAGED"
    FAILED = "FAILED"


VALID_TRANSITIONS = {
    ClipStatus.DISCOVERED: [ClipStatus.DOWNLOADED, ClipStatus.FAILED],
    ClipStatus.DOWNLOADED: [ClipStatus.TRANSCRIBED, ClipStatus.FAILED],
    ClipStatus.TRANSCRIBED: [ClipStatus.DECIDED, ClipStatus.FAILED],
    ClipStatus.DECIDED: [ClipStatus.RENDERED, ClipStatus.FAILED],
    ClipStatus.RENDERED: [ClipStatus.PACKAGED, ClipStatus.FAILED],
    ClipStatus.PACKAGED: [],
    ClipStatus.FAILED: [],
}


# ── Profile Rules JSON Schema ──
class ProfileRules(BaseModel):
    """Stored in profiles.rules_json"""
    niche: str = "funny livestreamers"
    categories: list[str] = ["Just Chatting", "IRL"]
    languages: list[str] = ["en"]
    length_band_sec: list[int] = Field(default=[20, 40], min_length=2, max_length=2)
    hook_max_delay_sec: float = 2.0
    silence_ratio_max: float = 0.20
    caption_style: str = "bold_white"
    caption_position: str = "bottom_center"
    caption_max_words: int = 3
    hashtag_bank: list[str] = [
        "#shorts", "#funny", "#streamer", "#viral",
        "#twitch", "#kick", "#livestream", "#clips"
    ]
    post_title_template: str = "{title} 😂 #{creator}"
    max_clips_per_creator_per_run: int = 3


# ── Edit Decision JSON Schema (Milestone 3) ──
class Segment(BaseModel):
    start: float
    end: float

class Layout(BaseModel):
    mode: str = "center_crop"  # center_crop | face_track | pip
    target: str = "9:16"
    fallback: str = "center_crop"

class CaptionConfig(BaseModel):
    enabled: bool = True
    style: str = "bold_white"
    position: str = "bottom_center"
    max_words: int = 3

class AudioConfig(BaseModel):
    normalize: bool = True

class OutputSpec(BaseModel):
    max_len_sec: int = 60

class PlatformCopy(BaseModel):
    title: str
    caption: str
    hashtags: list[str]

class EditDecision(BaseModel):
    """Generated per clip — deterministic render instructions."""
    profile_slug: str
    clip_id: str
    segment: Segment
    layout: Layout = Layout()
    captions: CaptionConfig = CaptionConfig()
    audio: AudioConfig = AudioConfig()
    outputs: dict[str, OutputSpec] = {
        "shorts": OutputSpec(max_len_sec=60),
        "tiktok": OutputSpec(max_len_sec=60),
        "reels": OutputSpec(max_len_sec=90),
    }
    post_copy: dict[str, PlatformCopy] = {}


# ── Clip Metadata (from platform APIs) ──
class ClipMeta(BaseModel):
    """Normalized clip metadata from any platform."""
    clip_id: str
    platform: str  # twitch | kick
    title: str = ""
    creator_name: str = ""
    duration_sec: float = 0.0
    view_count: int = 0
    created_at: str = ""
    thumbnail_url: str = ""
    download_url: str = ""
    language: str = "en"
    game_name: str = ""
    raw: dict = {}  # original API response

    def to_json(self) -> str:
        return self.model_dump_json()
