"""SQLite DB setup + schema. Upgrade notes for Postgres inline."""
import sqlite3
from pathlib import Path
from src.config import settings

SCHEMA = """
-- Profiles: each is a niche media identity
CREATE TABLE IF NOT EXISTS profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    rules_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Creators: streamers across platforms
CREATE TABLE IF NOT EXISTS creators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,           -- 'twitch' | 'kick'
    platform_user_id TEXT NOT NULL,   -- broadcaster_id (twitch) or slug (kick)
    display_name TEXT NOT NULL,
    channel_url TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(platform, platform_user_id)
);

-- Profile <-> Creator link
CREATE TABLE IF NOT EXISTS profile_creators (
    profile_id INTEGER NOT NULL REFERENCES profiles(id),
    creator_id INTEGER NOT NULL REFERENCES creators(id),
    is_enabled INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (profile_id, creator_id)
);

-- Clips with state machine
CREATE TABLE IF NOT EXISTS clips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,           -- 'twitch' | 'kick'
    clip_id TEXT NOT NULL,            -- platform's unique clip ID
    creator_id INTEGER NOT NULL REFERENCES creators(id),
    profile_id INTEGER NOT NULL REFERENCES profiles(id),
    status TEXT NOT NULL DEFAULT 'DISCOVERED',
    -- DISCOVERED -> DOWNLOADED -> TRANSCRIBED -> DECIDED -> RENDERED -> PACKAGED
    -- Any state can go to FAILED
    viral_score INTEGER,              -- LLM viral score (1-10), set at DECIDED
    fail_reason TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    paths_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(platform, clip_id)
);

-- Cursors: track last fetch per creator to avoid refetching
CREATE TABLE IF NOT EXISTS cursors (
    creator_id INTEGER PRIMARY KEY REFERENCES creators(id),
    last_fetched_at TEXT,             -- ISO datetime of newest clip seen
    platform_cursor TEXT,             -- platform-specific pagination token
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Index for clip lookups
CREATE INDEX IF NOT EXISTS idx_clips_status ON clips(status);
CREATE INDEX IF NOT EXISTS idx_clips_profile ON clips(profile_id, status);
"""

# Migration: add viral_score column to existing DBs
MIGRATIONS = [
    "ALTER TABLE clips ADD COLUMN viral_score INTEGER",
]


def get_db(db_path: str | None = None) -> sqlite3.Connection:
    """Get a SQLite connection with WAL mode + foreign keys."""
    path = db_path or settings.database_path
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str | None = None) -> sqlite3.Connection:
    """Create tables if they don't exist, then run migrations."""
    conn = get_db(db_path)
    conn.executescript(SCHEMA)
    conn.commit()

    # Run migrations (ignore if column already exists)
    for migration in MIGRATIONS:
        try:
            conn.execute(migration)
            conn.commit()
        except sqlite3.OperationalError:
            pass  # Column already exists

    return conn