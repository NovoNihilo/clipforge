# ClipForge — Milestone 0 + 1

## Architecture Decision Record

| Decision | Choice | Reason |
|----------|--------|--------|
| Language | Python 3.11+ | Best macOS support for faster-whisper + ffmpeg subprocess |
| DB | SQLite (WAL mode) | Zero setup MVP; upgrade path to Postgres documented |
| HTTP | httpx (async) | Async + streaming downloads + clean retry |
| Validation | Pydantic v2 | Schema validation for JSON, settings, edit decisions |
| Queue | In-process async | MVP; upgrade to Celery/Redis documented |
| Storage | Local filesystem | MVP; upgrade to S3/R2 = swap path helpers |

## Folder Structure

```
clipforge/
├── .env.example          # Config template
├── .env                  # Your secrets (git-ignored)
├── .gitignore
├── pyproject.toml        # Dependencies
├── src/
│   ├── config.py         # Settings from .env
│   ├── seed.py           # Milestone 0+1 runner
│   ├── db/
│   │   └── database.py   # Schema + connection
│   ├── models/
│   │   └── schemas.py    # Pydantic models + state machine
│   ├── discovery/
│   │   ├── discover.py   # Orchestrator
│   │   ├── twitch_api.py # Twitch Helix client
│   │   └── kick_api.py   # Kick unofficial client
│   ├── download/
│   │   └── downloader.py # Download + state update
│   └── utils/
│       ├── http.py       # Retry + backoff client
│       └── log.py        # Rich logging
├── assets/               # Downloaded clips (git-ignored)
├── outputs/              # Final publish packs (git-ignored)
└── tests/
```

## State Machine

```
DISCOVERED → DOWNLOADED → TRANSCRIBED → DECIDED → RENDERED → PACKAGED
     ↓            ↓            ↓           ↓          ↓
   FAILED       FAILED       FAILED      FAILED     FAILED
```
