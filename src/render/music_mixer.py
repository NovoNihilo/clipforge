"""
Background music mixer for ClipForge.

Setup:
  mkdir -p music/funny music/hype music/chill music/sad
  Drop royalty-free MP3s into the appropriate mood folder.

Recommended: https://pixabay.com/music/ (no attribution required)
"""
import random
from pathlib import Path
from src.utils.log import log


MUSIC_DIR = Path("music")
DEFAULT_MOOD = "funny"
BG_VOLUME = 0.10


def get_music_track(mood: str = DEFAULT_MOOD) -> str | None:
    """Pick a random music track from the mood folder."""
    mood_dir = MUSIC_DIR / mood

    if mood_dir.exists():
        tracks = list(mood_dir.glob("*.mp3")) + list(mood_dir.glob("*.wav")) + list(mood_dir.glob("*.mp4"))
        if tracks:
            track = random.choice(tracks)
            log.info(f"  🎵 Music: {track.name} ({mood})")
            return str(track)

    # Fallback: try any mood folder
    if MUSIC_DIR.exists():
        all_tracks = (
            list(MUSIC_DIR.rglob("*.mp3"))
            + list(MUSIC_DIR.rglob("*.wav"))
            + list(MUSIC_DIR.rglob("*.mp4"))
        )
        if all_tracks:
            track = random.choice(all_tracks)
            log.info(f"  🎵 Music (fallback): {track.name}")
            return str(track)

    return None


def build_music_filter(music_path: str, clip_duration: float, volume: float = BG_VOLUME) -> dict:
    """
    Build ffmpeg args to mix background music under clip audio.
    """
    fade_start = max(0, clip_duration - 2.0)

    filter_complex = (
        f"[0:a]loudnorm=I=-14:TP=-1:LRA=11[speech];"
        f"[1:a]atrim=0:{clip_duration:.1f},"
        f"afade=t=in:st=0:d=1.0,"
        f"afade=t=out:st={fade_start:.1f}:d=2.0,"
        f"volume={volume}[music];"
        f"[speech][music]amix=inputs=2:duration=first:dropout_transition=2[out]"
    )

    return {
        "input_args": ["-i", music_path],
        "filter_complex": filter_complex,
        "output_map": ["-map", "0:v", "-map", "[out]"],
    }