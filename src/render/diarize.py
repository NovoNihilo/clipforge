"""
Speaker diarization for ClipForge.

Uses pyannote-audio to detect "who spoke when" in clip audio.
Maps speaker labels onto existing Whisper word timestamps.

Setup:
  pip install pyannote-audio --break-system-packages
  # Accept model terms at: https://huggingface.co/pyannote/speaker-diarization-3.1
  # Set HF_TOKEN in .env

Usage:
  from src.render.diarize import diarize_speakers, assign_speakers_to_words

  speakers = diarize_speakers("audio.wav")
  words_with_speakers = assign_speakers_to_words(transcript_words, speakers)
"""
import os
from pathlib import Path
from src.utils.log import log
from src.config import settings

# Lazy-loaded pipeline
_pipeline = None


def _get_pipeline():
    """Lazy-load pyannote diarization pipeline."""
    global _pipeline
    if _pipeline is not None:
        return _pipeline

    hf_token = settings.hf_token
    if not hf_token:
        log.warning("HF_TOKEN not set in .env — speaker diarization disabled")
        return None

    try:
        from pyannote.audio import Pipeline
        import torch

        log.info("Loading pyannote speaker diarization model (first run downloads ~1GB)...")
        _pipeline = Pipeline.from_pretrained(
            "pyannote/speaker-diarization-3.1",
            token=hf_token,
        )
        # Use MPS on Apple Silicon if available, otherwise CPU
        if torch.backends.mps.is_available():
            _pipeline.to(torch.device("mps"))
            log.info("Pyannote loaded (MPS accelerated)")
        else:
            log.info("Pyannote loaded (CPU)")
        return _pipeline
    except ImportError:
        log.warning("pyannote-audio not installed — speaker diarization disabled")
        log.warning("  Install with: pip install pyannote-audio")
        return None
    except Exception as e:
        log.warning(f"Pyannote failed to load: {e}")
        return None


def diarize_speakers(
    audio_path: str,
    segment_start: float = 0,
    segment_end: float = 999,
    min_speakers: int = 1,
    max_speakers: int = 4,
) -> list[dict]:
    """
    Run speaker diarization on an audio file.

    Returns: [{"start": float, "end": float, "speaker": "SPEAKER_00"}, ...]
    Timestamps are absolute (matching the source file).
    """
    pipeline = _get_pipeline()
    if pipeline is None:
        return []

    if not Path(audio_path).exists():
        log.warning(f"Audio file not found for diarization: {audio_path}")
        return []

    try:
        diarization = pipeline(
            audio_path,
            min_speakers=min_speakers,
            max_speakers=max_speakers,
        )

        segments = []
        for turn, _, speaker in diarization.itertracks(yield_label=True):
            # Only keep segments that overlap with our edit window
            if turn.end <= segment_start or turn.start >= segment_end:
                continue
            segments.append({
                "start": round(turn.start, 3),
                "end": round(turn.end, 3),
                "speaker": speaker,
            })

        # Count unique speakers
        unique_speakers = set(s["speaker"] for s in segments)
        log.info(f"  🎙️ Diarization: {len(unique_speakers)} speaker(s) detected, {len(segments)} segments")

        return segments

    except Exception as e:
        log.warning(f"Diarization failed: {e}")
        return []


def assign_speakers_to_words(
    words: list[dict],
    diarization_segments: list[dict],
    segment_start: float = 0,
    segment_end: float = 999,
) -> list[dict]:
    """
    Assign speaker labels to each word based on diarization output.

    Takes Whisper word timestamps and pyannote speaker segments,
    returns words with an added "speaker" field.

    Words that don't fall within any diarization segment get "SPEAKER_00" (default).
    """
    if not diarization_segments:
        # No diarization data — all words get default speaker
        for w in words:
            w["speaker"] = "SPEAKER_00"
        return words

    for w in words:
        if w["end"] <= segment_start or w["start"] >= segment_end:
            continue

        word_mid = (w["start"] + w["end"]) / 2
        best_speaker = "SPEAKER_00"
        best_overlap = 0

        for seg in diarization_segments:
            # Check overlap between word and diarization segment
            overlap_start = max(w["start"], seg["start"])
            overlap_end = min(w["end"], seg["end"])
            overlap = max(0, overlap_end - overlap_start)

            if overlap > best_overlap:
                best_overlap = overlap
                best_speaker = seg["speaker"]

            # Also check if word midpoint falls within segment (backup)
            if best_overlap == 0 and seg["start"] <= word_mid <= seg["end"]:
                best_speaker = seg["speaker"]

        w["speaker"] = best_speaker

    return words