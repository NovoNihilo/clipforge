"""
Content moderation for ClipForge.

Two-phase approach:
  1. Pre-filter: fast regex scan rejects clips with slurs, gambling, explicit sexual content
  2. Word-level: identifies exact words to bleep in audio + replace in captions

Used by:
  - decider.py (pre-filter before LLM call)
  - renderer.py (word-level bleep map for audio/captions)
"""
import re
from src.utils.log import log


# ══════════════════════════════════════════════════════════════════════════════
# HARD REJECT — clip is completely unusable, skip entirely
# ══════════════════════════════════════════════════════════════════════════════

SLUR_PATTERNS = [
    r'\bn[-_]+(i|1)gg(a|er|ah?|uh?)s?\b',
    r'\bf[-_]*a[-_]*g(g[-_]*(o[-_]*t|i[-_]*t))?s?\b',
    r'\br[-_]*e[-_]*t[-_]*a[-_]*r[-_]*d(ed)?\b',
    r'\bk[-_]*i[-_]*k[-_]*e[-_]*s?\b',
    r'\bs[-_]*p[-_]*i[-_]*c[-_]*s?\b',
    r'\bch[-_]*i[-_]*n[-_]*k[-_]*s?\b',
    r'\btr[-_]*a[-_]*nn(y|ie)s?\b',
    r'\bw[-_]*e[-_]*t[-_]*b[-_]*a[-_]*c[-_]*k[-_]*s?\b',
]

# Gambling/casino — these clips get shadow-banned on all major platforms
GAMBLING_PATTERNS = [
    r'\b(slots?|slot.?machine)\b',
    r'\b(blackjack|roulette|baccarat|craps)\b',
    r'\b(casino|gambling|gamble|wagering)\b',
    r'\b(stake\.com|stake\b)',
    r'\b(kick.*sponsor|sponsored.*gambling)\b',
    r'\b(house.?edge|jackpot|big.?win|max.?bet)\b',
    r'\b(online.?poker)\b',
]

# Explicit sexual content — hard reject (not innuendo, actual explicit)
SEXUAL_EXPLICIT_PATTERNS = [
    r'\b(porn|pornhub|onlyfans|xxx|hentai)\b',
    r'\b(blow.?job|hand.?job|rim.?job)\b',
    r'\b(orgasm|cum(ming|shot)?|jerk(ing)?.?off|masturb)\b',
    r'\b(anal|dildo|vibrator|butt.?plug)\b',
    r'\b(nude|naked|tits|boobs|nipple)\b',
    r'\b(sex.?tape|sex.?act|intercourse)\b',
    r'\b(erection|penis|vagina|genitals?)\b',
    r'\b(fetish|bdsm|bondage|dominatrix)\b',
    r'\b(pedophile|pedo|grooming|minor)\b',
]


# ══════════════════════════════════════════════════════════════════════════════
# BLEEP-WORTHY — word gets muted in audio + replaced with [BLEEP] in captions
# These are words that, if left in, can trigger demonetization but the clip
# itself may still be good content.
# ══════════════════════════════════════════════════════════════════════════════

BLEEP_WORDS = {
    # Heavy profanity
    'fuck', 'fucking', 'fucked', 'fucker', 'fuckin', 'fucks', 'motherfucker',
    'motherfucking', 'motherfuckers',
    'shit', 'shitting', 'shitty', 'bullshit', 'horseshit',
    'bitch', 'bitches', 'bitching', 'bitchass',
    'ass', 'asshole', 'assholes', 'dumbass', 'jackass', 'badass',
    'dick', 'dicks', 'dickhead',
    'pussy', 'pussies',
    'cock', 'cocks', 'cocksucker',
    'damn', 'goddamn', 'goddammit',
    'bastard', 'bastards',
    'whore', 'whores',
    'cunt', 'cunts',
    # Slurs that might survive the hard-reject (whisper mis-transcriptions etc)
    'nigga', 'niggas', 'nigger', 'niggers',
    'faggot', 'faggots', 'fag', 'fags',
    'retard', 'retarded', 'retards',
}

# Max allowed bleep-worthy words before we reject the whole clip.
# If someone drops 1-3 f-bombs that's fine (we bleep them).
# If it's 10+ the clip is unwatchable even with bleeps.
MAX_BLEEP_WORDS = 8

# Max profanity density (bleepable words / total words)
MAX_PROFANITY_DENSITY = 0.12


def content_pre_filter(full_text: str) -> tuple[bool, str]:
    """
    Fast pre-filter before LLM call.
    Returns (passed, reject_reason).
    """
    text_lower = full_text.lower()

    # 1. Hard reject: slurs
    for pattern in SLUR_PATTERNS:
        if re.search(pattern, text_lower):
            return False, "hard_reject:slur_detected"

    # 2. Hard reject: gambling/casino content
    for pattern in GAMBLING_PATTERNS:
        if re.search(pattern, text_lower):
            return False, "hard_reject:gambling_content"

    # 3. Hard reject: explicit sexual content
    for pattern in SEXUAL_EXPLICIT_PATTERNS:
        if re.search(pattern, text_lower):
            return False, "hard_reject:explicit_sexual_content"

    # 4. Profanity density — too many bleeps makes the clip unwatchable
    words = text_lower.split()
    if words:
        bleep_count = sum(1 for w in words if _clean_word(w) in BLEEP_WORDS)

        if bleep_count > MAX_BLEEP_WORDS:
            return False, f"hard_reject:too_many_profanities({bleep_count})"

        density = bleep_count / len(words)
        if density > MAX_PROFANITY_DENSITY:
            return False, f"hard_reject:profanity_density({density:.0%})"

    return True, ""


def _clean_word(word: str) -> str:
    """Strip punctuation for matching."""
    return re.sub(r'[^a-z]', '', word.lower())


def get_bleep_map(transcript: dict, segment_start: float = 0, segment_end: float = 999) -> list[dict]:
    """
    Scan word-level timestamps and return a list of words to bleep.

    Returns: [{"start": float, "end": float, "word": str, "replacement": "[BLEEP]"}, ...]

    Used by renderer to:
      - Mute audio at these timestamps
      - Replace caption text with [BLEEP]
    """
    bleep_list = []

    words = transcript.get("words", [])
    if not words:
        # Fallback: no word timestamps, can't do precise bleeping
        return []

    for w in words:
        # Only process words within our edit segment
        if w["end"] <= segment_start or w["start"] >= segment_end:
            continue

        cleaned = _clean_word(w["word"])
        if cleaned in BLEEP_WORDS:
            bleep_list.append({
                "start": w["start"],
                "end": w["end"],
                "word": w["word"],
                "replacement": "[BLEEP]",
            })

    if bleep_list:
        log.info(f"  🔇 Found {len(bleep_list)} words to bleep")

    return bleep_list


def censor_caption_text(text: str) -> str:
    """
    Replace bleep-worthy words in a caption string with [BLEEP].
    Used for caption display text.
    """
    words = text.split()
    result = []
    for w in words:
        if _clean_word(w) in BLEEP_WORDS:
            result.append("[BLEEP]")
        else:
            result.append(w)
    return " ".join(result)