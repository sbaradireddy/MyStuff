"""
validators.py
Modality-specific validation rules, run right after parsing and before cleanup.
Each validator returns (is_valid: bool, reason: str).
"""

import logging
import os
from typing import Tuple

import config
import media_utils
from parsers import ParsedDocument

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    pass


def validate(doc: ParsedDocument) -> Tuple[bool, str]:
    """Dispatch to the correct validator based on modality."""
    validators = {
        "text": validate_text,
        "image": validate_image,
        "audio": validate_audio,
        "video": validate_video,
    }
    validator_fn = validators.get(doc.modality)
    if validator_fn is None:
        return False, f"No validator registered for modality '{doc.modality}'"
    return validator_fn(doc)


def validate_text(doc: ParsedDocument) -> Tuple[bool, str]:
    if not doc.content_text or not doc.content_text.strip():
        return False, "Empty text content"
    return True, "ok"


def validate_image(doc: ParsedDocument) -> Tuple[bool, str]:
    width = doc.extra.get("width", 0)
    height = doc.extra.get("height", 0)
    fmt = (doc.extra.get("format") or "").upper()

    if width < config.IMAGE_MIN_WIDTH_PX or height < config.IMAGE_MIN_HEIGHT_PX:
        return False, f"Image too small: {width}x{height}"

    if fmt not in config.IMAGE_ALLOWED_FORMATS:
        return False, f"Unsupported image format: {fmt}"

    if doc.local_media_path and os.path.exists(doc.local_media_path):
        size_mb = os.path.getsize(doc.local_media_path) / (1024 * 1024)
        if size_mb > config.IMAGE_MAX_SIZE_MB:
            return False, f"Image exceeds max size: {size_mb:.1f}MB"

    return True, "ok"


def validate_audio(doc: ParsedDocument) -> Tuple[bool, str]:
    duration = doc.extra.get("duration_sec", 0)
    sample_rate = doc.extra.get("sample_rate", 0)

    if duration <= config.AUDIO_MIN_DURATION_SEC:
        return False, f"Audio too short: {duration}s"

    if duration > config.AUDIO_MAX_DURATION_SEC:
        return False, f"Audio exceeds max duration: {duration}s"

    if sample_rate < config.AUDIO_MIN_SAMPLE_RATE:
        return False, f"Sample rate too low: {sample_rate}Hz"

    return True, "ok"


def validate_video(doc: ParsedDocument) -> Tuple[bool, str]:
    duration = doc.extra.get("duration_sec", 0)
    codec = doc.extra.get("codec", "")
    width = doc.extra.get("width", 0)
    height = doc.extra.get("height", 0)

    if duration <= 0:
        return False, "Zero/invalid video duration (likely corrupt container)"

    if duration > config.VIDEO_MAX_DURATION_SEC:
        return False, f"Video exceeds max duration: {duration}s"

    if codec not in config.VIDEO_ALLOWED_CODECS:
        return False, f"Unsupported video codec: {codec}"

    if max(width, height) > config.VIDEO_MAX_RESOLUTION_PX:
        return False, f"Video resolution exceeds max: {width}x{height}"

    return True, "ok"
