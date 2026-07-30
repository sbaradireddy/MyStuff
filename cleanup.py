"""
cleanup.py
Modality-specific cleanup: dedup, normalization, silence/noise trimming,
scene-aware keyframe pruning. Runs after validation, before PII redaction.
"""

import logging
import os
import subprocess
from typing import Dict, Optional, Set

import config
import media_utils
from parsers import ParsedDocument

logger = logging.getLogger(__name__)

# In-memory dedup cache for a single batch run. For production, back this
# with a persistent store (DynamoDB table keyed by perceptual hash) so
# dedup works across runs, not just within one.
_SEEN_HASHES: Set[str] = set()


def clean(doc: ParsedDocument) -> ParsedDocument:
    cleaners = {
        "text": clean_text,
        "image": clean_image,
        "audio": clean_audio,
        "video": clean_video,
    }
    cleaner_fn = cleaners.get(doc.modality)
    if cleaner_fn is None:
        logger.warning("No cleaner registered for modality '%s'; passing through", doc.modality)
        return doc
    return cleaner_fn(doc)


def clean_text(doc: ParsedDocument) -> ParsedDocument:
    # existing text cleanup logic (whitespace normalization, etc.) goes here
    doc.content_text = " ".join(doc.content_text.split())
    return doc


def clean_image(doc: ParsedDocument) -> ParsedDocument:
    """Perceptual-hash dedup + format normalization to a canonical type."""
    if doc.local_media_path is None:
        return doc

    phash = media_utils.perceptual_hash(doc.local_media_path)
    doc.extra["perceptual_hash"] = str(phash) if phash else None

    if phash is not None:
        is_dup = any(
            media_utils.hamming_distance(phash, _hash_from_str(seen)) <= config.IMAGE_DEDUP_HASH_DISTANCE
            for seen in _SEEN_HASHES
        )
        if is_dup:
            doc.extra["is_duplicate"] = True
            logger.info("Duplicate image detected for %s", doc.source_s3_key)
        else:
            _SEEN_HASHES.add(str(phash))
            doc.extra["is_duplicate"] = False

    doc.local_media_path = _normalize_image_format(doc.local_media_path)
    return doc


def _hash_from_str(hash_str: str):
    try:
        import imagehash
        return imagehash.hex_to_hash(hash_str)
    except Exception:
        return None


def _normalize_image_format(local_path: str, target_format: str = "PNG") -> str:
    try:
        from PIL import Image
        base, _ = os.path.splitext(local_path)
        normalized_path = f"{base}_normalized.png"
        with Image.open(local_path) as img:
            img.convert("RGB").save(normalized_path, format=target_format)
        return normalized_path
    except Exception as e:
        logger.warning("Image format normalization failed for %s: %s", local_path, e)
        return local_path


def clean_audio(doc: ParsedDocument) -> ParsedDocument:
    """Loudness normalization + silence trimming via ffmpeg filters."""
    if doc.local_media_path is None:
        return doc

    base, ext = os.path.splitext(doc.local_media_path)
    cleaned_path = f"{base}_cleaned.wav"

    cmd = [
        config.FFMPEG_BINARY, "-y", "-i", doc.local_media_path,
        "-af", "silenceremove=start_periods=1:start_threshold=-40dB:"
               "detection=peak,loudnorm",
        "-ar", "16000", "-ac", "1",
        cleaned_path,
    ]
    try:
        subprocess.run(cmd, capture_output=True, check=True, timeout=300)
        doc.local_media_path = cleaned_path
    except Exception as e:
        logger.warning("Audio cleanup failed for %s, keeping original: %s", doc.source_s3_key, e)

    return doc


def clean_video(doc: ParsedDocument) -> ParsedDocument:
    """Keyframe-level de-dup (drop near-identical consecutive scenes)."""
    kept_segments = []
    prev_hash = None

    for seg in doc.segments:
        frame_path = seg.get("keyframe_path")
        if not frame_path or not os.path.exists(frame_path):
            kept_segments.append(seg)
            continue

        phash = media_utils.perceptual_hash(frame_path)
        if prev_hash is not None and media_utils.hamming_distance(phash, prev_hash) <= config.IMAGE_DEDUP_HASH_DISTANCE:
            logger.info(
                "Dropping near-duplicate scene %s for %s", seg.get("scene_index"), doc.source_s3_key
            )
            continue

        prev_hash = phash
        kept_segments.append(seg)

    doc.segments = kept_segments
    return doc
