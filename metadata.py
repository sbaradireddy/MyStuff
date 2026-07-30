"""
metadata.py
Builds the metadata payload attached to each chunk before it's written to the
vector index. Existing text fields are untouched; modality-specific nested
blocks are additive.
"""

import datetime
from typing import Any, Dict

from chunker import Chunk
from parsers import ParsedDocument


def build_metadata(doc: ParsedDocument, chunk: Chunk) -> Dict[str, Any]:
    base = {
        "doc_id": doc.doc_id,
        "chunk_id": chunk.chunk_id,
        "modality": doc.modality,
        "source_s3_key": doc.source_s3_key,
        "ingestion_timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "pii_redacted": True,
    }

    if doc.modality == "text":
        base["text_meta"] = {
            "char_count": len(chunk.text),
        }

    elif doc.modality == "image":
        base["image_meta"] = {
            "width": doc.extra.get("width"),
            "height": doc.extra.get("height"),
            "format": doc.extra.get("format"),
            "ocr_text_present": bool(doc.extra.get("ocr_text")),
            "caption": doc.extra.get("caption", ""),
            "perceptual_hash": doc.extra.get("perceptual_hash"),
            "exif_stripped": doc.extra.get("exif_stripped", False),
            "is_tile": chunk.metadata.get("is_tile", False),
            "tile_position": chunk.position or None,
        }

    elif doc.modality == "audio":
        base["audio_meta"] = {
            "duration_sec": doc.extra.get("duration_sec"),
            "sample_rate": doc.extra.get("sample_rate"),
            "channels": doc.extra.get("channels"),
            "chunk_start_sec": chunk.start_sec,
            "chunk_end_sec": chunk.end_sec,
        }

    elif doc.modality == "video":
        base["video_meta"] = {
            "duration_sec": doc.extra.get("duration_sec"),
            "fps": doc.extra.get("fps"),
            "resolution": f"{doc.extra.get('width')}x{doc.extra.get('height')}",
            "scene_count": doc.extra.get("scene_count"),
            "scene_index": chunk.position.get("scene_index") if chunk.position else None,
            "chunk_start_sec": chunk.start_sec,
            "chunk_end_sec": chunk.end_sec,
        }

    return base
