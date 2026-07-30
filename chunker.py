"""
chunker.py
Modality-specific chunking. Each chunk is the atomic unit that gets embedded
and written to the vector index.
"""

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import config
from parsers import ParsedDocument

logger = logging.getLogger(__name__)


@dataclass
class Chunk:
    chunk_id: str
    doc_id: str
    modality: str
    text: str = ""                       # text to embed (OCR/caption/transcript slice)
    media_path: Optional[str] = None     # local path to associated media (image/keyframe)
    start_sec: Optional[float] = None
    end_sec: Optional[float] = None
    position: Dict[str, Any] = field(default_factory=dict)  # e.g. tile row/col, scene index
    metadata: Dict[str, Any] = field(default_factory=dict)


def chunk_document(doc: ParsedDocument) -> List[Chunk]:
    chunkers = {
        "text": chunk_text,
        "image": chunk_image,
        "audio": chunk_audio,
        "video": chunk_video,
    }
    chunker_fn = chunkers.get(doc.modality)
    if chunker_fn is None:
        raise ValueError(f"No chunker registered for modality '{doc.modality}'")
    return chunker_fn(doc)


def chunk_text(doc: ParsedDocument) -> List[Chunk]:
    # existing text chunking logic (sentence/paragraph/window-based) goes here.
    # simple fixed-size fallback shown for completeness:
    window = 1000
    overlap = 100
    text = doc.content_text
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + window, len(text))
        chunks.append(
            Chunk(
                chunk_id=str(uuid.uuid4()),
                doc_id=doc.doc_id,
                modality="text",
                text=text[start:end],
            )
        )
        start = end - overlap if end - overlap > start else end
    return chunks or [Chunk(chunk_id=str(uuid.uuid4()), doc_id=doc.doc_id, modality="text", text=text)]


def chunk_image(doc: ParsedDocument) -> List[Chunk]:
    """Default: 1 image = 1 chunk. Large images get tiled into a grid,
    each tile becoming its own chunk with row/col position metadata."""
    width = doc.extra.get("width", 0)
    height = doc.extra.get("height", 0)

    if max(width, height) <= config.IMAGE_TILE_THRESHOLD_PX:
        return [
            Chunk(
                chunk_id=str(uuid.uuid4()),
                doc_id=doc.doc_id,
                modality="image",
                text=doc.content_text,
                media_path=doc.local_media_path,
                metadata={"is_tile": False, "width": width, "height": height},
            )
        ]

    return _tile_image(doc, width, height)


def _tile_image(doc: ParsedDocument, width: int, height: int) -> List[Chunk]:
    tile_size = config.IMAGE_TILE_SIZE_PX
    chunks = []
    try:
        from PIL import Image
        with Image.open(doc.local_media_path) as img:
            for top in range(0, height, tile_size):
                for left in range(0, width, tile_size):
                    box = (left, top, min(left + tile_size, width), min(top + tile_size, height))
                    tile = img.crop(box)
                    tile_path = f"{doc.local_media_path}_tile_{top}_{left}.png"
                    tile.save(tile_path)
                    row, col = top // tile_size, left // tile_size
                    chunks.append(
                        Chunk(
                            chunk_id=str(uuid.uuid4()),
                            doc_id=doc.doc_id,
                            modality="image",
                            text=doc.content_text,  # OCR/caption applies to whole image; duplicated per tile
                            media_path=tile_path,
                            position={"row": row, "col": col},
                            metadata={"is_tile": True, "width": width, "height": height},
                        )
                    )
    except Exception as e:
        logger.warning("Image tiling failed for %s, falling back to single chunk: %s", doc.source_s3_key, e)
        return [
            Chunk(
                chunk_id=str(uuid.uuid4()),
                doc_id=doc.doc_id,
                modality="image",
                text=doc.content_text,
                media_path=doc.local_media_path,
                metadata={"is_tile": False, "width": width, "height": height},
            )
        ]
    return chunks


def chunk_audio(doc: ParsedDocument) -> List[Chunk]:
    """Fixed time-window chunks with overlap, each carrying the transcript
    slice that falls within [start_sec, end_sec)."""
    window = config.AUDIO_CHUNK_WINDOW_SEC
    overlap = config.AUDIO_CHUNK_OVERLAP_SEC
    duration = doc.extra.get("duration_sec", 0)

    if duration <= 0 or not doc.segments:
        return [
            Chunk(
                chunk_id=str(uuid.uuid4()),
                doc_id=doc.doc_id,
                modality="audio",
                text=doc.content_text,
                media_path=doc.local_media_path,
            )
        ]

    chunks = []
    start = 0.0
    while start < duration:
        end = min(start + window, duration)
        slice_text = " ".join(
            seg["text"] for seg in doc.segments
            if seg.get("start", 0) < end and seg.get("end", 0) > start
        )
        chunks.append(
            Chunk(
                chunk_id=str(uuid.uuid4()),
                doc_id=doc.doc_id,
                modality="audio",
                text=slice_text,
                media_path=doc.local_media_path,
                start_sec=start,
                end_sec=end,
            )
        )
        start = end - overlap if end - overlap > start else end

    return chunks


def chunk_video(doc: ParsedDocument) -> List[Chunk]:
    """One chunk per scene: keyframe + the transcript slice aligned to that
    scene's time window."""
    if not doc.segments:
        return [
            Chunk(
                chunk_id=str(uuid.uuid4()),
                doc_id=doc.doc_id,
                modality="video",
                text=doc.content_text,
                media_path=doc.local_media_path,
            )
        ]

    transcript_segments = doc.extra.get("transcript_segments", [])
    chunks = []
    for seg in doc.segments:
        start, end = seg["start_sec"], seg["end_sec"]
        slice_text = " ".join(
            t["text"] for t in transcript_segments
            if t.get("start", 0) < end and t.get("end", 0) > start
        )
        chunks.append(
            Chunk(
                chunk_id=str(uuid.uuid4()),
                doc_id=doc.doc_id,
                modality="video",
                text=slice_text,
                media_path=seg.get("keyframe_path"),
                start_sec=start,
                end_sec=end,
                position={"scene_index": seg.get("scene_index")},
            )
        )
    return chunks
