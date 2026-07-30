"""
index_manager.py
Writes chunks + embeddings + metadata to the S3 Vectors target index, and
persists processed media/artifacts to the processed bucket. Single shared
index across all modalities — filter by `modality` field at query time.
"""

import json
import logging
import os
from typing import List

import boto3

import config
from chunker import Chunk
from embeddings import EmbeddingRouter
from metadata import build_metadata
from parsers import ParsedDocument

logger = logging.getLogger(__name__)

s3_client = boto3.client("s3", region_name=config.AWS_REGION)
# S3 Vectors client — boto3 service name may differ depending on SDK version;
# adjust to the actual service identifier available in your environment.
try:
    s3vectors_client = boto3.client("s3vectors", region_name=config.AWS_REGION)
except Exception:
    s3vectors_client = None
    logger.warning("s3vectors client unavailable in this boto3 version; "
                    "vector writes will need a compatible SDK/runtime.")

_embedding_router = EmbeddingRouter()


def persist_chunk_media(doc: ParsedDocument, chunk: Chunk) -> str:
    """Upload the chunk's associated media (image/keyframe/audio slice) to the
    processed bucket. Returns the S3 key, or '' if there's no media file."""
    if not chunk.media_path or not os.path.exists(chunk.media_path):
        return ""

    prefix = config.PROCESSED_PREFIX[doc.modality]
    ext = os.path.splitext(chunk.media_path)[1]
    key = f"{prefix}{doc.doc_id}/chunks/{chunk.chunk_id}{ext}"
    s3_client.upload_file(chunk.media_path, config.PROCESSED_BUCKET, key)
    return key


def persist_chunk_record(doc: ParsedDocument, chunk: Chunk, meta: dict) -> str:
    """Persist the full chunk record (text + metadata) as JSON to the
    processed bucket for auditability / re-indexing without re-processing."""
    prefix = config.PROCESSED_PREFIX[doc.modality]
    key = f"{prefix}{doc.doc_id}/chunks/{chunk.chunk_id}.json"

    record = {
        "chunk_id": chunk.chunk_id,
        "doc_id": doc.doc_id,
        "modality": doc.modality,
        "text": chunk.text,
        "start_sec": chunk.start_sec,
        "end_sec": chunk.end_sec,
        "position": chunk.position,
        "metadata": meta,
    }
    s3_client.put_object(
        Bucket=config.PROCESSED_BUCKET,
        Key=key,
        Body=json.dumps(record).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def write_to_vector_index(chunk: Chunk, vector: List[float], aux_vector, meta: dict, media_key: str):
    """Write a single vector record to the shared S3 Vectors index."""
    if s3vectors_client is None:
        logger.error("No s3vectors client configured; skipping vector write for chunk %s", chunk.chunk_id)
        return

    record_metadata = dict(meta)
    record_metadata["processed_media_s3_key"] = media_key
    if aux_vector:
        record_metadata["has_aux_vector"] = True

    try:
        s3vectors_client.put_vectors(
            vectorBucketName=config.VECTOR_BUCKET,
            indexName=config.VECTOR_INDEX_NAME,
            vectors=[
                {
                    "key": chunk.chunk_id,
                    "data": {"float32": vector},
                    "metadata": record_metadata,
                }
            ],
        )
        if aux_vector:
            s3vectors_client.put_vectors(
                vectorBucketName=config.VECTOR_BUCKET,
                indexName=config.VECTOR_INDEX_NAME,
                vectors=[
                    {
                        "key": f"{chunk.chunk_id}_aux",
                        "data": {"float32": aux_vector},
                        "metadata": {**record_metadata, "vector_role": "aux_image"},
                    }
                ],
            )
    except Exception as e:
        logger.error("Failed to write vector for chunk %s: %s", chunk.chunk_id, e)
        raise


def index_document(doc: ParsedDocument, chunks: List[Chunk]) -> int:
    """Full indexing flow for all chunks belonging to one parsed document.
    Returns the count of successfully indexed chunks."""
    indexed_count = 0
    for chunk in chunks:
        try:
            meta = build_metadata(doc, chunk)
            media_key = persist_chunk_media(doc, chunk)
            persist_chunk_record(doc, chunk, meta)

            embedding_result = _embedding_router.embed_chunk(chunk)
            vector = embedding_result["vector"]
            aux_vector = embedding_result.get("aux_vector")

            if not vector:
                logger.warning("Empty embedding for chunk %s; skipping vector write", chunk.chunk_id)
                continue

            write_to_vector_index(chunk, vector, aux_vector, meta, media_key)
            indexed_count += 1
        except Exception as e:
            logger.error("Failed to index chunk %s (doc %s): %s", chunk.chunk_id, doc.doc_id, e)
            continue

    return indexed_count
