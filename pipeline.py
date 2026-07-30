"""
pipeline.py
Single orchestrator for the RAG ingestion framework, now routing across
text/image/audio/video. Same S3 Source -> Parse -> Validate -> Cleanup ->
PII Redact -> Chunk -> Tag Metadata -> Target flow for every modality.

Usage:
    python pipeline.py --prefix raw/                 # process everything under a prefix
    python pipeline.py --key raw/video/clip123.mp4    # process a single object
"""

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import List

import boto3

import chunker
import cleanup
import config
import index_manager
import parsers
import privacy
import validators

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("pipeline")

s3_client = boto3.client("s3", region_name=config.AWS_REGION)


class IngestionManifestEntry:
    """Mirrors the manifest pattern used by delete_vectors_manifest.py so
    failures/successes are auditable and retryable the same way."""

    def __init__(self, s3_key: str):
        self.s3_key = s3_key
        self.doc_id = str(uuid.uuid4())
        self.status = "pending"
        self.reason = ""
        self.chunks_indexed = 0
        self.timestamp = datetime.now(timezone.utc).isoformat()

    def to_dict(self):
        return {
            "s3_key": self.s3_key,
            "doc_id": self.doc_id,
            "status": self.status,
            "reason": self.reason,
            "chunks_indexed": self.chunks_indexed,
            "timestamp": self.timestamp,
        }


def quarantine_object(s3_key: str, modality: str, reason: str) -> None:
    dest_prefix = config.QUARANTINE_PREFIX.get(modality, "quarantine/unknown/")
    dest_key = f"{dest_prefix}{s3_key.split('/')[-1]}"
    try:
        s3_client.copy_object(
            Bucket=config.QUARANTINE_BUCKET,
            CopySource={"Bucket": config.SOURCE_BUCKET, "Key": s3_key},
            Key=dest_key,
        )
        logger.warning("Quarantined %s -> %s (%s)", s3_key, dest_key, reason)
    except Exception as e:
        logger.error("Failed to quarantine %s: %s", s3_key, e)


def process_object(s3_key: str) -> IngestionManifestEntry:
    entry = IngestionManifestEntry(s3_key)

    try:
        modality = config.modality_for_key(s3_key)
    except ValueError as e:
        entry.status = "failed"
        entry.reason = str(e)
        logger.error(entry.reason)
        return entry

    logger.info("Processing %s as modality=%s (doc_id=%s)", s3_key, modality, entry.doc_id)

    # ---- Parse ----
    try:
        parser = parsers.get_parser(modality)
        doc = parser.parse(s3_key, entry.doc_id)
    except Exception as e:
        entry.status = "failed"
        entry.reason = f"parse_error: {e}"
        logger.error("Parse failed for %s: %s", s3_key, e)
        quarantine_object(s3_key, modality, entry.reason)
        return entry

    # ---- Validate ----
    is_valid, reason = validators.validate(doc)
    if not is_valid:
        entry.status = "failed"
        entry.reason = f"validation_failed: {reason}"
        logger.warning("Validation failed for %s: %s", s3_key, reason)
        quarantine_object(s3_key, modality, entry.reason)
        return entry

    # ---- Cleanup ----
    try:
        doc = cleanup.clean(doc)
        if doc.extra.get("is_duplicate"):
            entry.status = "skipped"
            entry.reason = "duplicate_detected"
            logger.info("Skipping duplicate: %s", s3_key)
            return entry
    except Exception as e:
        entry.status = "failed"
        entry.reason = f"cleanup_error: {e}"
        logger.error("Cleanup failed for %s: %s", s3_key, e)
        quarantine_object(s3_key, modality, entry.reason)
        return entry

    # ---- PII Redact ----
    try:
        doc = privacy.apply_privacy_controls(doc)
    except Exception as e:
        entry.status = "failed"
        entry.reason = f"privacy_error: {e}"
        logger.error("Privacy step failed for %s: %s", s3_key, e)
        quarantine_object(s3_key, modality, entry.reason)
        return entry

    # ---- Chunk ----
    try:
        chunks = chunker.chunk_document(doc)
    except Exception as e:
        entry.status = "failed"
        entry.reason = f"chunk_error: {e}"
        logger.error("Chunking failed for %s: %s", s3_key, e)
        quarantine_object(s3_key, modality, entry.reason)
        return entry

    # ---- Tag Metadata + Target (index_manager builds metadata internally) ----
    try:
        indexed_count = index_manager.index_document(doc, chunks)
        entry.chunks_indexed = indexed_count
        entry.status = "success" if indexed_count > 0 else "failed"
        entry.reason = "ok" if indexed_count > 0 else "no_chunks_indexed"
    except Exception as e:
        entry.status = "failed"
        entry.reason = f"index_error: {e}"
        logger.error("Indexing failed for %s: %s", s3_key, e)
        quarantine_object(s3_key, modality, entry.reason)
        return entry

    logger.info("Completed %s: %s chunks indexed (doc_id=%s)", s3_key, entry.chunks_indexed, entry.doc_id)
    return entry


def list_objects(prefix: str) -> List[str]:
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=config.SOURCE_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def write_manifest(entries: List[IngestionManifestEntry]) -> None:
    manifest_key = f"manifests/ingestion_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    body = json.dumps([e.to_dict() for e in entries], indent=2).encode("utf-8")
    s3_client.put_object(Bucket=config.PROCESSED_BUCKET, Key=manifest_key, Body=body,
                          ContentType="application/json")
    logger.info("Wrote ingestion manifest: s3://%s/%s", config.PROCESSED_BUCKET, manifest_key)


def main():
    parser = argparse.ArgumentParser(description="RAG multi-modal ingestion pipeline")
    parser.add_argument("--prefix", help="S3 prefix to process (all objects under it)")
    parser.add_argument("--key", help="Single S3 object key to process")
    args = parser.parse_args()

    if not args.prefix and not args.key:
        parser.error("Provide either --prefix or --key")

    keys = [args.key] if args.key else list_objects(args.prefix)
    logger.info("Found %d object(s) to process", len(keys))

    entries = [process_object(k) for k in keys]
    write_manifest(entries)

    succeeded = sum(1 for e in entries if e.status == "success")
    failed = sum(1 for e in entries if e.status == "failed")
    skipped = sum(1 for e in entries if e.status == "skipped")
    logger.info("Done. success=%d failed=%d skipped=%d", succeeded, failed, skipped)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
