# RAG Ingestion Framework — Multi-Modal (Image / Audio / Video) Low-Level Design
## AWS-Native Implementation | Python

**Scope:** Extend the existing text-only ingestion framework to ingest image, audio, and video documents, using **AWS managed services only**, while preserving the current architecture, repo layout, and file responsibilities.

**Unchanged core flow:**
```
S3 (Source) → Parse → Validate → Cleanup → PII Redact → Chunk → Tag Metadata → S3 Vectors (Target)
```

---

## 1. Design Constraints

| Constraint | Decision |
|---|---|
| AWS services only | Textract (OCR), Rekognition (faces/moderation), Transcribe (ASR), Bedrock (embeddings — Titan Text + Titan Multimodal), S3 Vectors (target store), Lambda/Step Functions (orchestration), DynamoDB (dedup + manifest state), EventBridge (async job completion), KMS (encryption) |
| Language | Python 3.12, boto3 |
| Repo structure | No renames, no new top-level pipeline — same files extended, 2 new shared-helper files added |
| Async workloads | Transcribe (audio/video) and large Rekognition/Textract batch calls are **asynchronous** — orchestration must handle polling/callback, not just synchronous function calls |

---

## 2. Architecture (AWS Service-Annotated)

```
┌─────────────────────────────────────────────────────────────────────┐
│  S3 SOURCE BUCKET                                                   │
│  raw/text/  raw/image/  raw/audio/  raw/video/                      │
└───────────────────────────────┬─────────────────────────────────────┘
                                 │  S3 Event Notification (ObjectCreated)
                                 ▼
                    ┌────────────────────────────┐
                    │  EventBridge Rule            │
                    │  → triggers Step Functions    │
                    │    state machine per object   │
                    └───────────────┬────────────────┘
                                    ▼
                    ┌──────────────────────────────────────┐
                    │  STEP FUNCTIONS: IngestionWorkflow     │
                    │  (pipeline.py logic, expressed as a    │
                    │   state machine so long ASR/Rekognition│
                    │   jobs don't block a single Lambda)    │
                    └───────────────┬──────────────────────────┘
                                    │
        ┌───────────────┬──────────┼───────────┬───────────────┐
        ▼               ▼          ▼           ▼               ▼
   [Route by       [PARSE]     [VALIDATE]  [CLEANUP]     [PII REDACT]
    modality]        Lambda      Lambda      Lambda         Lambda
        │               │          │           │               │
        │       TEXT: read S3 object directly
        │       IMAGE: Textract (OCR, sync) + Rekognition DetectFaces (sync)
        │       AUDIO: Transcribe StartTranscriptionJob (ASYNC)
        │       VIDEO: MediaConvert/ffmpeg-Lambda-layer (frame extraction) +
        │              Transcribe StartTranscriptionJob (ASYNC) on extracted audio
        │
        │   ── Async branch (audio/video) ──
        │   Step Functions "Wait for Callback" (waitForTaskToken) OR
        │   EventBridge rule on "Transcribe Job State Change" → resumes workflow
        │
        ▼
   ┌────────────────┐   ┌────────────────┐   ┌──────────────────┐
   │    [CHUNK]      │→ │ [TAG METADATA]  │→ │ [EMBED + INDEX]   │
   │    Lambda        │   │   Lambda        │   │   Lambda          │
   └────────────────┘   └────────────────┘   └──────────────────┘
                                                        │
                              Bedrock InvokeModel                 │
                        (Titan Text Embeddings /                  │
                         Titan Multimodal Embeddings)              │
                                                        ▼
                                          ┌─────────────────────────┐
                                          │  S3 VECTORS (Target)     │
                                          │  single shared index,    │
                                          │  filter by `modality`    │
                                          └─────────────────────────┘

   Failure at any stage → DynamoDB manifest entry (status=failed) +
                           S3 copy to quarantine/{modality}/ prefix
                           (same pattern as delete_vectors_manifest.py)
```

---

## 3. AWS Service Mapping (per stage × modality)

| Stage | Text | Image | Audio | Video |
|---|---|---|---|---|
| **Parse** | S3 GetObject, read bytes | Textract `DetectDocumentText` (OCR, sync ≤10MB), Pillow for EXIF | Transcribe `StartTranscriptionJob` (async, output → S3), poll via EventBridge | ffmpeg (Lambda layer) demux audio+frames, then Transcribe on audio track (async) |
| **Validate** | Length/encoding check | Rekognition `DetectModerationLabels` (optional), Pillow dimension/format check | ffprobe duration/sample-rate check | ffprobe codec/duration/resolution check |
| **Cleanup** | Whitespace/dedup | Perceptual hash dedup (DynamoDB-backed), Pillow format normalize | ffmpeg loudnorm + silence trim | ffmpeg keyframe near-dup pruning |
| **PII Redact** | Regex/Comprehend PII | Rekognition `DetectFaces` → Pillow/OpenCV blur; EXIF strip | Comprehend `DetectPiiEntities` on transcript text | Rekognition on keyframes + Comprehend on transcript |
| **Chunk** | Window/paragraph split | 1 image = 1 chunk, or tile grid | Fixed time-window (30s/5s overlap) | Scene-based (ffmpeg scene filter) |
| **Embed** | Bedrock Titan Text Embeddings | Bedrock Titan Multimodal Embeddings | Bedrock Titan Text Embeddings (on transcript slice) | Titan Text (transcript) + Titan Multimodal (keyframe), dual-vector |
| **Index** | S3 Vectors `PutVectors` | S3 Vectors `PutVectors` | S3 Vectors `PutVectors` | S3 Vectors `PutVectors` (x2 per chunk if dual-vector) |

**Why Comprehend for PII on transcripts, not just regex:** `Comprehend DetectPiiEntities` catches names/addresses/PII that regex misses in free-form speech transcripts — regex stays as a fast first pass, Comprehend as the authoritative pass for audio/video transcript text specifically (config-gated, since it's a per-call cost).

---

## 4. Repo Structure Mapping (unchanged files, extended responsibilities)

| File | AWS-Specific Addition |
|---|---|
| `config.py` | AWS region, Transcribe/Textract/Rekognition/Bedrock/Comprehend client config, S3 Vectors bucket/index name, DynamoDB table names for manifest + dedup cache |
| `parsers.py` | `ImageParser` → boto3 `textract` + `rekognition`; `AudioParser` → boto3 `transcribe` (async job lifecycle); `VideoParser` → ffmpeg (Lambda layer) + delegates audio to `AudioParser` |
| `validators.py` | Add Rekognition moderation check (image), ffprobe-based codec/duration checks (audio/video) |
| `cleanup.py` | DynamoDB-backed perceptual-hash dedup table (cross-invocation persistent, unlike in-memory set) |
| `privacy.py` | Rekognition `DetectFaces` + OpenCV blur; Comprehend `DetectPiiEntities` for transcript redaction |
| `chunker.py` | Unchanged logic, no AWS calls — pure Python |
| `metadata.py` | Unchanged logic, no AWS calls — pure Python |
| `index_manager.py` | Bedrock `InvokeModel` for embeddings; S3 Vectors `PutVectors`; S3 `PutObject` for processed media/manifest |
| `pipeline.py` | Becomes the **Step Functions state machine definition** (ASL JSON) + a thin Lambda handler per state, instead of one long-running Python script — required because Transcribe jobs are async and can take minutes |
| `delete_vectors_manifest.py` | Reused as-is for reprocessing/deletion; extended to filter by `modality` field when bulk-deleting |
| `evaluate.py` | Add Textract OCR-confidence sampling, Transcribe confidence-score sampling, Rekognition face-detection recall spot-check |
| `test_credentials.py` | Extended to verify IAM permissions for Textract/Rekognition/Transcribe/Bedrock/S3 Vectors, not just S3 |
| **New:** `media_utils.py` | ffmpeg/ffprobe wrappers, perceptual hashing — pure Python + Lambda ffmpeg layer, no AWS SDK calls |
| **New:** `embeddings.py` | Bedrock `InvokeModel` wrapper for Titan Text / Titan Multimodal, isolates model-ID config from `index_manager.py` |

---

## 5. Handling Async AWS Calls (critical design point)

Transcribe (and large Rekognition video jobs, if used) are **asynchronous** — this is the one place the design must diverge from a simple synchronous Lambda chain:

**Pattern: Step Functions `waitForTaskToken` + EventBridge**

1. Lambda calls `transcribe.start_transcription_job(...)`, stores the Step Functions **task token** alongside the job name in DynamoDB, then returns without waiting.
2. An EventBridge rule listens for `"Transcribe Job State Change"` events (`COMPLETED` / `FAILED`).
3. A small Lambda (`transcribe_callback_handler`) receives the event, looks up the task token in DynamoDB by job name, and calls `SendTaskSuccess` / `SendTaskFailure` to resume the Step Functions execution.
4. The workflow proceeds to Cleanup with the transcript now available in S3.

This avoids Lambda timeout limits (max 15 min) blocking on long transcriptions, and avoids costly polling loops.

```json
// pipeline.py — expressed as Step Functions ASL (excerpt)
{
  "StartAt": "RouteByModality",
  "States": {
    "RouteByModality": {
      "Type": "Choice",
      "Choices": [
        {"Variable": "$.modality", "StringEquals": "audio", "Next": "ParseAudio"},
        {"Variable": "$.modality", "StringEquals": "video", "Next": "ParseVideo"},
        {"Variable": "$.modality", "StringEquals": "image", "Next": "ParseImage"}
      ],
      "Default": "ParseText"
    },
    "ParseAudio": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke.waitForTaskToken",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:REGION:ACCOUNT:function:start_transcribe_job",
        "Payload": {"TaskToken.$": "$$.Task.Token", "input.$": "$"}
      },
      "Next": "Validate"
    }
  }
}
```

---

## 6. IAM Permissions Required (least-privilege, per Lambda role)

| Lambda | Permissions |
|---|---|
| Parse (image) | `textract:DetectDocumentText`, `rekognition:DetectFaces`, `s3:GetObject` (source) |
| Parse (audio) | `transcribe:StartTranscriptionJob`, `transcribe:GetTranscriptionJob`, `s3:GetObject`/`PutObject` (source + transcribe-output prefix) |
| Parse (video) | Same as audio + `s3:PutObject` (extracted frames prefix) |
| PII Redact | `rekognition:DetectFaces`, `comprehend:DetectPiiEntities` |
| Embed/Index | `bedrock:InvokeModel` (Titan models), `s3vectors:PutVectors`, `s3vectors:GetIndex`, `s3:PutObject` (processed bucket) |
| Manifest/quarantine | `dynamodb:PutItem`/`UpdateItem` (manifest table), `s3:CopyObject` (quarantine bucket) |
| transcribe_callback_handler | `states:SendTaskSuccess`, `states:SendTaskFailure`, `dynamodb:GetItem` |

All Lambdas scoped to their specific bucket prefixes and specific Bedrock model ARNs — not `*` on any of the above actions.

---

## 7. DynamoDB Tables (new — replaces in-memory state from earlier draft)

**`rag-ingestion-manifest`** (replaces per-run in-memory manifest list)
```
PK: doc_id (S)
SK: s3_key (S)
Attributes: modality, status, reason, chunks_indexed, timestamp, transcribe_job_name (nullable)
```

**`rag-perceptual-hash-dedup`** (persistent cross-run image/keyframe dedup)
```
PK: perceptual_hash (S)
Attributes: doc_id, first_seen_s3_key, timestamp
TTL: optional, e.g. 90 days, if dedup only needs to be recent-window
```

---

## 8. Storage Layout (S3)

```
s3://<source-bucket>/raw/{text|image|audio|video}/...
s3://<processed-bucket>/processed/{modality}/{doc_id}/
      ├── redacted_original.*
      ├── keyframes/                     (video only)
      ├── transcribe_output/             (audio/video — raw Transcribe JSON)
      ├── transcript.json                (normalized, PII-redacted)
      └── chunks/{chunk_id}.json
s3://<quarantine-bucket>/quarantine/{modality}/...
s3://<vector-bucket>/index/<index_name>/  (single shared S3 Vectors index)
```

---

## 9. Metadata Schema (unchanged from framework design — additive per modality)

```json
{
  "doc_id": "uuid",
  "modality": "image | audio | video | text",
  "source_s3_key": "raw/video/clip123.mp4",
  "ingestion_timestamp": "iso8601",
  "chunk_id": "uuid",
  "pii_redacted": true,
  "image_meta": { "width": 0, "height": 0, "format": "", "ocr_text_present": false, "perceptual_hash": "" },
  "audio_meta": { "duration_sec": 0, "sample_rate": 0, "language": "", "transcribe_job_name": "" },
  "video_meta": { "duration_sec": 0, "fps": 0, "scene_count": 0, "scene_index": 0, "transcribe_job_name": "" }
}
```

---

## 10. Sequence — Single Video File (AWS calls explicit)

1. Upload lands in `s3://source/raw/video/clip.mp4` → S3 event → EventBridge → Step Functions execution starts.
2. **ParseVideo Lambda**: downloads object, runs ffmpeg (Lambda layer) to extract audio track + sample frames; uploads audio track to `processed/.../transcribe_output/` staging prefix.
3. **StartTranscribeJob Lambda** (`waitForTaskToken`): calls `transcribe.start_transcription_job`, writes task token to DynamoDB manifest, returns.
4. Transcribe completes → EventBridge `"Transcribe Job State Change"` → **transcribe_callback_handler Lambda** → `SendTaskSuccess` with the S3 transcript location.
5. **Validate Lambda**: ffprobe checks (duration/codec/resolution) — these ran earlier in step 2's output metadata, validated here.
6. **Cleanup Lambda**: scene-level keyframe de-dup via perceptual hash (DynamoDB lookup).
7. **PII Redact Lambda**: Rekognition `DetectFaces` on keyframes → blur; Comprehend `DetectPiiEntities` on transcript → redact.
8. **Chunk Lambda**: scene-based chunking, pure Python, no AWS calls.
9. **Embed+Index Lambda**: Bedrock `InvokeModel` (Titan Multimodal for keyframe, Titan Text for transcript slice) → `s3vectors.put_vectors` with `modality: "video"` metadata.
10. Manifest entry updated to `status: success` in DynamoDB.

Failure at any numbered step → object copied to `quarantine/video/`, manifest entry `status: failed` with `reason`, Step Functions execution ends in a `Fail` state (visible in Step Functions console for retry/audit).

---

## 11. Cost & Performance Notes

- **Textract/Rekognition sync calls** (image path) are cheap and fast (sub-second) — no async handling needed there.
- **Transcribe** is billed per second of audio — batch/dedupe before transcribing, not after, to avoid paying to transcribe near-duplicate clips.
- **Bedrock Titan Multimodal** embedding calls are the main per-chunk cost driver for image/video — tiling large images or over-sampling video frames directly multiplies this cost, so `VIDEO_FRAME_SAMPLE_FPS` and `IMAGE_TILE_THRESHOLD_PX` in `config.py` are the two levers to tune first.
- **Step Functions Standard workflow** (not Express) is required here because of the `waitForTaskToken` pattern and potential multi-minute Transcribe wait times.

---

## 12. Open Decisions Needing Your Confirmation

1. **Rekognition on video frames vs Rekognition Video API directly** — using `StartFaceDetection` (Rekognition Video, also async) on the whole video instead of per-keyframe `DetectFaces` would reduce API calls but adds another async branch to the state machine. Which do you prefer?
2. **Comprehend PII redaction** — enable for all transcripts by default, or config-gated per data-sensitivity classification of the source bucket?
3. **S3 Vectors dual-vector vs fused** for video chunks — confirmed still open from the earlier draft; affects `embeddings.py` and query-time retrieval logic.
4. **ffmpeg Lambda layer** — do you already have one, or should the implementation include a Dockerfile-based Lambda (container image) instead of a zip layer, since ffmpeg binaries are sizable?

Once these four are settled, I can convert this into the actual Step Functions ASL definition file + the per-stage Lambda handlers (reusing the `pipeline.py`/`parsers.py`/etc. code already provided, restructured as Lambda entry points).
