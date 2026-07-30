"""
config.py
Central configuration for the RAG ingestion framework.
Extended to support text, image, audio, and video modalities.
All values can be overridden via environment variables.
"""

import os


def _env(key: str, default):
    val = os.environ.get(key)
    if val is None:
        return default
    if isinstance(default, bool):
        return val.lower() in ("1", "true", "yes")
    if isinstance(default, int):
        return int(val)
    if isinstance(default, float):
        return float(val)
    return val


# ---------------------------------------------------------------------------
# S3 buckets / prefixes
# ---------------------------------------------------------------------------
SOURCE_BUCKET = _env("RAG_SOURCE_BUCKET", "my-rag-source-bucket")
PROCESSED_BUCKET = _env("RAG_PROCESSED_BUCKET", "my-rag-processed-bucket")
QUARANTINE_BUCKET = _env("RAG_QUARANTINE_BUCKET", "my-rag-quarantine-bucket")
VECTOR_BUCKET = _env("RAG_VECTOR_BUCKET", "my-rag-vector-bucket")
VECTOR_INDEX_NAME = _env("RAG_VECTOR_INDEX", "rag-multimodal-index")

RAW_PREFIX = {
    "text": "raw/text/",
    "image": "raw/image/",
    "audio": "raw/audio/",
    "video": "raw/video/",
}
PROCESSED_PREFIX = {
    "text": "processed/text/",
    "image": "processed/image/",
    "audio": "processed/audio/",
    "video": "processed/video/",
}
QUARANTINE_PREFIX = {
    "text": "quarantine/text/",
    "image": "quarantine/image/",
    "audio": "quarantine/audio/",
    "video": "quarantine/video/",
}

# ---------------------------------------------------------------------------
# Modality routing
# ---------------------------------------------------------------------------
SUPPORTED_MODALITIES = ["text", "image", "audio", "video"]

EXTENSION_MODALITY_MAP = {
    # text
    ".txt": "text", ".md": "text", ".pdf": "text", ".docx": "text",
    ".html": "text", ".csv": "text", ".json": "text",
    # image
    ".jpg": "image", ".jpeg": "image", ".png": "image",
    ".tiff": "image", ".tif": "image", ".webp": "image", ".bmp": "image",
    # audio
    ".mp3": "audio", ".wav": "audio", ".flac": "audio",
    ".m4a": "audio", ".aac": "audio", ".ogg": "audio",
    # video
    ".mp4": "video", ".mov": "video", ".avi": "video",
    ".mkv": "video", ".webm": "video",
}

# ---------------------------------------------------------------------------
# Image settings
# ---------------------------------------------------------------------------
IMAGE_MAX_SIZE_MB = _env("IMAGE_MAX_SIZE_MB", 25)
IMAGE_MIN_WIDTH_PX = _env("IMAGE_MIN_WIDTH_PX", 32)
IMAGE_MIN_HEIGHT_PX = _env("IMAGE_MIN_HEIGHT_PX", 32)
IMAGE_TILE_THRESHOLD_PX = _env("IMAGE_TILE_THRESHOLD_PX", 4096)
IMAGE_TILE_SIZE_PX = _env("IMAGE_TILE_SIZE_PX", 2048)
IMAGE_ALLOWED_FORMATS = ["JPEG", "PNG", "TIFF", "WEBP", "BMP"]
IMAGE_ENABLE_CAPTIONING = _env("IMAGE_ENABLE_CAPTIONING", False)
IMAGE_ENABLE_OCR = _env("IMAGE_ENABLE_OCR", True)
IMAGE_DEDUP_HASH_DISTANCE = _env("IMAGE_DEDUP_HASH_DISTANCE", 5)  # perceptual hash hamming distance

# ---------------------------------------------------------------------------
# Audio settings
# ---------------------------------------------------------------------------
AUDIO_MAX_DURATION_SEC = _env("AUDIO_MAX_DURATION_SEC", 3600)
AUDIO_MIN_DURATION_SEC = _env("AUDIO_MIN_DURATION_SEC", 0.5)
AUDIO_MIN_SAMPLE_RATE = _env("AUDIO_MIN_SAMPLE_RATE", 8000)
AUDIO_CHUNK_WINDOW_SEC = _env("AUDIO_CHUNK_WINDOW_SEC", 30)
AUDIO_CHUNK_OVERLAP_SEC = _env("AUDIO_CHUNK_OVERLAP_SEC", 5)
AUDIO_ALLOWED_FORMATS = ["mp3", "wav", "flac", "m4a", "aac", "ogg"]
AUDIO_SILENCE_THRESHOLD_DB = _env("AUDIO_SILENCE_THRESHOLD_DB", -40)
AUDIO_MAX_SILENCE_RATIO = _env("AUDIO_MAX_SILENCE_RATIO", 0.95)
AUDIO_ENABLE_DIARIZATION = _env("AUDIO_ENABLE_DIARIZATION", True)

# ---------------------------------------------------------------------------
# Video settings
# ---------------------------------------------------------------------------
VIDEO_MAX_DURATION_SEC = _env("VIDEO_MAX_DURATION_SEC", 7200)
VIDEO_FRAME_SAMPLE_FPS = _env("VIDEO_FRAME_SAMPLE_FPS", 1)
VIDEO_ALLOWED_CODECS = ["h264", "hevc", "vp9", "vp8"]
VIDEO_MAX_RESOLUTION_PX = _env("VIDEO_MAX_RESOLUTION_PX", 3840)  # 4K ceiling
VIDEO_SCENE_THRESHOLD = _env("VIDEO_SCENE_THRESHOLD", 30.0)  # ffmpeg scene score 0-100
VIDEO_MIN_SCENE_LEN_SEC = _env("VIDEO_MIN_SCENE_LEN_SEC", 2.0)
VIDEO_ENABLE_FRAME_CAPTIONING = _env("VIDEO_ENABLE_FRAME_CAPTIONING", False)

# ---------------------------------------------------------------------------
# Privacy / redaction
# ---------------------------------------------------------------------------
ENABLE_FACE_REDACTION = _env("ENABLE_FACE_REDACTION", True)
ENABLE_EXIF_STRIP = _env("ENABLE_EXIF_STRIP", True)
ENABLE_PII_TEXT_REDACTION = _env("ENABLE_PII_TEXT_REDACTION", True)
FACE_BLUR_KERNEL = _env("FACE_BLUR_KERNEL", 35)  # must be odd

# ---------------------------------------------------------------------------
# Model endpoints / engines (pluggable — set to your actual AWS resources)
# ---------------------------------------------------------------------------
ASR_ENGINE = _env("ASR_ENGINE", "aws_transcribe")        # aws_transcribe | whisper
OCR_ENGINE = _env("OCR_ENGINE", "aws_textract")          # aws_textract | tesseract
FACE_DETECTION_ENGINE = _env("FACE_DETECTION_ENGINE", "aws_rekognition")  # aws_rekognition | opencv
TEXT_EMBEDDING_MODEL_ID = _env("TEXT_EMBEDDING_MODEL_ID", "amazon.titan-embed-text-v2:0")
IMAGE_EMBEDDING_MODEL_ID = _env("IMAGE_EMBEDDING_MODEL_ID", "amazon.titan-embed-image-v1")
BEDROCK_REGION = _env("BEDROCK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# Embedding fusion strategy for audio/video
# ---------------------------------------------------------------------------
# "dual_vector"        -> store separate vectors (e.g. keyframe + transcript) per chunk
# "single_fused_vector" -> average/concat into one vector per chunk
EMBEDDING_STRATEGY = _env("EMBEDDING_STRATEGY", "dual_vector")

# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------
AWS_REGION = _env("AWS_REGION", "us-east-1")
LOG_LEVEL = _env("LOG_LEVEL", "INFO")
MAX_WORKERS = _env("MAX_WORKERS", 4)
FFMPEG_BINARY = _env("FFMPEG_BINARY", "ffmpeg")
FFPROBE_BINARY = _env("FFPROBE_BINARY", "ffprobe")


def modality_for_key(s3_key: str) -> str:
    """Return the modality ('text'|'image'|'audio'|'video') for a given S3 key,
    based on its file extension. Raises ValueError if unsupported."""
    ext = os.path.splitext(s3_key)[1].lower()
    modality = EXTENSION_MODALITY_MAP.get(ext)
    if modality is None:
        raise ValueError(f"Unsupported file extension '{ext}' for key: {s3_key}")
    return modality
