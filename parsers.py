"""
parsers.py
Modality-aware parsers. TextParser wraps your existing text-extraction logic
(kept as a stub here — plug in your current implementation).
ImageParser / AudioParser / VideoParser are new for the multi-modal extension.
"""

import logging
import os
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import boto3

import config
import media_utils

logger = logging.getLogger(__name__)
s3_client = boto3.client("s3", region_name=config.AWS_REGION)


@dataclass
class ParsedDocument:
    """Common output contract for every parser, regardless of modality."""
    doc_id: str
    source_s3_key: str
    modality: str
    content_text: str = ""                 # OCR text / transcript / body text
    local_media_path: Optional[str] = None  # local working copy of the raw media
    segments: List[Dict[str, Any]] = field(default_factory=list)  # transcript segments, scenes, tiles
    extra: Dict[str, Any] = field(default_factory=dict)  # modality-specific raw fields


class BaseParser(ABC):
    @abstractmethod
    def parse(self, s3_key: str, doc_id: str) -> ParsedDocument:
        ...

    @staticmethod
    def _download_to_tmp(s3_key: str) -> str:
        suffix = os.path.splitext(s3_key)[1]
        fd, local_path = tempfile.mkstemp(suffix=suffix, prefix="rag_src_")
        os.close(fd)
        s3_client.download_file(config.SOURCE_BUCKET, s3_key, local_path)
        return local_path


# ---------------------------------------------------------------------------
# TEXT (existing behavior — plug in your current parser here)
# ---------------------------------------------------------------------------
class TextParser(BaseParser):
    def parse(self, s3_key: str, doc_id: str) -> ParsedDocument:
        local_path = self._download_to_tmp(s3_key)
        with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
        return ParsedDocument(
            doc_id=doc_id,
            source_s3_key=s3_key,
            modality="text",
            content_text=text,
            local_media_path=local_path,
        )


# ---------------------------------------------------------------------------
# IMAGE
# ---------------------------------------------------------------------------
class ImageParser(BaseParser):
    """
    - Loads the image
    - Extracts EXIF metadata
    - Runs OCR to pull embedded text (if enabled)
    - Optionally runs an image-captioning model for a semantic description
    """

    def parse(self, s3_key: str, doc_id: str) -> ParsedDocument:
        local_path = self._download_to_tmp(s3_key)

        exif_data = self._extract_exif(local_path)
        ocr_text = self._run_ocr(local_path) if config.IMAGE_ENABLE_OCR else ""
        caption = self._run_captioning(local_path) if config.IMAGE_ENABLE_CAPTIONING else ""

        width, height, fmt = self._image_dims(local_path)

        content_text = "\n".join(t for t in [caption, ocr_text] if t)

        return ParsedDocument(
            doc_id=doc_id,
            source_s3_key=s3_key,
            modality="image",
            content_text=content_text,
            local_media_path=local_path,
            extra={
                "width": width,
                "height": height,
                "format": fmt,
                "exif": exif_data,
                "ocr_text": ocr_text,
                "caption": caption,
            },
        )

    @staticmethod
    def _image_dims(local_path: str):
        try:
            from PIL import Image
            with Image.open(local_path) as img:
                return img.width, img.height, img.format
        except Exception as e:
            logger.warning("Could not read image dimensions for %s: %s", local_path, e)
            return 0, 0, "UNKNOWN"

    @staticmethod
    def _extract_exif(local_path: str) -> dict:
        try:
            from PIL import Image
            from PIL.ExifTags import TAGS
            with Image.open(local_path) as img:
                raw_exif = img.getexif()
                if not raw_exif:
                    return {}
                return {TAGS.get(k, k): str(v) for k, v in raw_exif.items()}
        except Exception as e:
            logger.warning("EXIF extraction failed for %s: %s", local_path, e)
            return {}

    @staticmethod
    def _run_ocr(local_path: str) -> str:
        """Pluggable OCR: routes to AWS Textract or local Tesseract based on config."""
        if config.OCR_ENGINE == "aws_textract":
            return ImageParser._ocr_textract(local_path)
        return ImageParser._ocr_tesseract(local_path)

    @staticmethod
    def _ocr_textract(local_path: str) -> str:
        try:
            textract = boto3.client("textract", region_name=config.AWS_REGION)
            with open(local_path, "rb") as f:
                image_bytes = f.read()
            response = textract.detect_document_text(Document={"Bytes": image_bytes})
            lines = [b["Text"] for b in response.get("Blocks", []) if b["BlockType"] == "LINE"]
            return "\n".join(lines)
        except Exception as e:
            logger.warning("Textract OCR failed for %s: %s", local_path, e)
            return ""

    @staticmethod
    def _ocr_tesseract(local_path: str) -> str:
        try:
            import pytesseract
            from PIL import Image
            with Image.open(local_path) as img:
                return pytesseract.image_to_string(img)
        except ImportError:
            logger.warning("pytesseract not installed; OCR skipped for %s", local_path)
            return ""
        except Exception as e:
            logger.warning("Tesseract OCR failed for %s: %s", local_path, e)
            return ""

    @staticmethod
    def _run_captioning(local_path: str) -> str:
        """Placeholder hook for an image-captioning model call (e.g. Bedrock
        multimodal model). Wire up your actual model client here."""
        logger.info("Captioning stub called for %s (implement model call)", local_path)
        return ""


# ---------------------------------------------------------------------------
# AUDIO
# ---------------------------------------------------------------------------
class AudioParser(BaseParser):
    """
    - Probes duration/sample-rate/channels
    - Runs ASR (Amazon Transcribe or Whisper) to produce a timestamped transcript
    - Optionally runs speaker diarization
    """

    def parse(self, s3_key: str, doc_id: str) -> ParsedDocument:
        local_path = self._download_to_tmp(s3_key)
        probe = media_utils.probe_audio(local_path)

        transcript_segments = self._transcribe(local_path)
        full_text = " ".join(seg["text"] for seg in transcript_segments)

        return ParsedDocument(
            doc_id=doc_id,
            source_s3_key=s3_key,
            modality="audio",
            content_text=full_text,
            local_media_path=local_path,
            segments=transcript_segments,
            extra={
                "duration_sec": probe.duration_sec,
                "sample_rate": probe.sample_rate,
                "channels": probe.channels,
                "codec": probe.codec,
            },
        )

    @staticmethod
    def _transcribe(local_path: str) -> List[Dict[str, Any]]:
        """Pluggable ASR. Returns list of {start, end, text, speaker} segments.
        Wire up Amazon Transcribe (batch job + S3 output) or a local Whisper
        model here. Stubbed with an empty transcript if no engine is wired up.
        """
        if config.ASR_ENGINE == "whisper":
            return AudioParser._transcribe_whisper(local_path)
        return AudioParser._transcribe_aws(local_path)

    @staticmethod
    def _transcribe_aws(local_path: str) -> List[Dict[str, Any]]:
        logger.info(
            "AWS Transcribe stub called for %s — wire up a batch Transcribe job "
            "(start_transcription_job -> poll -> parse output JSON) here.",
            local_path,
        )
        return []

    @staticmethod
    def _transcribe_whisper(local_path: str) -> List[Dict[str, Any]]:
        try:
            import whisper
            model = whisper.load_model("base")
            result = model.transcribe(local_path, word_timestamps=False)
            segments = [
                {
                    "start": seg["start"],
                    "end": seg["end"],
                    "text": seg["text"].strip(),
                    "speaker": None,
                }
                for seg in result.get("segments", [])
            ]
            return segments
        except ImportError:
            logger.warning("openai-whisper not installed; transcription skipped for %s", local_path)
            return []
        except Exception as e:
            logger.warning("Whisper transcription failed for %s: %s", local_path, e)
            return []


# ---------------------------------------------------------------------------
# VIDEO
# ---------------------------------------------------------------------------
class VideoParser(BaseParser):
    """
    - Demuxes audio track -> delegates to AudioParser for transcript
    - Samples frames / detects scenes -> keyframes
    """

    def parse(self, s3_key: str, doc_id: str) -> ParsedDocument:
        local_path = self._download_to_tmp(s3_key)
        probe = media_utils.probe_video(local_path)

        transcript_segments: List[Dict[str, Any]] = []
        full_text = ""
        if probe.has_audio:
            audio_path = media_utils.extract_audio_track(local_path)
            # Re-use AudioParser's transcription step directly on the extracted track:
            transcript_segments = AudioParser._transcribe(audio_path)
            full_text = " ".join(seg["text"] for seg in transcript_segments)

        scenes = media_utils.detect_scenes(local_path)
        frame_dir = tempfile.mkdtemp(prefix="rag_frames_")
        keyframes = self._extract_keyframes(local_path, scenes, frame_dir)

        return ParsedDocument(
            doc_id=doc_id,
            source_s3_key=s3_key,
            modality="video",
            content_text=full_text,
            local_media_path=local_path,
            segments=[
                {
                    "scene_index": s.index,
                    "start_sec": s.start_sec,
                    "end_sec": s.end_sec,
                    "keyframe_path": keyframes.get(s.index),
                }
                for s in scenes
            ],
            extra={
                "duration_sec": probe.duration_sec,
                "width": probe.width,
                "height": probe.height,
                "fps": probe.fps,
                "codec": probe.codec,
                "scene_count": len(scenes),
                "transcript_segments": transcript_segments,
            },
        )

    @staticmethod
    def _extract_keyframes(video_path: str, scenes, output_dir: str) -> Dict[int, str]:
        """Extract one representative frame per scene (midpoint timestamp)."""
        keyframes = {}
        for scene in scenes:
            midpoint = (scene.start_sec + scene.end_sec) / 2.0
            frame_path = os.path.join(output_dir, f"scene_{scene.index:04d}.jpg")
            cmd = [
                config.FFMPEG_BINARY, "-y",
                "-ss", str(midpoint),
                "-i", video_path,
                "-frames:v", "1",
                "-qscale:v", "2",
                frame_path,
            ]
            import subprocess
            try:
                subprocess.run(cmd, capture_output=True, check=True, timeout=60)
                keyframes[scene.index] = frame_path
            except Exception as e:
                logger.warning("Keyframe extraction failed for scene %s: %s", scene.index, e)
        return keyframes


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------
_PARSER_REGISTRY = {
    "text": TextParser,
    "image": ImageParser,
    "audio": AudioParser,
    "video": VideoParser,
}


def get_parser(modality: str) -> BaseParser:
    parser_cls = _PARSER_REGISTRY.get(modality)
    if parser_cls is None:
        raise ValueError(f"No parser registered for modality '{modality}'")
    return parser_cls()
