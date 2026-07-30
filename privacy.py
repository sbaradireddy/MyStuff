"""
privacy.py
PII / privacy redaction. Existing text-regex PIIRedactor is reused as-is on
any transcript text (audio/video). New for this extension: FaceRedactor and
EXIFStripper for image/video frames.
"""

import logging
import os
import re
from typing import List

import boto3

import config
from parsers import ParsedDocument

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Existing text PII redaction (kept — reused across all modalities' text)
# ---------------------------------------------------------------------------
_PII_PATTERNS = {
    "email": re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"),
    "phone": re.compile(r"\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"),
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "credit_card": re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
}


class PIIRedactor:
    @staticmethod
    def redact_text(text: str) -> str:
        if not text:
            return text
        redacted = text
        for label, pattern in _PII_PATTERNS.items():
            redacted = pattern.sub(f"[REDACTED_{label.upper()}]", redacted)
        return redacted


# ---------------------------------------------------------------------------
# NEW: Face redaction for images / video keyframes
# ---------------------------------------------------------------------------
class FaceRedactor:
    """Detects faces and blurs them in-place before the media is persisted."""

    @staticmethod
    def redact(image_path: str) -> str:
        if config.FACE_DETECTION_ENGINE == "aws_rekognition":
            boxes = FaceRedactor._detect_faces_rekognition(image_path)
        else:
            boxes = FaceRedactor._detect_faces_opencv(image_path)

        if not boxes:
            return image_path

        return FaceRedactor._blur_regions(image_path, boxes)

    @staticmethod
    def _detect_faces_rekognition(image_path: str) -> List[dict]:
        try:
            rekognition = boto3.client("rekognition", region_name=config.AWS_REGION)
            with open(image_path, "rb") as f:
                image_bytes = f.read()
            response = rekognition.detect_faces(Image={"Bytes": image_bytes})
            boxes = []
            for face in response.get("FaceDetails", []):
                bb = face["BoundingBox"]
                boxes.append(bb)  # normalized Left/Top/Width/Height (0-1)
            return boxes
        except Exception as e:
            logger.warning("Rekognition face detection failed for %s: %s", image_path, e)
            return []

    @staticmethod
    def _detect_faces_opencv(image_path: str) -> List[dict]:
        try:
            import cv2
            cascade_path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
            face_cascade = cv2.CascadeClassifier(cascade_path)
            img = cv2.imread(image_path)
            if img is None:
                return []
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5)
            h, w = img.shape[:2]
            boxes = [
                {"Left": x / w, "Top": y / h, "Width": fw / w, "Height": fh / h}
                for (x, y, fw, fh) in faces
            ]
            return boxes
        except ImportError:
            logger.warning("opencv-python not installed; face detection skipped for %s", image_path)
            return []
        except Exception as e:
            logger.warning("OpenCV face detection failed for %s: %s", image_path, e)
            return []

    @staticmethod
    def _blur_regions(image_path: str, boxes: List[dict]) -> str:
        try:
            import cv2
            img = cv2.imread(image_path)
            if img is None:
                return image_path
            h, w = img.shape[:2]
            k = config.FACE_BLUR_KERNEL if config.FACE_BLUR_KERNEL % 2 == 1 else config.FACE_BLUR_KERNEL + 1

            for box in boxes:
                x = int(box["Left"] * w)
                y = int(box["Top"] * h)
                bw = int(box["Width"] * w)
                bh = int(box["Height"] * h)
                x, y = max(0, x), max(0, y)
                roi = img[y:y + bh, x:x + bw]
                if roi.size == 0:
                    continue
                blurred = cv2.GaussianBlur(roi, (k, k), 0)
                img[y:y + bh, x:x + bw] = blurred

            base, ext = os.path.splitext(image_path)
            redacted_path = f"{base}_redacted{ext}"
            cv2.imwrite(redacted_path, img)
            return redacted_path
        except ImportError:
            logger.warning("opencv-python not installed; cannot blur faces for %s", image_path)
            return image_path
        except Exception as e:
            logger.warning("Face blurring failed for %s: %s", image_path, e)
            return image_path


# ---------------------------------------------------------------------------
# NEW: EXIF / metadata stripping
# ---------------------------------------------------------------------------
class EXIFStripper:
    @staticmethod
    def strip(image_path: str) -> str:
        try:
            from PIL import Image
            with Image.open(image_path) as img:
                data = list(img.getdata())
                stripped = Image.new(img.mode, img.size)
                stripped.putdata(data)
                base, ext = os.path.splitext(image_path)
                out_path = f"{base}_noexif{ext}"
                stripped.save(out_path)
                return out_path
        except Exception as e:
            logger.warning("EXIF stripping failed for %s: %s", image_path, e)
            return image_path


# ---------------------------------------------------------------------------
# Orchestration entry point
# ---------------------------------------------------------------------------
def apply_privacy_controls(doc: ParsedDocument) -> ParsedDocument:
    if config.ENABLE_PII_TEXT_REDACTION and doc.content_text:
        doc.content_text = PIIRedactor.redact_text(doc.content_text)
        for seg in doc.segments:
            if "text" in seg:
                seg["text"] = PIIRedactor.redact_text(seg["text"])

    if doc.modality == "image" and doc.local_media_path:
        if config.ENABLE_FACE_REDACTION:
            doc.local_media_path = FaceRedactor.redact(doc.local_media_path)
        if config.ENABLE_EXIF_STRIP:
            doc.local_media_path = EXIFStripper.strip(doc.local_media_path)
            doc.extra["exif_stripped"] = True

    if doc.modality == "video" and config.ENABLE_FACE_REDACTION:
        for seg in doc.segments:
            frame_path = seg.get("keyframe_path")
            if frame_path and os.path.exists(frame_path):
                seg["keyframe_path"] = FaceRedactor.redact(frame_path)

    return doc
