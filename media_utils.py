"""
media_utils.py
Shared low-level helpers for image/audio/video processing:
frame extraction, audio demuxing, perceptual hashing, ffprobe inspection.
Used by parsers.py, cleanup.py, and chunker.py.
"""

import json
import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from typing import List, Optional

import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ffprobe / ffmpeg wrappers
# ---------------------------------------------------------------------------
def run_ffprobe(file_path: str) -> dict:
    """Return ffprobe JSON metadata for a media file (audio or video)."""
    cmd = [
        config.FFPROBE_BINARY,
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        file_path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        raise MediaProbeError(f"ffprobe failed for {file_path}: {e.stderr}") from e
    except subprocess.TimeoutExpired as e:
        raise MediaProbeError(f"ffprobe timed out for {file_path}") from e
    except json.JSONDecodeError as e:
        raise MediaProbeError(f"ffprobe returned invalid JSON for {file_path}") from e


class MediaProbeError(Exception):
    """Raised when ffprobe cannot read/parse a media file (likely corrupt)."""


@dataclass
class VideoProbeResult:
    duration_sec: float
    width: int
    height: int
    fps: float
    codec: str
    has_audio: bool


def probe_video(file_path: str) -> VideoProbeResult:
    data = run_ffprobe(file_path)
    fmt = data.get("format", {})
    streams = data.get("streams", [])

    video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
    audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)

    if video_stream is None:
        raise MediaProbeError(f"No video stream found in {file_path}")

    duration = float(fmt.get("duration", video_stream.get("duration", 0)) or 0)
    width = int(video_stream.get("width", 0))
    height = int(video_stream.get("height", 0))
    codec = video_stream.get("codec_name", "unknown")

    # frame rate comes as "30000/1001" style fraction
    fps_raw = video_stream.get("avg_frame_rate", "0/1")
    try:
        num, den = fps_raw.split("/")
        fps = float(num) / float(den) if float(den) != 0 else 0.0
    except (ValueError, ZeroDivisionError):
        fps = 0.0

    return VideoProbeResult(
        duration_sec=duration,
        width=width,
        height=height,
        fps=fps,
        codec=codec,
        has_audio=audio_stream is not None,
    )


@dataclass
class AudioProbeResult:
    duration_sec: float
    sample_rate: int
    channels: int
    codec: str


def probe_audio(file_path: str) -> AudioProbeResult:
    data = run_ffprobe(file_path)
    fmt = data.get("format", {})
    streams = data.get("streams", [])

    audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)
    if audio_stream is None:
        raise MediaProbeError(f"No audio stream found in {file_path}")

    duration = float(fmt.get("duration", audio_stream.get("duration", 0)) or 0)
    sample_rate = int(audio_stream.get("sample_rate", 0))
    channels = int(audio_stream.get("channels", 0))
    codec = audio_stream.get("codec_name", "unknown")

    return AudioProbeResult(
        duration_sec=duration,
        sample_rate=sample_rate,
        channels=channels,
        codec=codec,
    )


def extract_audio_track(video_path: str, output_dir: Optional[str] = None) -> str:
    """Extract the audio track from a video file as a mono 16kHz WAV
    (standard input format for most ASR engines). Returns output path."""
    output_dir = output_dir or tempfile.mkdtemp(prefix="rag_audio_")
    output_path = os.path.join(output_dir, "audio_track.wav")

    cmd = [
        config.FFMPEG_BINARY, "-y", "-i", video_path,
        "-vn", "-ac", "1", "-ar", "16000", "-acodec", "pcm_s16le",
        output_path,
    ]
    subprocess.run(cmd, capture_output=True, check=True, timeout=300)
    return output_path


def extract_frames(video_path: str, fps: Optional[float] = None,
                    output_dir: Optional[str] = None) -> List[str]:
    """Sample frames from a video at the configured fps. Returns list of frame file paths."""
    fps = fps or config.VIDEO_FRAME_SAMPLE_FPS
    output_dir = output_dir or tempfile.mkdtemp(prefix="rag_frames_")

    pattern = os.path.join(output_dir, "frame_%06d.jpg")
    cmd = [
        config.FFMPEG_BINARY, "-y", "-i", video_path,
        "-vf", f"fps={fps}",
        "-qscale:v", "2",
        pattern,
    ]
    subprocess.run(cmd, capture_output=True, check=True, timeout=600)

    frames = sorted(
        os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.startswith("frame_")
    )
    return frames


@dataclass
class Scene:
    index: int
    start_sec: float
    end_sec: float
    representative_frame: Optional[str] = None


def detect_scenes(video_path: str, threshold: Optional[float] = None) -> List[Scene]:
    """Detect scene changes using ffmpeg's scene-detection filter.
    Falls back to fixed-length pseudo-scenes if fewer than 2 scenes are found."""
    threshold = threshold if threshold is not None else config.VIDEO_SCENE_THRESHOLD / 100.0

    cmd = [
        config.FFMPEG_BINARY, "-i", video_path,
        "-filter:v", f"select='gt(scene,{threshold})',showinfo",
        "-f", "null", "-",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)

    timestamps = []
    for line in result.stderr.splitlines():
        if "pts_time:" in line:
            try:
                pts = line.split("pts_time:")[1].split(" ")[0]
                timestamps.append(float(pts))
            except (IndexError, ValueError):
                continue

    probe = probe_video(video_path)
    total_duration = probe.duration_sec

    boundaries = [0.0] + sorted(timestamps) + [total_duration]
    boundaries = sorted(set(boundaries))

    scenes = []
    min_len = config.VIDEO_MIN_SCENE_LEN_SEC
    idx = 0
    i = 0
    while i < len(boundaries) - 1:
        start = boundaries[i]
        end = boundaries[i + 1]
        # merge scenes shorter than min_len into the next one
        while end - start < min_len and i + 2 < len(boundaries):
            i += 1
            end = boundaries[i + 1]
        scenes.append(Scene(index=idx, start_sec=start, end_sec=end))
        idx += 1
        i += 1

    if len(scenes) < 2:
        # fallback: fixed windows
        scenes = _fixed_window_scenes(total_duration, config.AUDIO_CHUNK_WINDOW_SEC)

    return scenes


def _fixed_window_scenes(total_duration: float, window_sec: float) -> List[Scene]:
    scenes = []
    start = 0.0
    idx = 0
    while start < total_duration:
        end = min(start + window_sec, total_duration)
        scenes.append(Scene(index=idx, start_sec=start, end_sec=end))
        start = end
        idx += 1
    return scenes


# ---------------------------------------------------------------------------
# Perceptual hashing (image / keyframe dedup)
# ---------------------------------------------------------------------------
def perceptual_hash(image_path: str):
    """Compute a perceptual hash (average hash) for near-duplicate detection.
    Returns an imagehash.ImageHash object; falls back to None if libs missing."""
    try:
        import imagehash
        from PIL import Image
        with Image.open(image_path) as img:
            return imagehash.average_hash(img)
    except ImportError:
        logger.warning("imagehash/Pillow not installed; perceptual dedup disabled")
        return None


def hamming_distance(hash_a, hash_b) -> int:
    if hash_a is None or hash_b is None:
        return -1
    return hash_a - hash_b
