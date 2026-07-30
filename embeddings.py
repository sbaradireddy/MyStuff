"""
embeddings.py
Thin abstraction over modality-specific embedding calls so index_manager.py
doesn't need model-specific branching logic inline. Uses Amazon Bedrock by
default (Titan text + Titan multimodal); swap _invoke_* methods for your
preferred provider.
"""

import base64
import json
import logging
from typing import List, Optional

import boto3

import config
from chunker import Chunk

logger = logging.getLogger(__name__)

_bedrock_runtime = boto3.client("bedrock-runtime", region_name=config.BEDROCK_REGION)


class EmbeddingRouter:
    """
    Modality -> embedding strategy:
      text          -> text embedding model, embed chunk.text
      image         -> multimodal embedding model, embed chunk.media_path (+ optional text)
      audio / video -> transcript slice embedded with the TEXT model (keeps the
                        vector space unified across modalities for cross-modal
                        search). If EMBEDDING_STRATEGY == "dual_vector" and the
                        chunk has an associated keyframe (video), also compute
                        an image embedding and return both.
    """

    def embed_chunk(self, chunk: Chunk) -> dict:
        """Returns {'vector': [...], 'aux_vector': [...] or None}"""
        if chunk.modality == "text":
            return {"vector": self._embed_text(chunk.text), "aux_vector": None}

        if chunk.modality == "image":
            return {"vector": self._embed_image(chunk.media_path, chunk.text), "aux_vector": None}

        if chunk.modality == "audio":
            return {"vector": self._embed_text(chunk.text), "aux_vector": None}

        if chunk.modality == "video":
            text_vector = self._embed_text(chunk.text)
            if config.EMBEDDING_STRATEGY == "dual_vector" and chunk.media_path:
                image_vector = self._embed_image(chunk.media_path, "")
                return {"vector": text_vector, "aux_vector": image_vector}
            if config.EMBEDDING_STRATEGY == "single_fused_vector" and chunk.media_path:
                image_vector = self._embed_image(chunk.media_path, "")
                return {"vector": self._fuse(text_vector, image_vector), "aux_vector": None}
            return {"vector": text_vector, "aux_vector": None}

        raise ValueError(f"No embedding strategy for modality '{chunk.modality}'")

    # -----------------------------------------------------------------
    def _embed_text(self, text: str) -> List[float]:
        if not text:
            return []
        try:
            body = json.dumps({"inputText": text})
            response = _bedrock_runtime.invoke_model(
                modelId=config.TEXT_EMBEDDING_MODEL_ID,
                body=body,
                contentType="application/json",
                accept="application/json",
            )
            payload = json.loads(response["body"].read())
            return payload.get("embedding", [])
        except Exception as e:
            logger.warning("Text embedding failed: %s", e)
            return []

    def _embed_image(self, image_path: Optional[str], caption: str) -> List[float]:
        if not image_path:
            return []
        try:
            with open(image_path, "rb") as f:
                image_b64 = base64.b64encode(f.read()).decode("utf-8")
            request_body = {"inputImage": image_b64}
            if caption:
                request_body["inputText"] = caption
            body = json.dumps(request_body)
            response = _bedrock_runtime.invoke_model(
                modelId=config.IMAGE_EMBEDDING_MODEL_ID,
                body=body,
                contentType="application/json",
                accept="application/json",
            )
            payload = json.loads(response["body"].read())
            return payload.get("embedding", [])
        except Exception as e:
            logger.warning("Image embedding failed for %s: %s", image_path, e)
            return []

    @staticmethod
    def _fuse(vec_a: List[float], vec_b: List[float]) -> List[float]:
        if not vec_a:
            return vec_b
        if not vec_b:
            return vec_a
        if len(vec_a) != len(vec_b):
            logger.warning("Vector dims mismatch (%d vs %d); returning text vector only",
                            len(vec_a), len(vec_b))
            return vec_a
        return [(a + b) / 2.0 for a, b in zip(vec_a, vec_b)]
