from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable, List, Optional, Sequence, Tuple

import numpy as np

# Heavy deps are optional: installed via extras [semantic]
try:
    import faiss  # type: ignore
    from sentence_transformers import SentenceTransformer  # type: ignore
except Exception:  # pragma: no cover
    faiss = None
    SentenceTransformer = None


@dataclass
class NearestItem:
    text: str
    score: float


class SemanticSimilarityMatcher:
    """
    Generic semantic similarity layer (optional).

    - similarity(a, b) -> cosine similarity in [0..1] (usually)
    - build_index(items) + find_nearest(query) for fast lookup via FAISS

    Notes:
    - Requires extras: sentence-transformers + faiss-cpu (+ numpy)
    - Uses L2-normalized embeddings + inner product => cosine similarity.
    """

    def __init__(
        self,
        model_name: str = "paraphrase-multilingual-MiniLM-L12-v2",
        threshold: float = 0.75,
        cache_size: int = 20000,
    ):
        if SentenceTransformer is None:
            raise ImportError(
                "SemanticSimilarityMatcher requires optional dependencies. "
                "Install with: pip install -e '.[semantic]'"
            )
        if threshold < 0.0 or threshold > 1.0:
            raise ValueError("threshold must be in [0, 1]")

        self.model = SentenceTransformer(model_name)
        self.threshold = float(threshold)

        # Optional FAISS index
        self._index = None
        self._indexed_items: List[str] = []

        # make cache size configurable (wrap method)
        self._encode_one_norm_cached = lru_cache(maxsize=cache_size)(self._encode_one_norm_uncached)

    # -----------------------
    # Embeddings
    # -----------------------
    def encode_normalized(self, texts: Sequence[str]) -> np.ndarray:
        """
        Returns L2-normalized embeddings (float32), shape [n, dim].
        """
        texts = [str(t or "").strip() for t in texts]
        emb = self.model.encode(texts, convert_to_numpy=True).astype("float32")

        norms = np.linalg.norm(emb, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1.0, norms)
        emb = emb / norms
        return emb

    def _encode_one_norm_uncached(self, text: str) -> Optional[np.ndarray]:
        text = (text or "").strip()
        if not text:
            return None
        return self.encode_normalized([text])[0]

    def similarity(self, a: str, b: str) -> float:
        """
        Cosine similarity via dot product of L2-normalized vectors.
        """
        va = self._encode_one_norm_cached((a or "").strip())
        vb = self._encode_one_norm_cached((b or "").strip())
        if va is None or vb is None:
            return 0.0
        return float(np.dot(va, vb.T).flatten()[0])

   