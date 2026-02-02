"""BM25 и RRF (Reciprocal Rank Fusion) для гибридного поиска."""

import re
import logging
from typing import List, Dict, Any, Tuple

logger = logging.getLogger(__name__)

RRF_K = 60


def _tokenize(text: str) -> List[str]:
    """Простая токенизация для русского и английского: слова длиной >= 2."""
    text = (text or "").lower().strip()
    tokens = re.findall(r"[a-zа-яё0-9]{2,}", text, re.IGNORECASE)
    return tokens or [""]


def bm25_rank(query: str, documents: List[str], top_n: int = 30) -> List[int]:
    """Ранжирует документы по BM25 относительно запроса. Возвращает индексы по убыванию score."""
    if not documents or not query.strip():
        return list(range(min(len(documents), top_n)))
    try:
        from rank_bm25 import BM25Okapi
    except ImportError:
        logger.warning("[BM25] rank_bm25 не установлен, возвращаем порядок по индексу")
        return list(range(min(len(documents), top_n)))
    tokenized_corpus = [_tokenize(d) for d in documents]
    tokenized_query = _tokenize(query)
    bm25 = BM25Okapi(tokenized_corpus)
    scores = bm25.get_scores(tokenized_query)
    indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)
    return indices[:top_n]


def rrf_merge(
    vector_chunks: List[Dict[str, Any]],
    vector_scores: List[float],
    bm25_indices: List[int],
    k: int = RRF_K,
) -> List[Dict[str, Any]]:
    """Объединяет векторный и BM25 ранжирования через RRF. vector_chunks — список чанков в порядке векторного поиска."""
    n = len(vector_chunks)
    if n == 0:
        return []
    rrf_scores = [0.0] * n
    for rank, _ in enumerate(vector_chunks):
        rrf_scores[rank] += 1.0 / (k + rank + 1)
    for rank, idx in enumerate(bm25_indices):
        if 0 <= idx < n:
            rrf_scores[idx] += 1.0 / (k + rank + 1)
    indexed = list(enumerate(rrf_scores))
    indexed.sort(key=lambda x: x[1], reverse=True)
    return [vector_chunks[i] for i, _ in indexed]


def hybrid_vector_bm25(
    query: str,
    chunks: List[Dict[str, Any]],
    top_n: int = 15,
) -> List[Dict[str, Any]]:
    """Гибрид: векторный порядок (chunks уже отсортированы по score) + BM25 по текстам, RRF merge. Возвращает top_n чанков."""
    if not chunks:
        return []
    texts = [chunk.get("text", "") for chunk in chunks]
    bm25_indices = bm25_rank(query, texts, top_n=len(chunks))
    merged = rrf_merge(chunks, [c.get("score", 0) for c in chunks], bm25_indices, k=RRF_K)
    return merged[:top_n]
