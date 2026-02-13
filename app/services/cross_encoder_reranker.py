"""Cross-encoder reranker: обёртка над Cohere Rerank API."""

import asyncio
import logging
from typing import List, Dict, Any

from app.config import COHERE_API_KEY, RERANK_TOP_K

logger = logging.getLogger(__name__)

COHERE_RERANK_URL = "https://api.cohere.ai/v1/rerank"
COHERE_RERANK_MODEL = "rerank-multilingual-v3.0"
MAX_DOCS_COHERE = 100
MAX_CHUNK_CHARS = 2000  # Cohere v3 supports ~4K tokens; avoid truncating long chunks


def _call_cohere_rerank_sync(
    query: str,
    documents: List[str],
    top_n: int,
    api_key: str,
) -> List[Dict[str, Any]]:
    """Синхронный вызов Cohere Rerank API. Возвращает список {index, relevance_score}."""
    import urllib.request
    import json

    body = {
        "model": COHERE_RERANK_MODEL,
        "query": query,
        "documents": documents,
        "top_n": min(top_n, len(documents)),
        "return_documents": False,
    }
    req = urllib.request.Request(
        COHERE_RERANK_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode())
    return data.get("results", [])


async def rerank_chunks_with_cohere(
    question: str,
    chunks: List[Dict[str, Any]],
    top_k: int = None,
) -> List[Dict[str, Any]]:
    """Переранжирует чанки через Cohere Rerank API.
    
    Args:
        question: Вопрос пользователя
        chunks: Список чанков с "text" и "score"
        top_k: Количество топ-чанков для возврата
    
    Returns:
        Отсортированный список чанков с обновлёнными scores (relevance_score от Cohere)
    """
    if not COHERE_API_KEY or not chunks:
        return chunks[: top_k or RERANK_TOP_K] if chunks else []
    top_k = top_k or RERANK_TOP_K
    documents = [chunk.get("text", "")[:MAX_CHUNK_CHARS] for chunk in chunks]
    try:
        results = await asyncio.to_thread(
            _call_cohere_rerank_sync,
            question,
            documents,
            min(top_k, len(chunks)),
            COHERE_API_KEY,
        )
        out = []
        for r in results:
            idx = r.get("index", 0)
            score = float(r.get("relevance_score", 0))
            if 0 <= idx < len(chunks):
                chunk = {**chunks[idx], "score": score}
                out.append(chunk)
        logger.info(
            f"[CROSS_ENCODER] Cohere rerank: {len(out)} чанков для вопроса '{question[:50]}...'"
        )
        return out
    except Exception as e:
        logger.exception(f"[CROSS_ENCODER] Ошибка Cohere Rerank: {e}")
        return sorted(chunks, key=lambda x: x.get("score", 0), reverse=True)[:top_k]
