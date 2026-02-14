"""Поиск ответов в базе знаний help.kilbil.ru (kilbil RAG)."""

import asyncio
import logging
import re
from typing import Optional, Dict, Any, List

from app.services.openai_client import create_embedding
from app.services.qdrant_service import get_qdrant_service

logger = logging.getLogger(__name__)


def _normalize(text: str) -> str:
    """Нормализация текста для эмбеддингов."""
    text = (text or "").lower()
    text = re.sub(r"[^\w\sёЁа-яА-Я0-9]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


async def find_kilbil_answer(user_question: str) -> Optional[Dict[str, Any]]:
    """Ищет ответ в базе знаний kilbil (source=kilbil_help).

    Returns:
        Dict с ключами: answer, url, title — или None, если ничего не найдено.
    """
    try:
        qdrant_service = get_qdrant_service()
        norm_user = _normalize(user_question)
        user_emb = await asyncio.to_thread(create_embedding, norm_user)

        found_chunks = qdrant_service.search(
            query_embedding=user_emb,
            top_k=5,
            score_threshold=0.65,
            source_filter="kilbil_help",
        )

        if not found_chunks:
            return None

        best = found_chunks[0]
        metadata = best.get("metadata", {})
        return {
            "answer": best.get("text", ""),
            "url": metadata.get("article_url", ""),
            "title": metadata.get("document_title", ""),
            "score": best.get("score", 0),
        }
    except Exception as e:
        logger.exception(f"[KILBIL_SERVICE] Ошибка поиска: {e}")
        return None


def get_article_urls_from_chunks(chunks: List[Dict[str, Any]]) -> List[str]:
    """Собирает уникальные article_url из чанков с source=kilbil_help."""
    seen = set()
    urls = []
    for ch in chunks or []:
        meta = ch.get("metadata") or {}
        if meta.get("source") != "kilbil_help":
            continue
        url = (meta.get("article_url") or "").strip()
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
    return urls
