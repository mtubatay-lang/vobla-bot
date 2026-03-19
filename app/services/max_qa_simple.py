"""
Упрощённый одношаговый RAG для MAX (без FSM Telegram).
"""

from __future__ import annotations

import asyncio
import logging

from app.handlers.qa_mode import _expand_query_for_search, _generate_answer_from_chunks_private
from app.services.openai_client import create_embedding
from app.services.qdrant_service import get_qdrant_service

logger = logging.getLogger(__name__)


async def max_simple_rag_answer(question: str, user_display_name: str) -> str:
    """Один проход: расширение запроса → Qdrant → ответ по чанкам."""
    q = (question or "").strip()
    if not q:
        return "Напишите вопрос текстом."
    try:
        expanded = await _expand_query_for_search(q)
        emb = await asyncio.to_thread(create_embedding, expanded)
        svc = get_qdrant_service()
        chunks = svc.search_multi_level(
            query_embedding=emb,
            top_k=8,
            initial_threshold=0.45,
            fallback_thresholds=[0.3, 0.15],
        )
        if not chunks:
            return (
                "По базе знаний ничего похожего не нашлось. "
                "Переформулируйте вопрос или воспользуйтесь Telegram-ботом (расширенный режим)."
            )
        chunks = chunks[:6]
        answer = await _generate_answer_from_chunks_private(
            q,
            chunks,
            [],
            user_display_name,
            is_first_question=True,
            topics_summary="",
        )
        return answer
    except Exception as e:
        logger.exception("MAX simple RAG failed: %s", e)
        return (
            "Не удалось получить ответ. Попробуйте позже или задайте вопрос в Telegram-боте."
        )
