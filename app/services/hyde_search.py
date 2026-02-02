"""HyDE: генерация гипотетического ответа и объединение с основным поиском через RRF."""

import asyncio
import logging
from typing import List, Dict, Any

from app.services.openai_client import client, CHAT_MODEL

logger = logging.getLogger(__name__)

RRF_K = 60


async def generate_hypothetical_answer(query: str, max_tokens: int = 150) -> str:
    """Генерирует короткий гипотетический ответ на вопрос (1–2 предложения) для поиска по эмбеддингу."""
    if not query or not query.strip():
        return ""
    try:
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "Ты помощник. По вопросу пользователя напиши краткий гипотетический ответ в 1–2 предложениях, как могла бы выглядеть выдержка из документа. Только текст ответа, без цитат и пояснений.",
                },
                {"role": "user", "content": query},
            ],
            temperature=0.3,
            max_tokens=max_tokens,
        )
        out = (resp.choices[0].message.content or "").strip()
        if out:
            logger.debug(f"[HYDE] Гипотетический ответ: '{out[:80]}...'")
        return out
    except Exception as e:
        logger.exception(f"[HYDE] Ошибка генерации гипотетического ответа: {e}")
        return ""


def merge_hyde_with_main(
    main_chunks: List[Dict[str, Any]],
    hyde_chunks: List[Dict[str, Any]],
    top_n: int = 15,
    k: int = RRF_K,
) -> List[Dict[str, Any]]:
    """Объединяет результаты основного и HyDE-поиска через RRF. По ключу — текст чанка."""
    text_to_chunk = {}
    rrf_scores = {}
    for rank, chunk in enumerate(main_chunks):
        text = (chunk.get("text") or "").strip()
        if not text:
            continue
        text_to_chunk[text] = chunk
        rrf_scores[text] = rrf_scores.get(text, 0) + 1.0 / (k + rank + 1)
    for rank, chunk in enumerate(hyde_chunks):
        text = (chunk.get("text") or "").strip()
        if not text:
            continue
        text_to_chunk[text] = chunk
        rrf_scores[text] = rrf_scores.get(text, 0) + 1.0 / (k + rank + 1)
    sorted_texts = sorted(rrf_scores.keys(), key=lambda t: rrf_scores[t], reverse=True)
    return [text_to_chunk[t] for t in sorted_texts[:top_n]]
