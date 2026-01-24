"""Поиск похожих вопросов среди FAQ (Google Sheets + OpenAI embeddings + AI rerank).
Поддерживает hot-update кэша: добавление новых FAQ без редеплоя.
"""

from typing import Optional, List, Dict, Any
import math
import asyncio
import re
import time

from app.services.sheets_client import load_faq_rows
from app.services.openai_client import create_embedding, choose_best_faq_answer
from app.services.qdrant_service import get_qdrant_service


# -----------------------------
#   ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -----------------------------

def normalize(text: str) -> str:
    """Простая нормализация текста для эмбеддингов.

    - нижний регистр
    - убираем пунктуацию
    - схлопываем пробелы
    """
    text = (text or "").lower()
    text = re.sub(r"[^\w\sёЁа-яА-Я0-9]", " ", text)  # оставляем буквы/цифры/пробелы
    text = re.sub(r"\s+", " ", text).strip()
    return text


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Косинусное сходство двух векторов."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


# -----------------------------
#   КЭШ ВОПРОСОВ И ЭМБЕДДИНГОВ
# -----------------------------

FAQ_DATA: List[Dict[str, str]] = []
FAQ_EMBEDS: List[List[float]] = []
CACHE_READY = False

# защита от гонок, когда одновременно отвечают 2 менеджера
FAQ_LOCK = asyncio.Lock()

# чтобы не дёргать Google Sheets на каждый вопрос
_LAST_REFRESH_TS = 0.0
AUTO_REFRESH_EVERY_SEC = 120  # можно 60/300 на твой вкус


async def load_faq_cache(force: bool = False) -> None:
    """Загружает вопросы + ответы + эмбеддинги в память.
    По умолчанию 1 раз, но можно force=True.
    """
    global FAQ_DATA, FAQ_EMBEDS, CACHE_READY, _LAST_REFRESH_TS

    if CACHE_READY and not force:
        return

    async with FAQ_LOCK:
        if CACHE_READY and not force:
            return

        rows = load_faq_rows()  # [{question, answer}, ...]

        FAQ_DATA = []
        FAQ_EMBEDS = []

        for row in rows:
            question = (row.get("question") or "").strip()
            answer = (row.get("answer") or "").strip()
            if not question or not answer:
                continue

            norm_question = normalize(question)
            emb = await asyncio.to_thread(create_embedding, norm_question)

            media_json = (row.get("media_json") or "").strip()
            FAQ_DATA.append(
                {
                    "question": question,
                    "norm_question": norm_question,
                    "answer": answer,
                    "media_json": media_json,
                }
            )
            FAQ_EMBEDS.append(emb)

        CACHE_READY = True
        _LAST_REFRESH_TS = time.time()
        print(f"[FAQ] Загружено {len(FAQ_DATA)} записей FAQ (эмбеддинги готовы).")


async def add_faq_entry_to_cache(question: str, answer: str, media_json: str = "") -> None:
    """Добавляет одну новую пару Q/A в Qdrant и in-memory кэш (для обратной совместимости)."""
    global CACHE_READY

    question = (question or "").strip()
    answer = (answer or "").strip()
    media_json = (media_json or "").strip()
    if not question or not answer:
        return

    # Сохраняем в Qdrant
    try:
        from app.services.chunking_service import chunk_text
        from app.services.context_enrichment import enrich_chunks_batch
        from app.services.qdrant_service import get_qdrant_service
        from datetime import datetime
        
        # Создаем единый текст
        full_text = f"Вопрос: {question}\nОтвет: {answer}"
        
        # Разбиваем на чанки
        chunks = chunk_text(full_text)
        if not chunks:
            chunks = [{
                "text": full_text,
                "chunk_index": 0,
                "total_chunks": 1,
                "start_char": 0,
                "end_char": len(full_text),
            }]
        
        # Обогащаем контекстом
        document_title = f"FAQ: {question[:50]}..." if len(question) > 50 else f"FAQ: {question}"
        enriched_chunks = await enrich_chunks_batch(chunks, document_title)
        
        # Создаем эмбеддинги
        embeddings = []
        for chunk in enriched_chunks:
            embedding = await asyncio.to_thread(create_embedding, chunk.get("text", ""))
            embeddings.append(embedding)
        
        # Подготавливаем метаданные
        timestamp = datetime.now().isoformat()
        chunks_with_metadata = []
        for chunk in enriched_chunks:
            chunks_with_metadata.append({
                "text": chunk.get("text", ""),
                "metadata": {
                    "source": "faq_manual_add",
                    "original_question": question,
                    "original_answer": answer,
                    "media_json": media_json,
                    "added_at": timestamp,
                },
            })
        
        # Загружаем в Qdrant
        qdrant_service = get_qdrant_service()
        qdrant_service.add_documents(chunks_with_metadata, embeddings)
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception(f"[FAQ_SERVICE] Ошибка сохранения в Qdrant: {e}")
        # Продолжаем с fallback на in-memory кэш

    # Также сохраняем в in-memory кэш для обратной совместимости
    if not CACHE_READY:
        await load_faq_cache()
        return

    norm_question = normalize(question)
    emb = await asyncio.to_thread(create_embedding, norm_question)

    async with FAQ_LOCK:
        # защита от дублей
        for item in FAQ_DATA[-50:]:
            if item["norm_question"] == norm_question and item["answer"] == answer:
                return

        FAQ_DATA.append(
            {
                "question": question,
                "norm_question": norm_question,
                "answer": answer,
                "media_json": media_json,
            }
        )
        FAQ_EMBEDS.append(emb)

    print(f"[FAQ] Добавлена новая запись в кэш. Всего: {len(FAQ_DATA)}")


async def refresh_faq_cache_from_sheet_if_needed(force: bool = False) -> None:
    """Периодически подтягивает новые строки из Google Sheets и добавляет их в кэш.
    Это страховка: если кто-то дописал FAQ вручную или кэш не обновился.
    """
    global _LAST_REFRESH_TS

    now = time.time()
    if not force and (now - _LAST_REFRESH_TS) < AUTO_REFRESH_EVERY_SEC:
        return

    await load_faq_cache()  # гарантируем, что кэш есть

    rows = load_faq_rows()
    _LAST_REFRESH_TS = now

    async with FAQ_LOCK:
        current_len = len(FAQ_DATA)

    # если в таблице стало больше строк — добавим только новые (хвост)
    if len(rows) <= current_len:
        return

    new_rows = rows[current_len:]
    for row in new_rows:
        q = (row.get("question") or "").strip()
        a = (row.get("answer") or "").strip()
        if not q or not a:
            continue
        await add_faq_entry_to_cache(q, a)


# -----------------------------
#    ПОИСК ПОХОЖЕГО ВОПРОСА
# -----------------------------

async def find_similar_question(user_question: str) -> Optional[Dict[str, Any]]:
    """Возвращает {question, answer, score} или None, если ничего похожего нет.

    Использует Qdrant для поиска, с fallback на in-memory кэш, если Qdrant недоступен.
    """
    try:
        # Пробуем использовать Qdrant
        qdrant_service = get_qdrant_service()
        
        # Создаем эмбеддинг запроса
        norm_user = normalize(user_question)
        user_emb = await asyncio.to_thread(create_embedding, norm_user)
        
        # Ищем в Qdrant (приоритет FAQ из миграции)
        found_chunks = qdrant_service.search(
            query_embedding=user_emb,
            top_k=5,
            score_threshold=0.7,
            source_filter="faq_migration",
        )
        
        # Если не нашли в FAQ, ищем во всех источниках
        if not found_chunks:
            found_chunks = qdrant_service.search(
                query_embedding=user_emb,
                top_k=5,
                score_threshold=0.7,
            )
        
        if found_chunks:
            # Преобразуем результаты Qdrant в формат для choose_best_faq_answer
            candidates = []
            for chunk in found_chunks:
                metadata = chunk.get("metadata", {})
                candidates.append({
                    "question": metadata.get("original_question", ""),
                    "answer": metadata.get("original_answer", ""),
                    "score": chunk.get("score", 0),
                    "media_json": metadata.get("media_json", ""),
                })
            
            # AI reranking
            best = await asyncio.to_thread(
                choose_best_faq_answer,
                user_question,
                candidates,
            )
            return best
    except Exception as e:
        # Fallback на старый метод с in-memory кэшем
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"[FAQ_SERVICE] Qdrant недоступен, используем fallback: {e}")
    
    # Fallback: старый метод с in-memory кэшем
    await load_faq_cache()
    await refresh_faq_cache_from_sheet_if_needed()

    async with FAQ_LOCK:
        if not FAQ_DATA:
            return None
        local_data = list(FAQ_DATA)
        local_embeds = list(FAQ_EMBEDS)

    # Этап 1: эмбеддинг пользователя + базовый фильтр по cosine
    norm_user = normalize(user_question)
    user_emb = await asyncio.to_thread(create_embedding, norm_user)

    scores: List[float] = []
    for emb in local_embeds:
        scores.append(cosine_similarity(user_emb, emb))

    BASE_THRESHOLD = 0.55

    indexed_scores = [
        (idx, score) for idx, score in enumerate(scores) if score >= BASE_THRESHOLD
    ]

    if not indexed_scores:
        return None

    indexed_scores.sort(key=lambda x: x[1], reverse=True)
    TOP_K = 5
    top_indices = [idx for idx, _ in indexed_scores[:TOP_K]]

    candidates: List[Dict[str, Any]] = []
    for idx in top_indices:
        data = local_data[idx]
        candidates.append(
            {
                "question": data["question"],
                "answer": data["answer"],
                "score": scores[idx],
                "media_json": data.get("media_json", ""),
            }
        )

    best = await asyncio.to_thread(
        choose_best_faq_answer,
        user_question,
        candidates,
    )
    return best