"""Поиск похожих вопросов среди FAQ в Qdrant (OpenAI embeddings + AI rerank).
Использует только Qdrant для хранения и поиска FAQ.
"""

from typing import Optional, List, Dict, Any
import asyncio
import re

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


# -----------------------------
#    ДОБАВЛЕНИЕ FAQ В QDRANT
# -----------------------------


async def add_faq_entry_to_cache(question: str, answer: str, media_json: str = "") -> None:
    """Добавляет одну новую пару Q/A в Qdrant."""
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


# -----------------------------
#    ПОИСК ПОХОЖЕГО ВОПРОСА
# -----------------------------

async def find_similar_question(user_question: str) -> Optional[Dict[str, Any]]:
    """Возвращает {question, answer, score} или None, если ничего похожего нет.

    Использует только Qdrant для поиска.
    """
    try:
        # Используем Qdrant
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
        
        return None
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"[FAQ_SERVICE] Qdrant недоступен: {e}")
        return None