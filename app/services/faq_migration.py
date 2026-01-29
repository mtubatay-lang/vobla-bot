"""Миграция FAQ из Google Sheets в Qdrant."""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable, Awaitable

from app.services.sheets_client import load_faq_rows
from app.services.chunking_service import semantic_chunk_text, extract_metadata_from_text
from app.services.context_enrichment import enrich_chunks_batch
from app.services.openai_client import create_embedding
from app.services.qdrant_service import get_qdrant_service
from app.services.faq_llm_processor import deduplicate_and_normalize_faq, improve_faq_entry_llm

logger = logging.getLogger(__name__)


async def migrate_faq_to_qdrant(
    progress_callback: Optional[Callable[[str, str], Awaitable[None]]] = None,
) -> Dict[str, Any]:
    """Мигрирует все FAQ из Google Sheets в Qdrant (два прохода: дедупликация + улучшение).

    Args:
        progress_callback: опционально вызывается (stage, detail) для обновления статуса в Telegram.

    Returns:
        Словарь с результатами миграции:
        total_faqs, total_chunks, deduplicated_groups, success, error
    """
    async def _progress(stage: str, detail: str) -> None:
        if progress_callback:
            await progress_callback(stage, detail)

    try:
        logger.info("[FAQ_MIGRATION] Начинаю миграцию FAQ из Google Sheets в Qdrant")
        rows = load_faq_rows()

        if not rows:
            logger.warning("[FAQ_MIGRATION] Нет FAQ для миграции")
            return {
                "total_faqs": 0,
                "total_chunks": 0,
                "deduplicated_groups": 0,
                "success": True,
                "error": None,
            }

        logger.info(f"[FAQ_MIGRATION] Найдено {len(rows)} FAQ записей")
        await _progress("Читаю FAQ", f"{len(rows)} записей")

        # Проход 1: дедупликация
        await _progress("Проход 1", "дедупликация вопросов")
        normalized = await deduplicate_and_normalize_faq(rows)
        if not normalized and rows:
            logger.error("[FAQ_MIGRATION] Дедупликация вернула пустой список при непустых rows")
            return {
                "total_faqs": len(rows),
                "total_chunks": 0,
                "deduplicated_groups": 0,
                "success": False,
                "error": "Ошибка дедупликации: пустой результат",
            }
        await _progress("Проход 1", f"осталось {len(normalized)} групп")

        # Проход 2: улучшение формулировок
        await _progress("Проход 2", "улучшение формулировок")
        improved_entries: List[Dict[str, Any]] = []
        for entry in normalized:
            improved = await improve_faq_entry_llm(entry)
            improved_entries.append(improved)

        # Загрузка: чанкинг, обогащение, эмбеддинги
        await _progress("Загрузка", "создание чанков и эмбеддингов")
        all_chunks = []
        all_embeddings = []
        timestamp = datetime.now().isoformat()

        for idx, entry in enumerate(improved_entries):
            canonical_question = entry.get("canonical_question", "")
            question_variants = entry.get("question_variants", []) or [canonical_question]
            improved_answer = entry.get("improved_answer", entry.get("merged_answer", ""))
            synonym_questions = entry.get("synonym_questions", []) or []
            media_json = entry.get("media_json", "").strip()

            if not improved_answer:
                continue

            parts = ["Вопрос (варианты): " + ", ".join(question_variants)]
            if synonym_questions:
                parts.append("Синонимы: " + ", ".join(synonym_questions))
            parts.append("Ответ: " + improved_answer)
            full_text = "\n".join(parts)

            chunks = semantic_chunk_text(full_text)
            if not chunks:
                chunks = [{
                    "text": full_text,
                    "chunk_index": 0,
                    "total_chunks": 1,
                    "start_char": 0,
                    "end_char": len(full_text),
                }]

            document_title = f"FAQ: {canonical_question[:50]}..." if len(canonical_question) > 50 else f"FAQ: {canonical_question}"
            try:
                enriched_chunks = await enrich_chunks_batch(chunks, document_title)
            except Exception as e:
                logger.exception(f"[FAQ_MIGRATION] Ошибка обогащения FAQ {idx}: {e}")
                enriched_chunks = chunks

            extracted_metadata = extract_metadata_from_text(full_text, source="faq_migration")

            for chunk in enriched_chunks:
                try:
                    embedding = await asyncio.to_thread(
                        create_embedding,
                        chunk.get("text", ""),
                    )
                    all_embeddings.append(embedding)
                    chunk_with_metadata = {
                        "text": chunk.get("text", ""),
                        "metadata": {
                            "source": "faq_migration",
                            "document_type": extracted_metadata.get("document_type", "faq"),
                            "category": extracted_metadata.get("category", "общее"),
                            "tags": extracted_metadata.get("tags", []),
                            "keywords": extracted_metadata.get("keywords", []),
                            "original_question": canonical_question,
                            "original_answer": improved_answer,
                            "question_variants": question_variants,
                            "media_json": media_json,
                            "chunk_index": chunk.get("chunk_index", 0),
                            "total_chunks": chunk.get("total_chunks", len(enriched_chunks)),
                            "migrated_at": timestamp,
                            "document_title": document_title,
                        },
                    }
                    all_chunks.append(chunk_with_metadata)
                except Exception as e:
                    logger.exception(f"[FAQ_MIGRATION] Ошибка создания эмбеддинга для FAQ {idx}: {e}")
                    continue

            if (idx + 1) % 10 == 0:
                logger.info(f"[FAQ_MIGRATION] Обработано {idx + 1}/{len(improved_entries)} FAQ")

        if all_chunks:
            logger.info(f"[FAQ_MIGRATION] Загружаю {len(all_chunks)} чанков в Qdrant")
            qdrant_service = get_qdrant_service()
            qdrant_service.add_documents(all_chunks, all_embeddings)
            logger.info("[FAQ_MIGRATION] Миграция завершена успешно")
        else:
            logger.warning("[FAQ_MIGRATION] Нет чанков для загрузки")

        return {
            "total_faqs": len(rows),
            "total_chunks": len(all_chunks),
            "deduplicated_groups": len(normalized),
            "success": True,
            "error": None,
        }

    except Exception as e:
        logger.exception(f"[FAQ_MIGRATION] Ошибка миграции: {e}")
        return {
            "total_faqs": 0,
            "total_chunks": 0,
            "deduplicated_groups": 0,
            "success": False,
            "error": str(e),
        }
