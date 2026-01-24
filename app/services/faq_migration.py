"""Миграция FAQ из Google Sheets в Qdrant."""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any

from app.services.sheets_client import load_faq_rows
from app.services.chunking_service import chunk_text
from app.services.context_enrichment import enrich_chunks_batch
from app.services.openai_client import create_embedding
from app.services.qdrant_service import get_qdrant_service

logger = logging.getLogger(__name__)


async def migrate_faq_to_qdrant() -> Dict[str, Any]:
    """Мигрирует все FAQ из Google Sheets в Qdrant.
    
    Returns:
        Словарь с результатами миграции:
        {
            "total_faqs": int,
            "total_chunks": int,
            "success": bool,
            "error": str (если была ошибка)
        }
    """
    try:
        # 1. Читаем все FAQ из Google Sheets
        logger.info("[FAQ_MIGRATION] Начинаю миграцию FAQ из Google Sheets в Qdrant")
        rows = load_faq_rows()
        
        if not rows:
            logger.warning("[FAQ_MIGRATION] Нет FAQ для миграции")
            return {
                "total_faqs": 0,
                "total_chunks": 0,
                "success": True,
                "error": None,
            }
        
        logger.info(f"[FAQ_MIGRATION] Найдено {len(rows)} FAQ записей")
        
        # 2. Обрабатываем каждую пару вопрос-ответ
        all_chunks = []
        all_embeddings = []
        timestamp = datetime.now().isoformat()
        
        for idx, row in enumerate(rows):
            question = row.get("question", "").strip()
            answer = row.get("answer", "").strip()
            media_json = row.get("media_json", "").strip()
            
            if not question or not answer:
                continue
            
            # Создаем единый текст: вопрос + ответ
            full_text = f"Вопрос: {question}\nОтвет: {answer}"
            
            # Разбиваем на чанки (если длинный)
            chunks = chunk_text(full_text)
            
            if not chunks:
                # Если чанкинг не сработал, создаем один чанк
                chunks = [{
                    "text": full_text,
                    "chunk_index": 0,
                    "total_chunks": 1,
                    "start_char": 0,
                    "end_char": len(full_text),
                }]
            
            # Обогащаем контекстом
            document_title = f"FAQ: {question[:50]}..." if len(question) > 50 else f"FAQ: {question}"
            try:
                enriched_chunks = await enrich_chunks_batch(chunks, document_title)
            except Exception as e:
                logger.exception(f"[FAQ_MIGRATION] Ошибка обогащения FAQ {idx}: {e}")
                enriched_chunks = chunks
            
            # Создаем эмбеддинги для каждого чанка
            for chunk in enriched_chunks:
                try:
                    embedding = await asyncio.to_thread(
                        create_embedding,
                        chunk.get("text", ""),
                    )
                    all_embeddings.append(embedding)
                    
                    # Подготавливаем метаданные
                    chunk_with_metadata = {
                        "text": chunk.get("text", ""),
                        "metadata": {
                            "source": "faq_migration",
                            "original_question": question,
                            "original_answer": answer,
                            "media_json": media_json,
                            "chunk_index": chunk.get("chunk_index", 0),
                            "total_chunks": chunk.get("total_chunks", len(enriched_chunks)),
                            "migrated_at": timestamp,
                        },
                    }
                    all_chunks.append(chunk_with_metadata)
                except Exception as e:
                    logger.exception(f"[FAQ_MIGRATION] Ошибка создания эмбеддинга для FAQ {idx}: {e}")
                    continue
            
            if (idx + 1) % 10 == 0:
                logger.info(f"[FAQ_MIGRATION] Обработано {idx + 1}/{len(rows)} FAQ")
        
        # 3. Загружаем все в Qdrant
        if all_chunks:
            logger.info(f"[FAQ_MIGRATION] Загружаю {len(all_chunks)} чанков в Qdrant")
            qdrant_service = get_qdrant_service()
            qdrant_service.add_documents(all_chunks, all_embeddings)
            logger.info(f"[FAQ_MIGRATION] Миграция завершена успешно")
        else:
            logger.warning("[FAQ_MIGRATION] Нет чанков для загрузки")
        
        return {
            "total_faqs": len(rows),
            "total_chunks": len(all_chunks),
            "success": True,
            "error": None,
        }
        
    except Exception as e:
        logger.exception(f"[FAQ_MIGRATION] Ошибка миграции: {e}")
        return {
            "total_faqs": 0,
            "total_chunks": 0,
            "success": False,
            "error": str(e),
        }
