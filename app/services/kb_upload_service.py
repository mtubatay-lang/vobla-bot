"""
Пайплайн загрузки документа в Qdrant (общий для Telegram и MAX).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Awaitable, Callable, Optional

from app.services.chunking_service import extract_metadata_from_text, structure_aware_chunk_text
from app.services.context_enrichment import enrich_chunks_batch
from app.services.document_preparation import prepare_for_rag
from app.services.document_processor import extract_text_with_structure
from app.services.metrics_service import alog_event
from app.services.openai_client import create_embedding
from app.services.qdrant_service import get_qdrant_service

logger = logging.getLogger(__name__)

Notify = Callable[[str], Awaitable[None]]


async def process_kb_document_pipeline(
    file_content: bytes,
    filename: str,
    document_title: str,
    user_id: Optional[int],
    notify: Notify,
) -> bool:
    """
    Предподготовка → извлечение текста → чанки → обогащение → эмбеддинги → Qdrant.
    notify(text) — статус (HTML допустим).
    Возвращает True при успехе.
    """
    is_csv = filename.lower().endswith(".csv")

    await notify("⏳ Предподготавливаю документ для RAG...")
    try:
        prepared = await asyncio.to_thread(prepare_for_rag, file_content, filename)
    except Exception as e:
        logger.exception("[KB_UPLOAD] prepare_for_rag: %s", e)
        prepared = None
    if prepared is not None:
        file_content, filename = prepared[0], prepared[1]
    elif is_csv:
        await notify(
            "❌ CSV не распознан. Нужны колонки: «Вопросы из Телеграм», «Ответ в Телеграм» или «Ответы Финал»."
        )
        return False

    await notify("⏳ Извлекаю текст из документа...")
    try:
        extracted = extract_text_with_structure(file_content, filename)
        text = extracted.get("text", "")
        structure = extracted.get("structure", [])
        if not text or not text.strip():
            await notify("❌ Не удалось извлечь текст из документа")
            return False
    except Exception as e:
        logger.exception("[KB_UPLOAD] extract: %s", e)
        await notify(f"❌ Ошибка извлечения текста: {str(e)[:300]}")
        return False

    await notify("⏳ Разбиваю документ на чанки...")
    chunks = structure_aware_chunk_text(text, structure=structure, document_title=document_title)
    if not chunks:
        await notify("❌ Не удалось разбить документ на чанки")
        return False

    await notify(f"⏳ Обогащаю {len(chunks)} чанков контекстом...")
    try:
        enriched_chunks = await enrich_chunks_batch(chunks, document_title)
    except Exception as e:
        logger.exception("[KB_UPLOAD] enrich: %s", e)
        enriched_chunks = chunks

    await notify(f"⏳ Создаю эмбеддинги для {len(enriched_chunks)} чанков...")
    embeddings = []
    for chunk in enriched_chunks:
        try:
            embedding = await asyncio.to_thread(create_embedding, chunk.get("text", ""))
            embeddings.append(embedding)
        except Exception as e:
            logger.exception("[KB_UPLOAD] embedding: %s", e)
            embeddings.append([0.0] * 1536)

    extracted_metadata = extract_metadata_from_text(text, source="manual_upload")
    timestamp = datetime.now().isoformat()
    chunks_with_metadata = []
    for chunk in enriched_chunks:
        chunk_meta = chunk.get("metadata") or {}
        meta = {
            "source": "manual_upload",
            "document_type": extracted_metadata.get("document_type", "reference"),
            "category": extracted_metadata.get("category", "общее"),
            "tags": extracted_metadata.get("tags", []),
            "keywords": extracted_metadata.get("keywords", []),
            "document_title": document_title,
            "filename": filename,
            "chunk_index": chunk.get("chunk_index", 0),
            "total_chunks": chunk.get("total_chunks", len(enriched_chunks)),
            "uploaded_by": user_id,
            "uploaded_at": timestamp,
        }
        for key in ("section_path", "section_id", "section_title", "chunk_id"):
            if chunk_meta.get(key) is not None:
                meta[key] = chunk_meta[key]
        chunks_with_metadata.append({"text": chunk.get("text", ""), "metadata": meta})

    await notify("⏳ Загружаю в Qdrant...")
    try:
        qdrant_service = get_qdrant_service()
        qdrant_service.add_documents(chunks_with_metadata, embeddings)
    except Exception as e:
        logger.exception("[KB_UPLOAD] qdrant: %s", e)
        await notify(f"❌ Ошибка загрузки в Qdrant: {str(e)[:300]}")
        return False

    await notify(
        f"✅ <b>Документ добавлен в базу знаний</b>\n\n"
        f"📄 {document_title}\n📊 Чанков: {len(enriched_chunks)}\n📁 {filename}"
    )
    await alog_event(
        user_id=user_id,
        username=None,
        event="kb_document_uploaded",
        meta={
            "document_title": document_title,
            "filename": filename,
            "chunks_count": len(enriched_chunks),
        },
    )
    return True
