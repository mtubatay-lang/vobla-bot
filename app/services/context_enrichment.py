"""Обогащение чанков контекстом через OpenAI API."""

import asyncio
import logging
from typing import Optional, List, Dict, Any

from app.services.openai_client import client, CHAT_MODEL

logger = logging.getLogger(__name__)


def enrich_chunk_with_context(
    chunk_text: str,
    document_title: str,
    previous_chunk: Optional[str] = None,
    next_chunk: Optional[str] = None,
) -> str:
    """Обогащает чанк контекстом через OpenAI.
    
    Добавляет краткий контекст в начало чанка, чтобы его было легче найти по вопросам.
    Оригинальный текст сохраняется без изменений.
    
    Args:
        chunk_text: Текст чанка для обогащения
        document_title: Название документа
        previous_chunk: Предыдущий чанк (для контекста)
        next_chunk: Следующий чанк (для контекста)
    
    Returns:
        Обогащенный текст в формате: "[Контекст] Оригинальный текст чанка"
    """
    if not chunk_text or not chunk_text.strip():
        return chunk_text
    
    # Формируем контекст из соседних чанков
    context_parts = []
    if previous_chunk:
        # Берем последние 200 символов предыдущего чанка
        prev_snippet = previous_chunk[-200:].strip()
        if prev_snippet:
            context_parts.append(f"Предыдущий фрагмент: {prev_snippet}")
    
    if next_chunk:
        # Берем первые 200 символов следующего чанка
        next_snippet = next_chunk[:200].strip()
        if next_snippet:
            context_parts.append(f"Следующий фрагмент: {next_snippet}")
    
    context_info = "\n".join(context_parts) if context_parts else "Нет соседних фрагментов"
    
    system_prompt = (
        "Ты помощник для обогащения фрагментов документов контекстом.\n"
        "Твоя задача — добавить краткий контекст к фрагменту документа, "
        "чтобы его было легче найти по вопросам.\n\n"
        "Правила:\n"
        "1. Сохрани оригинальный текст фрагмента БЕЗ ИЗМЕНЕНИЙ.\n"
        "2. Добавь краткий контекст в начале (1-2 предложения).\n"
        "3. Контекст должен описывать, о чем этот фрагмент, в каком документе он находится.\n"
        "4. Используй информацию о соседних фрагментах, если она есть.\n"
        "5. Формат: [Контекст] Оригинальный текст\n"
        "6. Не добавляй лишних деталей, только самое важное для поиска."
    )
    
    user_prompt = (
        f"Название документа: {document_title}\n\n"
        f"Контекст соседних фрагментов:\n{context_info}\n\n"
        f"Фрагмент документа для обогащения:\n{chunk_text}\n\n"
        "Добавь краткий контекст в начале, сохранив оригинальный текст без изменений."
    )
    
    try:
        resp = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )
        
        enriched_text = resp.choices[0].message.content or chunk_text
        return enriched_text.strip()
    except Exception as e:
        logger.exception(f"[CONTEXT_ENRICHMENT] Ошибка обогащения чанка: {e}")
        # При ошибке возвращаем оригинал с минимальным контекстом
        return f"[Документ: {document_title}] {chunk_text}"


async def enrich_chunks_batch(
    chunks: List[Dict[str, Any]],
    document_title: str,
) -> List[Dict[str, Any]]:
    """Обогащает список чанков контекстом (асинхронно, батчами).
    
    Args:
        chunks: Список словарей с чанками (должны содержать поле "text")
        document_title: Название документа
    
    Returns:
        Список обогащенных чанков (с обновленным полем "text")
    """
    if not chunks:
        return chunks
    
    enriched_chunks = []
    
    for i, chunk in enumerate(chunks):
        chunk_text = chunk.get("text", "")
        if not chunk_text:
            enriched_chunks.append(chunk)
            continue
        
        # Получаем соседние чанки
        previous_chunk = None
        if i > 0:
            previous_chunk = chunks[i - 1].get("text")
        
        next_chunk = None
        if i < len(chunks) - 1:
            next_chunk = chunks[i + 1].get("text")
        
        # Обогащаем в отдельном потоке, чтобы не блокировать event loop
        enriched_text = await asyncio.to_thread(
            enrich_chunk_with_context,
            chunk_text,
            document_title,
            previous_chunk,
            next_chunk,
        )
        
        # Создаем копию чанка с обновленным текстом
        enriched_chunk = chunk.copy()
        enriched_chunk["text"] = enriched_text
        enriched_chunks.append(enriched_chunk)
    
    return enriched_chunks
