"""Сервис для анализа и выбора чанков через LLM."""

import asyncio
import logging
from typing import List, Dict, Any, Tuple, Optional

from app.services.openai_client import client, CHAT_MODEL
from app.config import CHUNK_ANALYSIS_ENABLED, MAX_CHUNKS_TO_ANALYZE

logger = logging.getLogger(__name__)


async def analyze_chunks_relevance(
    question: str,
    chunks: List[Dict[str, Any]],
) -> List[Tuple[Dict[str, Any], float, str]]:
    """Анализирует релевантность каждого чанка к вопросу.
    
    Args:
        question: Вопрос пользователя
        chunks: Список чанков для анализа
    
    Returns:
        Список кортежей: (chunk, relevance_score, explanation)
        relevance_score: от 0.0 до 1.0
        explanation: краткое объяснение релевантности
    """
    if not CHUNK_ANALYSIS_ENABLED:
        # Если анализ отключен, возвращаем исходные чанки с их scores
        return [(chunk, chunk.get("score", 0.5), "Анализ отключен") for chunk in chunks]
    
    if not chunks:
        return []
    
    # Ограничиваем количество чанков для анализа
    chunks_to_analyze = chunks[:MAX_CHUNKS_TO_ANALYZE]
    
    try:
        # Формируем промпт для анализа
        chunks_text = "\n\n---\n\n".join([
            f"Чанк {i+1}:\n{chunk.get('text', '')[:400]}"
            for i, chunk in enumerate(chunks_to_analyze)
        ])
        
        system_prompt = (
            "Ты помощник для анализа релевантности фрагментов текста к вопросу.\n"
            "Твоя задача — оценить, насколько каждый фрагмент релевантен вопросу.\n\n"
            "Правила:\n"
            "1. Оценивай релевантность от 0.0 до 1.0\n"
            "2. 1.0 = фрагмент напрямую отвечает на вопрос\n"
            "3. 0.5 = фрагмент частично релевантен\n"
            "4. 0.0 = фрагмент не релевантен\n"
            "5. Для каждого фрагмента верни оценку и краткое объяснение (1-2 предложения)\n"
            "6. Формат: для каждого фрагмента напиши 'Чанк N: оценка, объяснение'"
        )
        
        user_prompt = (
            f"Вопрос пользователя: {question}\n\n"
            f"Фрагменты для анализа:\n{chunks_text}\n\n"
            "Оцени релевантность каждого фрагмента к вопросу. "
            "Для каждого фрагмента укажи номер, оценку (0.0-1.0) и краткое объяснение."
        )
        
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=500,
        )
        
        answer = (resp.choices[0].message.content or "").strip()
        
        # Парсим ответ
        results = []
        import re
        
        for i, chunk in enumerate(chunks_to_analyze):
            # Ищем оценку для этого чанка в ответе
            pattern = rf"Чанк\s+{i+1}:\s*([\d.]+)"
            match = re.search(pattern, answer)
            
            if match:
                score = float(match.group(1))
                # Извлекаем объяснение (текст после оценки до следующего "Чанк" или конца)
                explanation_match = re.search(
                    rf"Чанк\s+{i+1}:\s*[\d.]+\s*,\s*(.+?)(?=Чанк\s+\d+:|$)",
                    answer,
                    re.DOTALL
                )
                explanation = explanation_match.group(1).strip() if explanation_match else "Релевантен"
            else:
                # Если не нашли, используем исходный score
                score = chunk.get("score", 0.5)
                explanation = "Оценка не найдена, используется исходный score"
            
            results.append((chunk, score, explanation))
        
        # Добавляем оставшиеся чанки без анализа
        for chunk in chunks[len(chunks_to_analyze):]:
            results.append((chunk, chunk.get("score", 0.5), "Не анализировался"))
        
        logger.info(
            f"[CHUNK_ANALYZER] Проанализировано {len(chunks_to_analyze)} чанков из {len(chunks)} "
            f"для вопроса: '{question[:50]}...'"
        )
        
        return results
        
    except Exception as e:
        logger.exception(f"[CHUNK_ANALYZER] Ошибка анализа чанков: {e}")
        # При ошибке возвращаем исходные scores
        return [(chunk, chunk.get("score", 0.5), "Ошибка анализа") for chunk in chunks]


async def select_and_combine_chunks(
    question: str,
    analyzed_chunks: List[Tuple[Dict[str, Any], float, str]],
    max_chunks: int = 5,
) -> List[Dict[str, Any]]:
    """Выбирает наиболее релевантные чанки и определяет, какие дополняют друг друга.
    
    Args:
        question: Вопрос пользователя
        analyzed_chunks: Список кортежей (chunk, relevance_score, explanation)
        max_chunks: Максимальное количество чанков для возврата
    
    Returns:
        Список выбранных и обработанных чанков
    """
    if not analyzed_chunks:
        return []
    
    # Сортируем по релевантности
    sorted_chunks = sorted(analyzed_chunks, key=lambda x: x[1], reverse=True)
    
    # Выбираем топ-N
    top_chunks = sorted_chunks[:max_chunks]
    
    # Обновляем scores в чанках
    selected_chunks = []
    for chunk, new_score, explanation in top_chunks:
        chunk_copy = chunk.copy()
        chunk_copy["score"] = new_score
        chunk_copy["relevance_explanation"] = explanation
        selected_chunks.append(chunk_copy)
    
    logger.info(
        f"[CHUNK_ANALYZER] Выбрано {len(selected_chunks)} чанков из {len(analyzed_chunks)} "
        f"для вопроса: '{question[:50]}...'"
    )
    
    return selected_chunks


async def extract_key_information(
    question: str,
    chunks: List[Dict[str, Any]],
) -> str:
    """Извлекает ключевую информацию из чанков и структурирует её для генерации ответа.
    
    Args:
        question: Вопрос пользователя
        chunks: Список релевантных чанков
    
    Returns:
        Структурированная информация в виде текста
    """
    if not chunks:
        return ""
    
    try:
        chunks_text = "\n\n---\n\n".join([
            f"Фрагмент {i+1}:\n{chunk.get('text', '')}"
            for i, chunk in enumerate(chunks)
        ])
        
        system_prompt = (
            "Ты помощник для извлечения ключевой информации из фрагментов текста.\n"
            "Твоя задача — извлечь и структурировать информацию, релевантную вопросу.\n\n"
            "Правила:\n"
            "1. Извлекай только информацию, которая отвечает на вопрос\n"
            "2. Структурируй информацию логично\n"
            "3. Убирай дубликаты и перекрывающуюся информацию\n"
            "4. Сохраняй важные детали\n"
            "5. Форматируй текст для удобного чтения"
        )
        
        user_prompt = (
            f"Вопрос пользователя: {question}\n\n"
            f"Фрагменты текста:\n{chunks_text}\n\n"
            "Извлеки и структурируй ключевую информацию, релевантную вопросу."
        )
        
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=1000,
        )
        
        extracted_info = (resp.choices[0].message.content or "").strip()
        
        logger.info(
            f"[CHUNK_ANALYZER] Извлечена ключевая информация из {len(chunks)} чанков "
            f"для вопроса: '{question[:50]}...'"
        )
        
        return extracted_info
        
    except Exception as e:
        logger.exception(f"[CHUNK_ANALYZER] Ошибка извлечения информации: {e}")
        # При ошибке возвращаем простое объединение чанков
        return "\n\n".join([chunk.get("text", "") for chunk in chunks])
