"""Сервис для разбивки текста на чанки."""

import re
import logging
from typing import List, Dict, Any, Optional
from app.config import (
    CHUNK_SIZE, 
    CHUNK_OVERLAP,
    SEMANTIC_CHUNK_MIN_SIZE,
    SEMANTIC_CHUNK_MAX_SIZE,
    SEMANTIC_CHUNK_OVERLAP,
)

logger = logging.getLogger(__name__)


def get_chunk_structural_metadata(chunk_text: str) -> Dict[str, Any]:
    """Возвращает структурные метаданные для чанка: is_checklist, item_count, section_heading."""
    text = (chunk_text or "").strip()
    numbered_lines = re.findall(r"^\d+[\.\)]\s+.+", text, re.MULTILINE)
    item_count = len(numbered_lines)
    is_checklist = item_count >= 3
    section_heading = ""
    first_line = text.split("\n")[0].strip() if text else ""
    if first_line and (first_line.endswith(":") or first_line.startswith("#") or re.match(r"^\d+[\.\)]\s+", first_line)):
        section_heading = first_line[:200]
    return {
        "is_checklist": is_checklist,
        "item_count": item_count,
        "section_heading": section_heading,
    }


def chunk_text(text: str, chunk_size: int = None, overlap: int = None) -> List[Dict[str, Any]]:
    """Разбивает текст на чанки с перекрытием.
    
    Args:
        text: Текст для разбивки
        chunk_size: Размер чанка в символах (по умолчанию из конфига)
        overlap: Размер перекрытия между чанками в символах (по умолчанию из конфига)
    
    Returns:
        Список словарей с чанками:
        [
            {
                "text": "...",
                "chunk_index": 0,
                "total_chunks": 5,
                "start_char": 0,
                "end_char": 1000
            },
            ...
        ]
    """
    if chunk_size is None:
        chunk_size = CHUNK_SIZE
    if overlap is None:
        overlap = CHUNK_OVERLAP
    
    if not text or not text.strip():
        return []
    
    # Разбиваем текст на предложения
    # Паттерн для предложений: точка/вопросительный/восклицательный знак + пробел или конец строки
    sentences = re.split(r'([.!?]\s+|\.\n)', text)
    
    # Объединяем разделители с предыдущими предложениями
    merged_sentences = []
    for i, sentence in enumerate(sentences):
        if i == 0:
            merged_sentences.append(sentence)
        elif re.match(r'^[.!?]\s*$', sentence):
            # Это разделитель, добавляем к предыдущему
            if merged_sentences:
                merged_sentences[-1] += sentence
            else:
                merged_sentences.append(sentence)
        else:
            merged_sentences.append(sentence)
    
    # Фильтруем пустые предложения
    sentences = [s.strip() for s in merged_sentences if s.strip()]
    
    if not sentences:
        return []
    
    chunks = []
    current_chunk = []
    current_length = 0
    start_char = 0
    
    for sentence in sentences:
        sentence_length = len(sentence)
        
        # Если добавление предложения превысит размер чанка
        if current_length + sentence_length > chunk_size and current_chunk:
            # Сохраняем текущий чанк
            chunk_text = " ".join(current_chunk)
            end_char = start_char + len(chunk_text)
            
            chunks.append({
                "text": chunk_text,
                "chunk_index": len(chunks),
                "total_chunks": 0,  # Заполнится позже
                "start_char": start_char,
                "end_char": end_char,
            })
            
            # Начинаем новый чанк с перекрытием
            # Берем последние N символов из текущего чанка для перекрытия
            overlap_text = chunk_text[-overlap:] if len(chunk_text) > overlap else chunk_text
            # Разбиваем перекрытие на слова для более аккуратного разрыва
            overlap_words = overlap_text.split()
            
            current_chunk = overlap_words + [sentence]
            current_length = sum(len(w) + 1 for w in current_chunk) - 1  # +1 за пробелы, -1 за последний
            start_char = end_char - len(overlap_text)
        else:
            current_chunk.append(sentence)
            current_length += sentence_length + 1  # +1 за пробел
    
    # Добавляем последний чанк, если есть
    if current_chunk:
        chunk_text = " ".join(current_chunk)
        end_char = start_char + len(chunk_text)
        
        chunks.append({
            "text": chunk_text,
            "chunk_index": len(chunks),
            "total_chunks": 0,  # Заполнится ниже
            "start_char": start_char,
            "end_char": end_char,
        })
    
    # Обновляем total_chunks для всех чанков
    total_chunks = len(chunks)
    for chunk in chunks:
        chunk["total_chunks"] = total_chunks
    
    return chunks


def detect_semantic_boundaries(text: str) -> List[int]:
    """Определяет границы смысловых блоков в тексте.
    
    Ищет:
    - Заголовки (строки, заканчивающиеся на : или начинающиеся с #, цифры)
    - Двойные переносы строк (абзацы)
    - Списки (маркированные или нумерованные)
    - Таблицы (строки с разделителями | или табуляцией)
    
    Args:
        text: Текст для анализа
    
    Returns:
        Список позиций символов, где начинаются смысловые блоки
    """
    boundaries = [0]  # Начало текста - всегда граница
    
    lines = text.split('\n')
    current_pos = 0
    
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        
        # Пропускаем пустые строки
        if not line_stripped:
            current_pos += len(line) + 1  # +1 за \n
            continue
        
        # Заголовки: строка заканчивается на : или начинается с #, цифры с точкой
        is_heading = (
            line_stripped.endswith(':') and len(line_stripped) < 100
            or line_stripped.startswith('#')
            or re.match(r'^\d+[\.\)]\s+[А-ЯЁ]', line_stripped)  # Нумерованный список с заголовком
        )
        
        # Двойной перенос строки (абзац)
        is_paragraph_break = (
            i > 0 and not lines[i-1].strip() and line_stripped
        )
        
        # Начало списка
        is_list_start = re.match(r'^[\-\*\•]\s+|^\d+[\.\)]\s+', line_stripped)
        
        if is_heading or (is_paragraph_break and current_pos not in boundaries):
            if current_pos not in boundaries:
                boundaries.append(current_pos)
        
        current_pos += len(line) + 1  # +1 за \n
    
    # Добавляем конец текста
    if len(text) not in boundaries:
        boundaries.append(len(text))
    
    return sorted(set(boundaries))


def semantic_chunk_text(
    text: str,
    min_size: int = None,
    max_size: int = None,
    overlap: int = None,
) -> List[Dict[str, Any]]:
    """Разбивает текст на чанки семантически (по абзацам и смысловым блокам).
    
    Приоритет разбивки:
    1. По абзацам (двойной перенос строки)
    2. По смысловым границам (заголовки, списки)
    3. По предложениям (если абзац слишком большой)
    
    Args:
        text: Текст для разбивки
        min_size: Минимальный размер чанка в символах
        max_size: Максимальный размер чанка в символах
        overlap: Размер перекрытия между чанками в символах
    
    Returns:
        Список словарей с чанками (формат как в chunk_text)
    """
    if min_size is None:
        min_size = SEMANTIC_CHUNK_MIN_SIZE
    if max_size is None:
        max_size = SEMANTIC_CHUNK_MAX_SIZE
    if overlap is None:
        overlap = SEMANTIC_CHUNK_OVERLAP
    
    if not text or not text.strip():
        return []
    
    # Шаг 1: Разбиваем по абзацам (двойной перенос строки)
    paragraphs = re.split(r'\n\s*\n+', text)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
    
    if not paragraphs:
        # Если нет абзацев, разбиваем по одинарным переносам
        paragraphs = [line.strip() for line in text.split('\n') if line.strip()]
    
    if not paragraphs:
        return []
    
    # Шаг 2: Определяем смысловые границы
    boundaries = detect_semantic_boundaries(text)
    
    # Шаг 3: Группируем абзацы в чанки
    chunks = []
    current_chunk_parts = []
    current_length = 0
    start_char = 0
    
    for i, paragraph in enumerate(paragraphs):
        para_length = len(paragraph)
        is_numbered_item = bool(re.match(r'^\d+[\.\)]\s+', paragraph))

        # Проверяем, не является ли это границей смыслового блока
        # (упрощенная проверка - если абзац короткий и похож на заголовок)
        is_boundary = (
            para_length < 100 and 
            (paragraph.endswith(':') or paragraph.startswith('#') or
             re.match(r'^\d+[\.\)]\s+[А-ЯЁ]', paragraph))
        )
        
        # Не разрываем нумерованный список: если текущий чанк заканчивается на пункт списка и абзац — тоже пункт, не режем
        last_is_numbered = (
            current_chunk_parts and
            re.match(r'^\d+[\.\)]\s+', current_chunk_parts[-1])
        )
        if last_is_numbered and is_numbered_item and (current_length + para_length > max_size):
            # Превысили max_size, но оба — пункты списка: не режем, добавляем в текущий чанк (до разумного лимита)
            if current_length + para_length <= max_size * 2:
                current_chunk_parts.append(paragraph)
                current_length += para_length + 2
                continue
        
        # Если добавление абзаца превысит max_size
        if current_length + para_length > max_size and current_chunk_parts:
            # Сохраняем текущий чанк
            chunk_text = "\n\n".join(current_chunk_parts)
            # Убеждаемся, что чанк не слишком маленький
            if len(chunk_text) >= min_size or not chunks:
                end_char = start_char + len(chunk_text)
                chunks.append({
                    "text": chunk_text,
                    "chunk_index": len(chunks),
                    "total_chunks": 0,  # Заполнится позже
                    "start_char": start_char,
                    "end_char": end_char,
                })
                start_char = end_char - overlap  # Перекрытие
                # Начинаем новый чанк с перекрытия (последние N символов)
                overlap_text = chunk_text[-overlap:] if len(chunk_text) > overlap else ""
                if overlap_text:
                    current_chunk_parts = [overlap_text]
                    current_length = len(overlap_text)
                else:
                    current_chunk_parts = []
                    current_length = 0
            else:
                # Чанк слишком маленький, продолжаем накапливать
                pass
        
        # Если это граница смыслового блока и текущий чанк достаточно большой
        if is_boundary and current_length >= min_size and current_chunk_parts:
            # Сохраняем текущий чанк перед границей
            chunk_text = "\n\n".join(current_chunk_parts)
            end_char = start_char + len(chunk_text)
            chunks.append({
                "text": chunk_text,
                "chunk_index": len(chunks),
                "total_chunks": 0,
                "start_char": start_char,
                "end_char": end_char,
            })
            start_char = end_char
            current_chunk_parts = []
            current_length = 0
        
        # Добавляем абзац к текущему чанку
        current_chunk_parts.append(paragraph)
        current_length += para_length + 2  # +2 за \n\n
    
    # Добавляем последний чанк
    if current_chunk_parts:
        chunk_text = "\n\n".join(current_chunk_parts)
        # Если последний чанк слишком маленький, объединяем с предыдущим
        if len(chunk_text) < min_size and chunks:
            # Объединяем с предыдущим чанком
            prev_chunk = chunks[-1]
            combined_text = prev_chunk["text"] + "\n\n" + chunk_text
            prev_chunk["text"] = combined_text
            prev_chunk["end_char"] = prev_chunk["start_char"] + len(combined_text)
        else:
            end_char = start_char + len(chunk_text)
            chunks.append({
                "text": chunk_text,
                "chunk_index": len(chunks),
                "total_chunks": 0,
                "start_char": start_char,
                "end_char": end_char,
            })
    
    # Если чанк слишком большой, разбиваем его по предложениям
    final_chunks = []
    for chunk in chunks:
        if len(chunk["text"]) > max_size:
            # Разбиваем большой чанк по предложениям
            sentences = re.split(r'([.!?]\s+|\.\n)', chunk["text"])
            merged_sentences = []
            for i, sentence in enumerate(sentences):
                if i == 0:
                    merged_sentences.append(sentence)
                elif re.match(r'^[.!?]\s*$', sentence):
                    if merged_sentences:
                        merged_sentences[-1] += sentence
                else:
                    merged_sentences.append(sentence)
            
            sentences = [s.strip() for s in merged_sentences if s.strip()]
            sub_chunk_parts = []
            sub_length = 0
            sub_start = chunk["start_char"]
            
            for sentence in sentences:
                sent_length = len(sentence)
                if sub_length + sent_length > max_size and sub_chunk_parts:
                    sub_text = " ".join(sub_chunk_parts)
                    final_chunks.append({
                        "text": sub_text,
                        "chunk_index": len(final_chunks),
                        "total_chunks": 0,
                        "start_char": sub_start,
                        "end_char": sub_start + len(sub_text),
                    })
                    sub_start = sub_start + len(sub_text) - overlap
                    overlap_text = sub_text[-overlap:] if len(sub_text) > overlap else ""
                    sub_chunk_parts = [overlap_text] if overlap_text else []
                    sub_length = len(overlap_text)
                
                sub_chunk_parts.append(sentence)
                sub_length += sent_length + 1
            
            if sub_chunk_parts:
                sub_text = " ".join(sub_chunk_parts)
                final_chunks.append({
                    "text": sub_text,
                    "chunk_index": len(final_chunks),
                    "total_chunks": 0,
                    "start_char": sub_start,
                    "end_char": sub_start + len(sub_text),
                })
        else:
            final_chunks.append(chunk)
    
    # Обновляем total_chunks для всех чанков
    total_chunks = len(final_chunks)
    for chunk in final_chunks:
        chunk["total_chunks"] = total_chunks
        chunk["chunk_index"] = final_chunks.index(chunk)
    
    logger.info(
        f"[CHUNKING] Семантический chunking: создано {total_chunks} чанков "
        f"(min={min_size}, max={max_size}, overlap={overlap})"
    )
    
    return final_chunks


def extract_metadata_from_text(text: str, source: str = "unknown") -> Dict[str, Any]:
    """Извлекает метаданные из текста.
    
    Определяет:
    - Тип контента (FAQ, инструкция, справочник, процесс)
    - Категории/теги
    - Ключевые слова
    
    Args:
        text: Текст для анализа
        source: Источник документа (для определения типа)
    
    Returns:
        Словарь с метаданными:
        {
            "document_type": "faq" | "instruction" | "reference" | "process",
            "category": str,
            "tags": List[str],
            "keywords": List[str],
        }
    """
    text_lower = text.lower()
    
    # Определяем тип документа
    document_type = "reference"  # По умолчанию
    
    if source and "faq" in source.lower():
        document_type = "faq"
    elif any(word in text_lower for word in ["инструкция", "как сделать", "шаги", "процесс"]):
        document_type = "instruction"
    elif any(word in text_lower for word in ["процесс", "алгоритм", "последовательность"]):
        document_type = "process"
    elif any(word in text_lower for word in ["справочник", "справка", "описание"]):
        document_type = "reference"
    
    # Определяем категорию по ключевым словам
    category = "общее"
    category_keywords = {
        "авторизация": ["авторизация", "вход", "код доступа", "логин", "пароль"],
        "рассылки": ["рассылка", "broadcast", "уведомление"],
        "база знаний": ["база знаний", "документ", "добавить"],
        "вопросы": ["вопрос", "ответ", "faq"],
        "настройки": ["настройка", "конфигурация", "параметр"],
    }
    
    for cat, keywords in category_keywords.items():
        if any(kw in text_lower for kw in keywords):
            category = cat
            break
    
    # Извлекаем теги (ключевые слова из текста)
    tags = []
    tag_patterns = [
        r'\b(авторизация|вход|код|пароль|логин)\b',
        r'\b(рассылка|broadcast|уведомление)\b',
        r'\b(база знаний|документ|добавить)\b',
        r'\b(вопрос|ответ|faq)\b',
        r'\b(настройка|конфигурация)\b',
    ]
    
    for pattern in tag_patterns:
        matches = re.findall(pattern, text_lower, re.IGNORECASE)
        tags.extend([m.lower() if isinstance(m, str) else m[0].lower() for m in matches])
    
    tags = list(set(tags))[:5]  # Уникальные, максимум 5
    
    # Извлекаем ключевые слова (слова длиннее 4 символов, не стоп-слова)
    stop_words = {
        "это", "как", "что", "для", "когда", "где", "который", "которые",
        "можно", "нужно", "должен", "будет", "есть", "быть", "был", "была",
    }
    words = re.findall(r'\b[а-яё]{4,}\b', text_lower)
    keywords = [w for w in words if w not in stop_words and w not in tags]
    keywords = list(set(keywords))[:10]  # Уникальные, максимум 10
    
    return {
        "document_type": document_type,
        "category": category,
        "tags": tags,
        "keywords": keywords,
    }
