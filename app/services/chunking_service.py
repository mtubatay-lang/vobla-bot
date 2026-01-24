"""Сервис для разбивки текста на чанки."""

import re
from typing import List, Dict, Any
from app.config import CHUNK_SIZE, CHUNK_OVERLAP


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
