"""Поиск похожих вопросов среди FAQ (Google Sheets + OpenAI embeddings)."""

from typing import Optional, List, Dict
import math
import asyncio

from app.services.sheets_client import load_faq_rows
from app.services.openai_client import create_embedding


# -----------------------------
#   КЭШ ВОПРОСОВ И ЭМБЕДДИНГОВ
# -----------------------------

FAQ_DATA: List[Dict[str, str]] = []
FAQ_EMBEDS: List[List[float]] = []
CACHE_READY = False


async def load_faq_cache() -> None:
    """Загружает вопросы + ответы + эмбеддинги в память (один раз)."""
    global FAQ_DATA, FAQ_EMBEDS, CACHE_READY

    if CACHE_READY:
        return

    # Загружаем строки из таблицы
    rows = load_faq_rows()

    FAQ_DATA = rows
    FAQ_EMBEDS = []

    # Генерируем embedding для каждого вопроса
    for row in rows:
        emb = await asyncio.to_thread(create_embedding, row["question"])
        FAQ_EMBEDS.append(emb)

    CACHE_READY = True
    print(f"[FAQ] Загружено {len(FAQ_DATA)} записей FAQ.")


# -----------------------------
#    ПОИСК ПОХОЖЕГО ВОПРОСА
# -----------------------------

def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Косинусное сходство двух векторов."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


async def find_similar_question(user_question: str) -> Optional[Dict[str, str]]:
    """Возвращает {question, answer, score} или None, если ничего похожего нет."""

    # Убедимся, что кэш загружен
    await load_faq_cache()

    # Создаем embedding вопроса пользователя
    user_emb = await asyncio.to_thread(create_embedding, user_question)

    best_idx = None
    best_score = 0.0

    for idx, emb in enumerate(FAQ_EMBEDS):
        score = cosine_similarity(user_emb, emb)
        if score > best_score:
            best_idx = idx
            best_score = score

    # Если максимальное сходство слишком маленькое → похожего вопроса нет
    if best_idx is None or best_score < 0.82:   # порог можно регулировать
        return None

    return {
        "question": FAQ_DATA[best_idx]["question"],
        "answer": FAQ_DATA[best_idx]["answer"],
        "score": best_score,
    }

