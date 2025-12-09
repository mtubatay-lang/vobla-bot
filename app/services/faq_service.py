"""Поиск похожих вопросов среди FAQ (Google Sheets + OpenAI embeddings)."""

from typing import Optional, List, Dict
import math
import asyncio
import re

from app.services.sheets_client import load_faq_rows
from app.services.openai_client import create_embedding


# -----------------------------
#   ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -----------------------------

def normalize(text: str) -> str:
    """Простая нормализация текста для эмбеддингов.

    - нижний регистр
    - убираем пунктуацию
    - схлопываем пробелы
    """
    text = text.lower()
    # оставляем буквы/цифры/пробелы (включая русские)
    text = re.sub(r"[^\w\sёЁа-яА-Я0-9]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


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

    rows = load_faq_rows()  # [{question, answer}, ...]

    FAQ_DATA = []
    FAQ_EMBEDS = []

    for row in rows:
        question = row["question"]
        answer = row["answer"]

        norm_question = normalize(question)

        emb = await asyncio.to_thread(create_embedding, norm_question)

        FAQ_DATA.append(
            {
                "question": question,
                "norm_question": norm_question,
                "answer": answer,
            }
        )
        FAQ_EMBEDS.append(emb)

    CACHE_READY = True
    print(f"[FAQ] Загружено {len(FAQ_DATA)} записей FAQ (эмбеддинги готовы).")


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

    await load_faq_cache()

    norm_user = normalize(user_question)
    user_emb = await asyncio.to_thread(create_embedding, norm_user)

    best_idx = None
    best_score = 0.0

    for idx, emb in enumerate(FAQ_EMBEDS):
        score = cosine_similarity(user_emb, emb)
        if score > best_score:
            best_idx = idx
            best_score = score

    # Порог похожести – можно потом подкрутить (0.65–0.75)
    THRESHOLD = 0.70

    if best_idx is None or best_score < THRESHOLD:
        return None

    data = FAQ_DATA[best_idx]
    return {
        "question": data["question"],
        "answer": data["answer"],
        "score": best_score,
    }