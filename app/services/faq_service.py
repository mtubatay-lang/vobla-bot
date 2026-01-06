"""Поиск похожих вопросов среди FAQ (Google Sheets + OpenAI embeddings + AI rerank).
Поддерживает hot-update кэша: добавление новых FAQ без редеплоя.
"""

from typing import Optional, List, Dict, Any
import math
import asyncio
import re
import time

from app.services.sheets_client import load_faq_rows
from app.services.openai_client import create_embedding, choose_best_faq_answer


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


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Косинусное сходство двух векторов."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


# -----------------------------
#   КЭШ ВОПРОСОВ И ЭМБЕДДИНГОВ
# -----------------------------

FAQ_DATA: List[Dict[str, str]] = []
FAQ_EMBEDS: List[List[float]] = []
CACHE_READY = False

# защита от гонок, когда одновременно отвечают 2 менеджера
FAQ_LOCK = asyncio.Lock()

# чтобы не дёргать Google Sheets на каждый вопрос
_LAST_REFRESH_TS = 0.0
AUTO_REFRESH_EVERY_SEC = 120  # можно 60/300 на твой вкус


async def load_faq_cache(force: bool = False) -> None:
    """Загружает вопросы + ответы + эмбеддинги в память.
    По умолчанию 1 раз, но можно force=True.
    """
    global FAQ_DATA, FAQ_EMBEDS, CACHE_READY, _LAST_REFRESH_TS

    if CACHE_READY and not force:
        return

    async with FAQ_LOCK:
        if CACHE_READY and not force:
            return

        rows = load_faq_rows()  # [{question, answer}, ...]

        FAQ_DATA = []
        FAQ_EMBEDS = []

        for row in rows:
            question = (row.get("question") or "").strip()
            answer = (row.get("answer") or "").strip()
            if not question or not answer:
                continue

            norm_question = normalize(question)
            emb = await asyncio.to_thread(create_embedding, norm_question)

            media_json = (row.get("media_json") or "").strip()
            FAQ_DATA.append(
                {
                    "question": question,
                    "norm_question": norm_question,
                    "answer": answer,
                    "media_json": media_json,
                }
            )
            FAQ_EMBEDS.append(emb)

        CACHE_READY = True
        _LAST_REFRESH_TS = time.time()
        print(f"[FAQ] Загружено {len(FAQ_DATA)} записей FAQ (эмбеддинги готовы).")


async def add_faq_entry_to_cache(question: str, answer: str, media_json: str = "") -> None:
    """Добавляет одну новую пару Q/A в in-memory кэш (без перечитывания всего Sheet)."""
    global CACHE_READY

    question = (question or "").strip()
    answer = (answer or "").strip()
    media_json = (media_json or "").strip()
    if not question or not answer:
        return

    # если кэш еще не загружен — загрузим полностью (чтобы структура была консистентной)
    if not CACHE_READY:
        await load_faq_cache()
        return

    norm_question = normalize(question)
    emb = await asyncio.to_thread(create_embedding, norm_question)

    async with FAQ_LOCK:
        # защита от дублей (если кто-то добавил вручную и менеджер тоже)
        for item in FAQ_DATA[-50:]:  # дешёвая проверка хвоста
            if item["norm_question"] == norm_question and item["answer"] == answer:
                return

        FAQ_DATA.append(
            {
                "question": question,
                "norm_question": norm_question,
                "answer": answer,
                "media_json": media_json,
            }
        )
        FAQ_EMBEDS.append(emb)

    print(f"[FAQ] Добавлена новая запись в кэш. Всего: {len(FAQ_DATA)}")


async def refresh_faq_cache_from_sheet_if_needed(force: bool = False) -> None:
    """Периодически подтягивает новые строки из Google Sheets и добавляет их в кэш.
    Это страховка: если кто-то дописал FAQ вручную или кэш не обновился.
    """
    global _LAST_REFRESH_TS

    now = time.time()
    if not force and (now - _LAST_REFRESH_TS) < AUTO_REFRESH_EVERY_SEC:
        return

    await load_faq_cache()  # гарантируем, что кэш есть

    rows = load_faq_rows()
    _LAST_REFRESH_TS = now

    async with FAQ_LOCK:
        current_len = len(FAQ_DATA)

    # если в таблице стало больше строк — добавим только новые (хвост)
    if len(rows) <= current_len:
        return

    new_rows = rows[current_len:]
    for row in new_rows:
        q = (row.get("question") or "").strip()
        a = (row.get("answer") or "").strip()
        if not q or not a:
            continue
        await add_faq_entry_to_cache(q, a)


# -----------------------------
#    ПОИСК ПОХОЖЕГО ВОПРОСА
# -----------------------------

async def find_similar_question(user_question: str) -> Optional[Dict[str, Any]]:
    """Возвращает {question, answer, score} или None, если ничего похожего нет.

    Этап 1: эмбеддинги → выбираем топ-K по cosine similarity.
    Этап 2: передаём кандидатов в GPT, он решает, что лучше подходит
            (или что лучше ничего не отвечать).
    """
    await load_faq_cache()
    await refresh_faq_cache_from_sheet_if_needed()

    async with FAQ_LOCK:
        if not FAQ_DATA:
            return None
        local_data = list(FAQ_DATA)
        local_embeds = list(FAQ_EMBEDS)

    # Этап 1: эмбеддинг пользователя + базовый фильтр по cosine
    norm_user = normalize(user_question)
    user_emb = await asyncio.to_thread(create_embedding, norm_user)

    scores: List[float] = []
    for emb in local_embeds:
        scores.append(cosine_similarity(user_emb, emb))

    BASE_THRESHOLD = 0.55

    indexed_scores = [
        (idx, score) for idx, score in enumerate(scores) if score >= BASE_THRESHOLD
    ]

    if not indexed_scores:
        return None

    indexed_scores.sort(key=lambda x: x[1], reverse=True)
    TOP_K = 5
    top_indices = [idx for idx, _ in indexed_scores[:TOP_K]]

    candidates: List[Dict[str, Any]] = []
    for idx in top_indices:
        data = local_data[idx]
        candidates.append(
            {
                "question": data["question"],
                "answer": data["answer"],
                "score": scores[idx],
                "media_json": data.get("media_json", ""),
            }
        )

    best = await asyncio.to_thread(
        choose_best_faq_answer,
        user_question,
        candidates,
    )
    return best