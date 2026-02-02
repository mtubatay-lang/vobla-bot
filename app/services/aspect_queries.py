"""Подзапросы по аспектам для multi-aspect retrieval (ответ «как весь документ»)."""

import logging
from typing import List

logger = logging.getLogger(__name__)

# Триггеры: при совпадении с вопросом включаем multi-aspect
MULTI_ASPECT_TRIGGERS = [
    "как выбрать",
    "как подобрать",
    "по каким критериям",
    "чек-лист",
    "чек лист",
    "место для магазина",
    "выбор месторасположения",
    "выбор помещения",
    "требования к помещению",
    "требования к месту",
    "критерии выбора",
]

# Максимум подзапросов и top_k на один аспект
MAX_ASPECT_QUERIES = 4
ASPECT_SEARCH_TOP_K = 6


# Карта: триггерная фраза (или ключ) -> список подзапросов по аспектам
# Используется первый совпавший набор
ASPECT_QUERIES_MAP = [
    # Вопросы про выбор места/помещения/магазина
    (
        ["место", "помещение", "месторасположение", "локация", "магазин"],
        [
            "чек-лист выбора месторасположения требования площадь этаж отдельный вход вывеска",
            "скоринг локаций трафик соседи баллы коммерческая сила",
            "полевая проверка документы арендодателя выписка правообладатель",
            "мини-чеклист выехать проверить 15 минут парковка разгрузка",
        ],
    ),
]


def should_use_multi_aspect(question: str) -> bool:
    """Решает, нужен ли multi-aspect для данного вопроса."""
    if not question or len(question.strip()) > 80:
        return False
    q = question.strip().lower()
    return any(trigger in q for trigger in MULTI_ASPECT_TRIGGERS)


def get_aspect_queries(question: str) -> List[str]:
    """
    Возвращает список подзапросов по аспектам (3–5 фраз) или пустой список.
    Вариант A: ключевые слова по карте триггеров.
    """
    if not should_use_multi_aspect(question):
        return []
    q = question.strip().lower()
    for keywords, queries in ASPECT_QUERIES_MAP:
        if any(kw in q for kw in keywords):
            chosen = queries[:MAX_ASPECT_QUERIES]
            logger.info(f"[ASPECT_QUERIES] Multi-aspect для вопроса: {len(chosen)} подзапросов")
            return chosen
    return []
