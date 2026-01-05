"""Сервис для чтения qa_feedback (для отчётов)."""

from typing import Any, Dict, List, Optional

from app.config import STATS_SHEET_ID, QA_FEEDBACK_SHEET_TAB
from app.services.sheets_client import get_sheets_client


def read_qa_feedback_by_dates(date_from: str, date_to: str) -> List[Dict[str, Any]]:
    """
    Читает оценки из qa_feedback за диапазон включительно.
    date_from/date_to: 'YYYY-MM-DD'

    Возвращает список dict:
      {ts, date, session_id, user_id, username, helped, completeness, clarity, comment, questions_count, last_question, last_answer_source}
    """
    if not STATS_SHEET_ID:
        return []

    client = get_sheets_client()
    sh = client.open_by_key(STATS_SHEET_ID)
    ws = sh.worksheet(QA_FEEDBACK_SHEET_TAB)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers) if h}

    def col(name: str) -> Optional[int]:
        return idx.get(name)

    ts_i = col("ts")
    d_i = col("date")
    session_id_i = col("session_id")
    uid_i = col("user_id")
    uname_i = col("username")
    helped_i = col("helped")
    completeness_i = col("completeness")
    clarity_i = col("clarity")
    comment_i = col("comment")
    questions_count_i = col("questions_count")
    last_question_i = col("last_question")
    last_answer_source_i = col("last_answer_source")

    # Если основные колонки не найдены, возвращаем пустой список
    required = [ts_i, d_i, session_id_i, uid_i, helped_i, completeness_i, clarity_i]
    if any(i is None for i in required):
        return []

    out: List[Dict[str, Any]] = []
    for r in values[1:]:
        def get(i: int) -> str:
            return (r[i] if i < len(r) else "").strip()

        d_raw = get(d_i)
        if not d_raw:
            continue

        # нормализуем дату в ISO day: YYYY-MM-DD
        d = d_raw[:10].strip()

        if d < date_from or d > date_to:
            continue

        # Парсим helped с нормализацией (поддержка русских и английских значений)
        helped_raw = get(helped_i).strip().lower()
        helped = helped_raw
        if helped_raw in ("помог", "да", "yes", "helped"):
            helped = "helped"
        elif helped_raw in ("частично", "partial"):
            helped = "partial"
        elif helped_raw in ("не помог", "нет", "no", "not_helped", "not helped"):
            helped = "not_helped"

        # Парсим completeness и clarity в int
        completeness_str = get(completeness_i)
        completeness: Optional[int] = None
        if completeness_str:
            try:
                completeness = int(completeness_str)
            except Exception:
                pass

        clarity_str = get(clarity_i)
        clarity: Optional[int] = None
        if clarity_str:
            try:
                clarity = int(clarity_str)
            except Exception:
                pass

        questions_count_str = get(questions_count_i) if questions_count_i is not None else ""
        questions_count: Optional[int] = None
        if questions_count_str:
            try:
                questions_count = int(questions_count_str)
            except Exception:
                pass

        out.append(
            {
                "ts": get(ts_i),
                "date": d,
                "session_id": get(session_id_i),
                "user_id": get(uid_i),
                "username": get(uname_i) if uname_i is not None else "",
                "helped": helped,
                "completeness": completeness,
                "clarity": clarity,
                "comment": get(comment_i) if comment_i is not None else "",
                "questions_count": questions_count,
                "last_question": get(last_question_i) if last_question_i is not None else "",
                "last_answer_source": get(last_answer_source_i) if last_answer_source_i is not None else "",
            }
        )

    return out

