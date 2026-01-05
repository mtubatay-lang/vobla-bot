"""Запись оценки навыка 'Ответы на вопросы' в Google Sheets (лист qa_feedback)."""

from datetime import datetime, timezone
from typing import Optional

from app.config import STATS_SHEET_ID, QA_FEEDBACK_SHEET_TAB
from app.services.sheets_client import get_sheets_client


def _now_ts_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _today_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def save_qa_feedback(
    *,
    session_id: str,
    user_id: int,
    username: Optional[str],
    helped: str,          # yes/partial/no
    completeness: int,    # 1..5
    clarity: int,         # 1..5
    comment: str = "",
    questions_count: int = 0,
    last_question: str = "",
    last_answer_source: str = "",  # faq/manager/none (пока можно пусто)
) -> None:
    if not STATS_SHEET_ID:
        return

    client = get_sheets_client()
    sh = client.open_by_key(STATS_SHEET_ID)
    ws = sh.worksheet(QA_FEEDBACK_SHEET_TAB)

    row = [
        _now_ts_iso(),
        _today_date(),
        session_id,
        str(user_id),
        username or "",
        helped,
        str(completeness),
        str(clarity),
        comment or "",
        str(questions_count),
        last_question or "",
        last_answer_source or "",
    ]

    ws.append_row(row, value_input_option="RAW")


