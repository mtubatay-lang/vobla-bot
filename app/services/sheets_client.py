"""Клиент для чтения данных из Google Sheets."""

import json
import logging
import time
from typing import List, Dict, Optional

import gspread
from google.oauth2.service_account import Credentials

from app.config import GOOGLE_SERVICE_ACCOUNT_JSON, SHEET_ID, SHEET_RANGE

# Синглтоны: один клиент с полным scope (чтение и запись), один только для чтения
_sheets_client_rw: Optional[gspread.Client] = None
_sheets_client_ro: Optional[gspread.Client] = None
logger = logging.getLogger(__name__)


def _is_quota_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "429" in text or "quota exceeded" in text or "rate limit" in text


def run_with_retry_on_429(fn, retries: int = 3):
    """
    Выполняет fn() с retry на Google API quota/rate limit.
    Нужен для сервисов, где массовые чтения в Sheets могут дать 429.
    """
    global _sheets_client_ro, _sheets_client_rw
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:
            if not _is_quota_error(exc) or attempt >= retries:
                raise
            delay = min(2 ** (attempt - 1), 8)
            logger.warning(
                "Sheets 429/quota: retry %s/%s after %ss",
                attempt,
                retries,
                delay,
            )
            # Сбрасываем клиенты, чтобы пересоздать соединение на следующей попытке.
            _sheets_client_ro = None
            _sheets_client_rw = None
            time.sleep(delay)


def _get_client() -> gspread.Client:
    """Возвращает gspread-клиент с правами только на чтение (singleton)."""
    global _sheets_client_ro
    if _sheets_client_ro is not None:
        return _sheets_client_ro
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError(
            "GOOGLE_SERVICE_ACCOUNT_JSON не задан. "
            "Добавь JSON сервисного аккаунта в переменные окружения."
        )
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    _sheets_client_ro = gspread.authorize(creds)
    return _sheets_client_ro


def get_sheets_client() -> gspread.Client:
    """Возвращает gspread-клиент с правами на чтение и запись (singleton)."""
    global _sheets_client_rw
    if _sheets_client_rw is not None:
        return _sheets_client_rw
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError(
            "GOOGLE_SERVICE_ACCOUNT_JSON не задан. "
            "Добавь JSON сервисного аккаунта в переменные окружения."
        )
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    _sheets_client_rw = gspread.authorize(creds)
    return _sheets_client_rw


def load_faq_rows() -> List[Dict[str, str]]:
    """Загружает строки FAQ из Google Sheets.

    Ожидаем:
    - столбец C: вопрос
    - столбец D: ответ
    - столбец E: media_json (опционально)

    Диапазон задаем через SHEET_RANGE, например: 'Sheet1'!C:D или 'Sheet1'!C:E
    """
    def _load():
        client = _get_client()
        sh = client.open_by_key(SHEET_ID)

        # Если SHEET_RANGE вида 'Sheet1'!C:D
        if "!" in SHEET_RANGE:
            sheet_name, rng = SHEET_RANGE.split("!", 1)
            sheet_name = sheet_name.strip().strip("'\"")
            ws = sh.worksheet(sheet_name)
            return ws.get(rng)
        ws = sh.sheet1
        return ws.get(SHEET_RANGE)

    rows = run_with_retry_on_429(_load)

    result: List[Dict[str, str]] = []
    for row in rows:
        # row = [вопрос, ответ, media_json?]
        if not row or len(row) < 2:
            continue
        question = (row[0] or "").strip()
        answer = (row[1] or "").strip()
        media_json = (row[2] or "").strip() if len(row) > 2 else ""
        if question and answer:
            result.append({
                "question": question,
                "answer": answer,
                "media_json": media_json,
            })

    return result

