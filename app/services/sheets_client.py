"""Клиент для чтения данных из Google Sheets."""

import json
from typing import List, Dict, Optional

import gspread
from google.oauth2.service_account import Credentials

from app.config import GOOGLE_SERVICE_ACCOUNT_JSON, SHEET_ID, SHEET_RANGE

# Синглтоны: один клиент с полным scope (чтение и запись), один только для чтения
_sheets_client_rw: Optional[gspread.Client] = None
_sheets_client_ro: Optional[gspread.Client] = None


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
    client = _get_client()
    sh = client.open_by_key(SHEET_ID)

    # Если SHEET_RANGE вида 'Sheet1'!C:D
    if "!" in SHEET_RANGE:
        sheet_name, rng = SHEET_RANGE.split("!", 1)
        sheet_name = sheet_name.strip().strip("'\"")
        ws = sh.worksheet(sheet_name)
        rows = ws.get(rng)
    else:
        ws = sh.sheet1
        rows = ws.get(SHEET_RANGE)

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

