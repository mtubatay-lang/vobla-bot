"""Клиент для чтения данных из Google Sheets."""

import json
from typing import List, Dict

import gspread
from google.oauth2.service_account import Credentials

from app.config import GOOGLE_SERVICE_ACCOUNT_JSON, SHEET_ID, SHEET_RANGE


def _get_client() -> gspread.Client:
    """Создает gspread-клиент из JSON сервисного аккаунта."""
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError(
            "GOOGLE_SERVICE_ACCOUNT_JSON не задан. "
            "Добавь JSON сервисного аккаунта в переменные окружения."
        )

    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)

    return gspread.authorize(creds)


def get_sheets_client() -> gspread.Client:
    """Публичная функция для получения gspread-клиента с правами на чтение и запись."""
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError(
            "GOOGLE_SERVICE_ACCOUNT_JSON не задан. "
            "Добавь JSON сервисного аккаунта в переменные окружения."
        )

    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)

    # Права на чтение и запись (нужно для auth_service)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)

    return gspread.authorize(creds)


def load_faq_rows() -> List[Dict[str, str]]:
    """Загружает строки FAQ из Google Sheets.

    Ожидаем:
    - столбец C: вопрос
    - столбец D: ответ

    Диапазон задаем через SHEET_RANGE, например: 'Sheet1'!C:D
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
        # row = [вопрос, ответ]
        if not row or len(row) < 2:
            continue
        question = (row[0] or "").strip()
        answer = (row[1] or "").strip()
        if question and answer:
            result.append({"question": question, "answer": answer})

    return result

