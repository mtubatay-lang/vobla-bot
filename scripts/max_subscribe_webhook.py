#!/usr/bin/env python3
"""
Регистрация webhook в MAX: POST /subscriptions.

Документация: https://dev.max.ru/docs-api/methods/POST/subscriptions

Перед запуском:
  1. Подними отдельный сервис с `python -m app.max_entrypoint` и публичным HTTPS (Railway и т.п.).
  2. Задай переменные окружения (или .env в корне проекта).

Пример:
  export MAX_BOT_TOKEN=<токен_из_кабинета_MAX>
  export MAX_WEBHOOK_PUBLIC_BASE=https://your-service.up.railway.app
  python scripts/max_subscribe_webhook.py

Полный URL вместо base+path:
  python scripts/max_subscribe_webhook.py --url https://your-service.up.railway.app/webhook/max

При 401 попробуй:
  export MAX_AUTH_BEARER_PREFIX=false
  python scripts/max_subscribe_webhook.py ...
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import requests

# Загрузка .env из корня репозитория (как в app.config)
try:
    from dotenv import load_dotenv

    _root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    load_dotenv(os.path.join(_root, ".env"))
except ImportError:
    pass

DEFAULT_UPDATE_TYPES = [
    "message_created",
    "message_callback",
    "bot_started",
]


def _auth_header(token: str, use_bearer_prefix: bool) -> str:
    t = token.strip()
    if t.lower().startswith("bearer "):
        return t
    if use_bearer_prefix:
        return f"Bearer {t}"
    return t


def main() -> int:
    parser = argparse.ArgumentParser(description="MAX: POST /subscriptions (webhook)")
    parser.add_argument(
        "--url",
        help="Полный HTTPS URL webhook (например https://host/webhook/max)",
    )
    parser.add_argument(
        "--public-base",
        default=os.getenv("MAX_WEBHOOK_PUBLIC_BASE", ""),
        help="База без пути; к ней добавится MAX_WEBHOOK_PATH (env или /webhook/max)",
    )
    parser.add_argument(
        "--path",
        default=os.getenv("MAX_WEBHOOK_PATH", "/webhook/max"),
        help="Путь webhook, если задан только --public-base",
    )
    parser.add_argument(
        "--api-base",
        default=os.getenv("MAX_API_BASE_URL", "https://platform-api.max.ru").rstrip("/"),
    )
    parser.add_argument(
        "--secret",
        default=os.getenv("MAX_SUBSCRIPTION_SECRET", os.getenv("MAX_WEBHOOK_SECRET", "")),
        help="Опционально: secret для заголовка X-Max-Bot-Api-Secret (5–256 символов, [a-zA-Z0-9_-])",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Только GET /subscriptions (текущие подписки), без POST",
    )
    parser.add_argument(
        "--no-bearer",
        action="store_true",
        help="Authorization: только токен, без префикса Bearer",
    )
    args = parser.parse_args()

    token = os.getenv("MAX_BOT_TOKEN", "")
    if not token:
        print("MAX_BOT_TOKEN не задан", file=sys.stderr)
        return 1

    use_bearer = os.getenv("MAX_AUTH_BEARER_PREFIX", "false").lower() == "true"
    if args.no_bearer:
        use_bearer = False

    headers = {
        "Authorization": _auth_header(token, use_bearer),
        "Content-Type": "application/json",
    }

    if args.list:
        r = requests.get(f"{args.api_base}/subscriptions", headers=headers, timeout=60)
        print(r.status_code, r.text)
        return 0 if r.ok else 1

    if args.url:
        webhook_url = args.url.strip()
    elif args.public_base:
        base = args.public_base.rstrip("/")
        path = args.path if args.path.startswith("/") else f"/{args.path}"
        webhook_url = f"{base}{path}"
    else:
        print("Укажи --url или MAX_WEBHOOK_PUBLIC_BASE (или --public-base)", file=sys.stderr)
        return 1

    if not webhook_url.startswith("https://"):
        print("URL должен начинаться с https:// (требование MAX)", file=sys.stderr)
        return 1

    payload: dict = {
        "url": webhook_url,
        "update_types": DEFAULT_UPDATE_TYPES,
    }
    if args.secret:
        payload["secret"] = args.secret

    r = requests.post(
        f"{args.api_base}/subscriptions",
        headers=headers,
        data=json.dumps(payload),
        timeout=60,
    )
    print(f"POST {args.api_base}/subscriptions -> {r.status_code}")
    print(r.text or r.content.decode("utf-8", errors="replace"))
    if r.status_code == 401:
        print(
            "\nПодсказка: при 401 попробуй MAX_AUTH_BEARER_PREFIX=false или флаг --no-bearer",
            file=sys.stderr,
        )
    return 0 if r.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
