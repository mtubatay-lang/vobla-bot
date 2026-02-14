#!/usr/bin/env python3
"""Полная индексация базы знаний help.kilbil.ru в Qdrant RAG.

Использование:
    python scripts/ingest_kilbil_help.py          # продолжить с сохранённого прогресса или начать заново
    python scripts/ingest_kilbil_help.py --fresh  # принудительно заново (обход + полная переиндексация)

Прогресс сохраняется в data/kilbil_ingest_progress.json.
Требует: OPENAI_API_KEY, QDRANT_URL (и QDRANT_API_KEY при необходимости).
"""

import argparse
import asyncio
import logging
import os
import sys

# Добавляем корень проекта в path
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def _run() -> dict:
    parser = argparse.ArgumentParser(description="Индексация help.kilbil.ru в Qdrant RAG")
    parser.add_argument("--fresh", action="store_true", help="Начать заново (обход + полная переиндексация)")
    args = parser.parse_args()

    from app.services.kilbil_ingest_service import run_ingestion

    async def progress(stage: str, detail: str) -> None:
        print(f"[KILBIL] {stage}: {detail}")

    return await run_ingestion(fresh=args.fresh, progress_callback=progress)


def main() -> None:
    result = asyncio.run(_run())
    print(f"Готово: {result['chunks']} чанков из {result['articles']} статей")
    if result.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
