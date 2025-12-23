import asyncio
import inspect
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from app.config import BOT_TOKEN, MANAGER_CHAT_ID
from app.services.report_service import build_daily_report


def _parse_chat_ids(raw: str) -> list[int]:
    if not raw:
        return []
    parts = [p.strip() for p in str(raw).split(",") if p.strip()]
    ids: list[int] = []
    for p in parts:
        try:
            ids.append(int(p))
        except Exception:
            continue
    return ids


async def _maybe_await(x):
    if inspect.isawaitable(x):
        return await x
    return x


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан. Проверь .env или Railway Variables.")
    chat_ids = _parse_chat_ids(MANAGER_CHAT_ID)
    if not chat_ids:
        raise RuntimeError("MANAGER_CHAT_ID не задан или не распознан. Укажи chat id (можно через запятую).")

    # Считаем “вчера” по МСК, чтобы дата в отчёте совпадала с ожиданиями команды
    tz = ZoneInfo("Europe/Moscow")
    target_date = (datetime.now(tz) - timedelta(days=1)).date()

    text = await _maybe_await(build_daily_report(target_date))

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    try:
        for cid in chat_ids:
            await bot.send_message(
                chat_id=cid,
                text=text,
                disable_web_page_preview=True,
            )
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())