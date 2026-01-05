import asyncio
from datetime import datetime, timezone, timedelta

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from app.config import BOT_TOKEN, MANAGER_CHAT_ID
from app.services.report_service import build_daily_report


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")
    if not MANAGER_CHAT_ID:
        raise RuntimeError("MANAGER_CHAT_ID не задан")

    # Берём "вчера" по UTC (согласовано с тем, как пишется колонка date в bot_stats)
    target = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    text = build_daily_report(target)

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    try:
        await bot.send_message(
            chat_id=int(MANAGER_CHAT_ID),
            text=text,
            disable_web_page_preview=True,
        )
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
