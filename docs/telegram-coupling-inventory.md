# Реестр привязки к Telegram/aiogram (для мультиплатформы)

Документ фиксирует все места сильной привязки к Telegram/aiogram. Используется при рефакторинге под Telegram + MAX.

## main.py

- `Bot`, `Dispatcher`, `BaseMiddleware`, `DefaultBotProperties`, `ParseMode`, `MemoryStorage`, `BotCommand`, `Update`
- Создание `Bot(token=BOT_TOKEN, ...)`, `Dispatcher(storage=MemoryStorage())`
- `dp.include_router(...)` — все роутеры
- `bot.set_my_commands(...)`
- `bot.delete_webhook(drop_pending_updates=True)`
- `dp.start_polling(bot)`
- CommandLoggingMiddleware — обращение к `event.text`, `event.from_user.id`

## handlers (по файлам)

| Файл | Типы/импорты | Использование |
|------|--------------|---------------|
| start.py | Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ChatAction | pending_auth dict по tg_id, message.from_user.id, callback.message.answer, bind_telegram_id |
| help.py | Router, Command, Message | find_user_by_telegram_id(tg_id), main_menu_kb(), message.answer |
| auth_handler.py | Router, Command, Message, FSMContext, StatesGroup, State | AuthState.waiting_for_code, find_user_by_telegram_id, bind_telegram_id, message.answer, reply_markup |
| qa_mode.py | Router, F, Command, CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton, FSMContext, StatesGroup, State, ParseMode, ChatAction | QAMode, FeedbackState, find_user_by_telegram_id, message.bot.send_chat_action, callback.message.answer, reply_markup, _send_media_from_json(bot, chat_id, ...) |
| group_chat_qa.py | Router, F, FSMContext, State, StatesGroup, Message | find_user_by_telegram_id, bot.send_message(KB_MANAGERS_CHAT_ID), callback.data, InlineKeyboardButton |
| faq.py | Router, F, Command, Message, InlineKeyboardMarkup, InlineKeyboardButton, ChatAction, ParseMode | find_user_by_telegram_id, message.bot.send_chat_action, send_media_group, send_document, send_message(MANAGER_CHAT_ID), reply_markup |
| manager_reply.py | Router, F, CallbackQuery, Message, ForceReply, InputMediaPhoto, InputMediaVideo, TelegramForbiddenError, TelegramBadRequest | MANAGER_CHAT_ID, reply_to_message, callback.message, message.bot.send_message |
| broadcast.py | Router, F, ParseMode, ChatAction, TelegramForbiddenError, TelegramBadRequest, Command, FSMContext, State, StatesGroup, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, InputMediaPhoto, InputMediaVideo | BroadcastState, find_user_by_telegram_id, execute_broadcast(bot, ...), message.bot.send_message, callback.message.answer, reply_markup |
| knowledge_base_admin.py | Router, F, Command, FSMContext, StatesGroup, State, Message, CallbackQuery | find_user_by_telegram_id, message.document, message.bot.get_file, message.bot.download_file, cb.message.answer |
| kilbil.py | Router, F, Command, BaseFilter, Message, ChatAction | find_user_by_telegram_id, PendingKilbilFilter(message), message.bot.send_chat_action, message.answer |
| voice_to_text.py | Router, F, Message | message.voice, message.bot.download(voice.file_id) |
| recipients_collector.py | Router, SkipHandler, ChatMemberUpdated, Message | event.chat.type, message.chat.type, message.from_user.id, upsert_user_recipient(user_id), upsert_chat_recipient(chat_id) |
| echo.py | Router, F, Message | message.answer(message.text) |
| debug_passthrough.py | (дебаг) | — |

## ui/keyboards.py

- `InlineKeyboardMarkup`, `InlineKeyboardButton` из aiogram.types
- `main_menu_kb()` → InlineKeyboardMarkup с callback_data "qa_start"
- `qa_kb()` → InlineKeyboardMarkup с callback_data "qa_exit"

## services с привязкой к Telegram

- **auth_service**: User.telegram_id, find_user_by_telegram_id, bind_telegram_id — колонка 5 листа «Пользователи»
- **broadcast_service**: execute_broadcast(bot, ...), bot.send_message, send_media_to_recipient(bot, ...), TelegramForbiddenError
- **broadcast_recipients_service**: user_id, chat_id (без поля platform)
- **pending_questions_service**: create_ticket_and_notify_managers(message: Message), message.bot.send_message(MANAGER_CHAT_ID), InlineKeyboardMarkup, InlineKeyboardButton

## jobs

- **scheduled_broadcasts.py**: Bot, DefaultBotProperties, ParseMode, execute_broadcast(bot, ...)
- **daily_report.py**, **monthly_report.py**: Bot, MANAGER_CHAT_ID, bot.send_message(chat_id=MANAGER_CHAT_ID)

## Конфиг

- BOT_TOKEN, MANAGER_CHAT_ID, KB_MANAGERS_CHAT_ID — идентификаторы Telegram.
