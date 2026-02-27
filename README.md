# Vobla Bot

Бот для партнёров Воблабир: авторизация, RAG (база знаний), рассылки, FAQ, kilbil.

## Запуск Telegram-бота

1. Установи зависимости: `pip install -r requirements.txt`
2. Настрой переменные окружения (см. раздел «Конфигурация»).
3. Запуск:
   ```bash
   python -m app.main
   ```
   Бот работает в режиме long polling.

## Запуск MAX (опционально)

Для приёма обновлений из мессенджера MAX запусти отдельный webhook-сервер:

1. Включи MAX и задай токен:
   ```bash
   export ENABLE_MAX=true
   export MAX_BOT_TOKEN=<токен_бота_MAX>
   ```
2. При необходимости укажи базовый URL API и путь webhook:
   ```bash
   export MAX_API_BASE_URL=https://platform-api.max.ru   # по умолчанию
   export MAX_WEBHOOK_PATH=/webhook/max
   export MAX_WEBHOOK_HOST=0.0.0.0
   export MAX_WEBHOOK_PORT=8080
   ```
3. Запуск webhook-сервера:
   ```bash
   python -m app.max_entrypoint
   ```
4. Настрой в панели MAX webhook URL: `https://<твой_домен><MAX_WEBHOOK_PATH>` (например `https://example.com/webhook/max`).

На webhook приходят обновления (сообщения и callback от кнопок). Обрабатываются команды `/start`, `/help` и авторизация по коду; остальные сценарии (RAG, рассылки и т.д.) пока только в Telegram.

## Конфигурация

### Обязательные переменные (для основного бота)

- `BOT_TOKEN` — токен Telegram-бота  
- `OPENAI_API_KEY` — ключ OpenAI  
- `SHEET_ID`, `USERS_SHEET_ID`, `GOOGLE_SERVICE_ACCOUNT_JSON` — Google Sheets  
- При необходимости: `STATS_SHEET_ID`, `MANAGER_CHAT_ID`, `KB_MANAGERS_CHAT_ID` и др. (см. `app/config.py`)

### MAX (только при `ENABLE_MAX=true`)

- `MAX_BOT_TOKEN` — токен бота в MAX  
- `MAX_API_BASE_URL` — базовый URL API (по умолчанию `https://platform-api.max.ru`)  
- `MAX_WEBHOOK_PATH` — путь для webhook (по умолчанию `/webhook/max`)

## Плановые рассылки

Запуск job для плановых рассылок (cron каждые 15–60 мин):

```bash
python -m app.jobs.scheduled_broadcasts
```

Используется тот же `execute_broadcast`, что и ручная рассылка; получатели берутся из Google Sheets (пока только Telegram).

## Ветка мультиплатформы

Работа с Telegram и MAX ведётся в ветке `feature/multi-messenger`. После проверки сценариев в Telegram и при необходимости в MAX изменения мержатся в `main`.
