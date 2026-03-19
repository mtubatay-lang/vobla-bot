# Vobla Bot

Бот для партнёров Воблабир: авторизация, RAG (база знаний), рассылки, FAQ, kilbil.

## Функции бота

### Для всех пользователей

- **Старт и авторизация** — `/start` показывает кнопку «Авторизация»; `/login` — ввод одноразового кода доступа. Пользователи хранятся в Google Sheets (`USERS_SHEET_ID`), привязка по Telegram ID.
- **Помощь** — `/help`: для авторизованных — подсказки по командам и кнопкам; для неавторизованных — инструкция по авторизации.

### Вопросы и база знаний

- **Режим навыка «Задать вопрос»** — `/ask` или кнопка «❓ Задать вопрос». Последовательные вопросы по базе знаний (RAG): векторный поиск в Qdrant, опционально HyDE, гибрид BM25, ререйнкинг (LLM или Cohere). При отсутствии ответа — тикет в чат менеджеров. После ответа — сбор обратной связи (помог/частично/не помог, звёзды, комментарий).
- **FAQ** — `/faq`: пользователь вводит вопрос; поиск похожего в базе FAQ (Google Sheets + кэш в Qdrant), при необходимости адаптация ответа через LLM. Если ответ не найден — создаётся тикет для менеджера.
- **Kilbil** — `/kilbil`: вопросы по платформе kilbil; поиск по базе знаний help.kilbil.ru (локальный инжест), ответ + ссылка на статью.
- **Групповой чат RAG** — в группах/супергруппах бот реагирует на вопросы (в т.ч. по ключевым словам тем базы). Тот же пайплайн RAG (Qdrant, ререйнк, проверка grounding). При низком скоре — молчит или короткое «не нашёл» (настраивается). Менеджеры могут ответить reply’ем; бот предлагает записать пару вопрос–ответ в базу с подтверждением в отдельном чате (`KB_MANAGERS_CHAT_ID`).

### Голосовые сообщения

- **Голос → текст** — голосовые сообщения транскрибируются через OpenAI Whisper; опционально структурирование ответа (резюме, блоки, чеклист). Лимит размера файла 25 МБ. Включается/выключается через `VOICE_TO_TEXT_ENABLED`.

### Для менеджеров

- **Ответы на тикеты** — в чат менеджеров (`MANAGER_CHAT_ID`) приходят вопросы без ответа (из FAQ и из режима /ask). Кнопка «✍️ Ответить» открывает ввод ответа; поддержка текста, фото, видео, альбомов. Ответ уходит пользователю; опционально запись пары вопрос–ответ в FAQ (Google Sheets + Qdrant).

### Для администраторов (/admin)

- **Рассылки** — `/broadcast` или кнопка «📢 Запуск рассылки»: ввод текста (и опционально медиа), улучшение текста через LLM, выбор аудитории (тест себе / пользователи / чаты / оба), сегментация по регионам или по чатам. Отправка сейчас или планирование (еженедельно/ежемесячно). Получатели — из Google Sheets (`recipients_users`, `recipients_chats`).
- **Плановые рассылки** — список активных плановых рассылок, отключение. Фактическая отправка выполняется job’ом `scheduled_broadcasts` (cron).
- **Пополнение базы знаний** — загрузка документов (PDF, TXT, DOCX, MD, CSV): предподготовка, извлечение текста, чанкинг (structure-aware/semantic), обогащение, эмбеддинги в Qdrant. Команда `/kb_migrate` — миграция FAQ из Google Sheets в Qdrant.
- **Создание документов** — генерация DOCX из шаблонов (договоры и т.п.): выбор шаблона, пошаговый ввод полей (номер, даты, реквизиты заказчика). Реквизиты можно отправить текстом, фото/скриншотом или PDF/DOCX — извлечение полей через AI/Vision. Добавление новых шаблонов через «➕ Добавить шаблон».

### Фоновые процессы

- **Сбор получателей рассылок** — при добавлении бота в группу чат попадает в `recipients_chats`; при любом сообщении в личку/группе обновляются данные пользователя/чата в Google Sheets. Обработчик не отвечает пользователю, только пишет в таблицы.
- **Ежедневный отчёт** — job `daily_report`: сводка за вчера (по UTC) по статистике бота, отправка в `MANAGER_CHAT_ID`.
- **Ежемесячный отчёт** — job `monthly_report`: сводка за прошлый месяц, отправка в `MANAGER_CHAT_ID`.

## Запуск Telegram-бота

1. Установи зависимости: `pip install -r requirements.txt`
2. Настрой переменные окружения (см. раздел «Конфигурация»).
3. Запуск:
   ```bash
   python -m app.main
   ```
   Бот работает в режиме long polling.

## Запуск MAX (опционально)

MAX работает через **HTTPS webhook** к отдельному процессу (не через Telegram long polling). Реализация соответствует [документации API MAX](https://dev.max.ru/docs-api): разбор `update_type`, `POST /messages`, `POST /answers`, inline-клавиатура через `attachments`.

### Важно: два процесса

- **Telegram:** `python -m app.main` (как в [Procfile](Procfile)).
- **MAX webhook:** `python -m app.max_entrypoint` — запускай **отдельным сервисом** (например второй сервис на Railway) с публичным HTTPS URL.

Логи вида `aiogram.event: ... bot id=...` относятся **только к Telegram**. Для MAX смотри логи процесса с `uvicorn` / `MAX webhook`.

Если в логах **TelegramConflictError: terminated by other getUpdates request** — одновременно запущены **два** экземпляра с тем же `BOT_TOKEN` (например локальный `python -m app.main` и Railway). Останови лишний, иначе polling конфликтует.

Если **POST /webhook/max** с кодом **422** — это баг разбора аннотаций FastAPI у вложенного handler (исправлено в `app/max_entrypoint.py`): задеплой свежий коммит.

**Лимит Google Sheets (429 / Quota exceeded):** при большом числе запросов к таблицам (статистика, рассылки, лист получателей) API может вернуть 429. Запись в `bot_stats` через `log_event` **не роняет** сценарии RAG/QA — в логах будет предупреждение; при необходимости увеличь квоту проекта в Google Cloud или снизь частоту обращений к Sheets.

**Повторный запрос кода в MAX после кнопки «Авторизация»:** если `user_id` в webhook приходит строкой, ключ ожидания кода должен совпадать (исправлено в `app/core/handlers.py` — нормализация в int).

**MAX `POST /answers` и 400:** пустой JSON `{}` с `Content-Type: application/json` API может отклонять — клиент шлёт запрос **без тела**, если нет `notification`.

**Кнопка «Задать вопрос» в Telegram «зависает»:** callback нужно подтвердить быстро (~10 с); в `qa_mode` после проверки авторизации вызывается `cb.answer()` до тяжёлой работы.

### Локально

1. Включи MAX и задай токен:
   ```bash
   export ENABLE_MAX=true
   export MAX_BOT_TOKEN=<токен_бота_MAX>
   ```
2. При необходимости:
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

### Railway

**Вариант A — один сервис (текущий [Procfile](Procfile)):** процесс `worker` запускает `python -m app.max_entrypoint` в фоне (uvicorn на `PORT`) и затем `python -m app.main`. Нужны публичный домен и включённый **Public Networking**, иначе MAX не достучится до webhook. Домен можно сгенерировать в Railway (или через MCP `generate-domain` для связанного сервиса).

**Вариант B — второй сервис только под MAX (предпочтительно для продакшена):**

1. Создай **новый** сервис в том же проекте Railway (не тот, где Telegram).
2. Подключи тот же репозиторий; **Start Command:** `python -m app.max_entrypoint` (и в [Procfile](Procfile) верни строку `worker: python -m app.main` для Telegram-сервиса).
3. Переменные окружения: `ENABLE_MAX=true`, `MAX_BOT_TOKEN`, те же секреты, что у Telegram-сервиса (импорт `app.config` требует `BOT_TOKEN` и др.). Railway подставит **`PORT`**.
4. **Settings → Networking → Generate Domain** — HTTPS URL вида `https://…up.railway.app`.

### Подписка на обновления (POST /subscriptions)

Оформляется вызовом API MAX после того, как webhook-сервис уже доступен по HTTPS. Формат тела — в [документации POST /subscriptions](https://dev.max.ru/docs-api/methods/POST/subscriptions): `url`, `update_types`, опционально `secret` (тогда на сервере задай тот же секрет в **`MAX_WEBHOOK_SECRET`** — бот сверяет заголовок `X-Max-Bot-Api-Secret`).

Из корня репозитория:

```bash
export MAX_BOT_TOKEN=<токен_из_кабинета_MAX>
export MAX_WEBHOOK_PUBLIC_BASE=https://<твой-railway-домен>
python scripts/max_subscribe_webhook.py
```

Либо полный URL одной строкой:

```bash
python scripts/max_subscribe_webhook.py --url https://<домен>/webhook/max
```

Проверить текущие подписки: `python scripts/max_subscribe_webhook.py --list`

По умолчанию в коде **`MAX_AUTH_BEARER_PREFIX=false`** — заголовок **`Authorization: <token>`** (как в примерах [dev.max.ru/docs-api](https://dev.max.ru/docs-api)). Скрипт подписки без флагов использует то же из env. Если API отвечает **401**, задай **`MAX_AUTH_BEARER_PREFIX=true`** или запусти скрипт **без** `--no-bearer`.

### Поведение в MAX

- Обрабатываются: событие **`bot_started`** (как `/start`), текстовые **`/start`**, **`/help`** (в т.ч. с суффиксом `@botname`), нажатие «Авторизация» и ввод кода после неё.
- После авторизации кнопка **«❓ Задать вопрос»** (`qa_start`): упрощённый **одношаговый RAG** по Qdrant (см. `app/services/max_qa_simple.py`), кнопка **«Завершить навык»** (`qa_exit`). Полный сценарий QA с FSM — в Telegram.
- **`POST /answers`**: клиент перебирает варианты тела запроса (без тела, `{}`, `notification: ""` и т.д.) — у разных версий API MAX требования различаются.
- **Меню команд как в Telegram** (`setMyCommands`) в MAX **не выставляется** — отдельного аналога в используемом API нет.
- Рассылки, админка и часть сценариев по-прежнему только в Telegram.

Если в логах всё ещё виден старый стек (`answer_callback` → `_request` с `json=body` на одной строке с прежним номером) — на Railway не задеплоен последний коммит; сделай **Redeploy**.

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
- `MAX_AUTH_BEARER_PREFIX` — `false` (по умолчанию): только токен в `Authorization`; `true` — префикс `Bearer ` (если без него API отвечает 401)  
- `MAX_WEBHOOK_SECRET` — если задан при подписке с `secret`, входящие запросы без совпадающего `X-Max-Bot-Api-Secret` отклоняются (403)  
- Для скрипта подписки: `MAX_WEBHOOK_PUBLIC_BASE` — HTTPS-база Railway без пути (к `MAX_WEBHOOK_PATH` он добавится)

## Плановые рассылки

Запуск job для плановых рассылок (cron каждые 15–60 мин):

```bash
python -m app.jobs.scheduled_broadcasts
```

Используется тот же `execute_broadcast`, что и ручная рассылка; получатели берутся из Google Sheets (пока только Telegram).

**Отчёты в чат менеджеров:**

- Ежедневный отчёт (за вчера по UTC): `python -m app.jobs.daily_report`
- Ежемесячный отчёт (за прошлый месяц): `python -m app.jobs.monthly_report`

## Ветка мультиплатформы

Работа с Telegram и MAX ведётся в ветке `feature/multi-messenger`. После проверки сценариев в Telegram и при необходимости в MAX изменения мержатся в `main`.
