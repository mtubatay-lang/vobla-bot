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
- **Групповой чат RAG** — в группах **Telegram** и **MAX** бот слушает чат, фильтрует шум, определяет вопрос и использует тот же пайплайн RAG (Qdrant, ререйнк, grounding). В личке MAX — отдельно навык `/ask` (упрощённый RAG). **`RAG_TEST_CHAT_ID`**: не задана в env — дефолтный тестовый Telegram-чат; **пустая строка** — все группы; иначе id одной группы (Telegram или MAX). Reply менеджеров и запись в базу — как в Telegram; см. `KB_MANAGERS_CHAT_ID`, `RAG_SEND_NO_ANSWER_REPLY`.

### Голосовые сообщения

- **Голос → текст** — в Telegram и **MAX** голосовые/аудио-вложения транскрибируются через OpenAI Whisper; опционально структурирование ответа (резюме, блоки, чеклист). Лимит размера файла 25 МБ. Включается/выключается через `VOICE_TO_TEXT_ENABLED`.

### Для менеджеров

- **Ответы на тикеты** — в чат менеджеров (`MANAGER_CHAT_ID`) приходят вопросы без ответа (из FAQ и из режима /ask). Кнопка «✍️ Ответить» открывает ввод ответа; поддержка текста, фото, видео, альбомов. Ответ уходит пользователю; опционально запись пары вопрос–ответ в FAQ (Google Sheets + Qdrant).

### Для администраторов (/admin)

- **Рассылки** — `/broadcast` или кнопка «📢 Запуск рассылки»: ввод текста (и опционально медиа), улучшение текста через LLM, выбор аудитории (тест себе / пользователи / чаты / оба), сегментация по регионам или по чатам. Отправка сейчас или планирование (еженедельно/ежемесячно). Получатели — из Google Sheets (`recipients_users`, `recipients_chats`).
- Для сценария «тест отправлен → финальная аудитория» добавлено восстановление payload из черновика (`broadcasts`), если FSM-состояние потеряно между шагами. Для диагностики этапов используется `broadcast_id` в логах (`[BROADCAST_TRACE]`).
- **Плановые рассылки** — список активных плановых рассылок, отключение. Фактическая отправка выполняется job’ом `scheduled_broadcasts` (cron).
- **Пополнение базы знаний** — загрузка документов (PDF, TXT, DOCX, MD, CSV): предподготовка, извлечение текста, чанкинг (structure-aware/semantic), обогащение, эмбеддинги в Qdrant. Команда `/kb_migrate` — миграция FAQ из Google Sheets в Qdrant.
- **Создание документов** — генерация DOCX из шаблонов (договоры и т.п.): выбор шаблона, пошаговый ввод полей (номер, даты, реквизиты заказчика). Реквизиты можно отправить текстом, фото/скриншотом или PDF/DOCX — извлечение полей через AI/Vision. Добавление новых шаблонов через «➕ Добавить шаблон».

### Фоновые процессы

- **Сбор получателей рассылок** — в Telegram: при добавлении бота в группу чат попадает в `recipients_chats`; при любом сообщении в личку/группе обновляются данные пользователя/чата в Google Sheets. В MAX: пользователь (`recipients_users`) — после `/start` у авторизованного; группа (`recipients_chats`) — при любом событии из группового чата в webhook. Обработчик не отвечает пользователю, только пишет в таблицы.
- **Ежедневный отчёт** — job `daily_report`: сводка за вчера (по UTC) по статистике бота, отправка в `MANAGER_CHAT_ID`.
- **Ежемесячный отчёт** — job `monthly_report`: сводка за прошлый месяц, отправка в `MANAGER_CHAT_ID`.

## Логи Railway (CLI и MCP)

Команды вроде `railway logs` и инструменты MCP по умолчанию читают логи **сервиса, привязанного к каталогу репозитория** (`railway link` в корне проекта; в Cursor — действие **link service** у Railway MCP для этого workspace). В одном Railway-проекте может быть несколько сервисов (Telegram `vobla-bot`, `worker`, MAX-only и т.д.): если привязан не тот сервис, в выгрузке окажутся **чужие** процессы (вплоть до другого стека — например `telegram.ext` вместо aiogram). Перед анализом логов убедитесь, что выбран сервис, где реально запущен нужный **Start Command**.

При `LOG_LEVEL=INFO` (по умолчанию) логгеры **`httpx`** и **`httpcore`** подняты до **WARNING**, чтобы в логах не светился полный URL `getUpdates` с токеном бота. Для отладки HTTP оставьте `LOG_LEVEL=DEBUG`, но в проде это нежелательно; при утечке токена в логи — перевыпустите токен у BotFather.

**502 / Bad Gateway** от `api.telegram.org` в логах обычно **кратковременны**; при следующем long poll aiogram снова подключается. Если сбой длится долго, проверьте статус Telegram, сеть Railway и отсутствие прокси.

После `railway link` на нужный сервис (например `vobla-bot`) в логах **не** должно быть стека **`python-telegram-bot`** / **`telegram.ext.Updater`** — этот репозиторий на **aiogram**. Если такие строки есть, логи идут с **другого сервиса** в том же проекте или запущен старый образ; проверьте **Start Command** и привязку сервиса.

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

### TelegramConflictError (`getUpdates` / «terminated by other getUpdates request»)

Одновременно в сети не должно быть **двух** клиентов **long polling** с **одним** `BOT_TOKEN`. Иначе Telegram рвёт соединение, aiogram в логах спамит ошибкой, **бот не получает апдейты** (рассылка и любые сценарии в Telegram «не работают»).

**Что проверить:**

- На Railway у сервиса с `python -m app.main` — **Replicas = 1** (не масштабировать горизонтально polling).
- Не запущен ли **второй** деплой/staging/локальный `python -m app.main` с **продакшен**-токеном.
- Два сервиса из одного репозитория: оба не должны выполнять polling с одним токеном. Для сервиса **только MAX** задай **`TELEGRAM_POLLING_ENABLED=false`** (см. конфигурацию) и **Start Command** только `python -m app.max_entrypoint`, без `app.main`.

**Опционально:** `TELEGRAM_POLLING_ENABLED=false` в процессе, где намеренно не нужен Telegram — процесс не вызывает `getUpdates` и не конфликтует с основным воркером.

Если логи смотрите у **правильного** сервиса (см. раздел «Логи Railway» про `railway link`) и всё равно видите `TelegramConflictError`, второй poller с тем же `BOT_TOKEN` всё ещё запущен — пройдите чеклист выше снова (реплики, локалка, второй деплой).

При наличии `REDIS_URL` в `app/main.py` работает распределённый polling guard: второй инстанс не запускает `start_polling`, если lock уже занят другим процессом.

Если **POST /webhook/max** с кодом **422** — это баг разбора аннотаций FastAPI у вложенного handler (исправлено в `app/max_entrypoint.py`): задеплой свежий коммит.

**Лимит Google Sheets (429 / Quota exceeded):** при большом числе запросов к таблицам (статистика, рассылки, лист получателей) API может вернуть 429. Запись в `bot_stats` через `log_event` **не роняет** сценарии RAG/QA — в логах будет предупреждение; при необходимости увеличь квоту проекта в Google Cloud или снизь частоту обращений к Sheets. Обработчик **MAX webhook** при типичной ошибке Sheets 429 отвечает платформе **`200`** с телом `{"ok": true, "degraded": true}`, чтобы не провоцировать лавину повторных доставок webhook; в логах — предупреждение.
Для ключевых сервисов (`auth`, `broadcast`, `recipients`) добавлен общий retry на 429 с backoff и пересозданием gspread-клиента (`run_with_retry_on_429` в `app/services/sheets_client.py`).

**Повторный запрос кода в MAX после кнопки «Авторизация»:** проверь колонку MAX ID в листе «Пользователи». Каноничное имя колонки — **`max_user_id`**. Legacy-имя **`max_id`** поддерживается, но в логах появляется предупреждение `legacy MAX column alias 'max_id'` — **переименуйте колонку в таблице в `max_user_id`**, чтобы убрать шум и путаницу.

**MAX `POST /answers` и 400:** API требует непустой payload (`message` или `notification`). Клиент теперь сначала шлёт `{"notification": ""}` и только затем fallback-стратегии.

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

Два сервиса из одного репозитория: **[railway.toml](railway.toml)** задаёт общий **Start Command** — ветка по **`$RAILWAY_SERVICE_NAME`**: сервис с именем **`vobla-max`** запускает `python -m app.max_entrypoint`, любой другой (например **`vobla-bot`**) — `python -m app.main`. [Procfile](Procfile) и [Dockerfile](Dockerfile) по умолчанию — только Telegram.

#### Сервис 1 — Telegram (`vobla-bot`)

- Деплой из `main`, **Replicas = 1.** На `PORT` — **`GET /health`** для проверки Railway (см. `app/main.py`).
- MCP Railway **не создаёт** новые сервисы в проекте; деплой/переменные — `link-service`, `set-variables`, `deploy`, `generate-domain`.

#### Сервис 2 — MAX webhook (`vobla-max`)

1. **CLI** (из корня репо, проект уже `railway link`):  
   `railway add --service vobla-max --repo mtubatay-lang/vobla-bot`  
   Имя сервиса должно быть ровно **`vobla-max`**, иначе поправьте ветку `vobla-max)` в [railway.toml](railway.toml).
2. Переменные: на сервисе **`TELEGRAM_POLLING_ENABLED=false`**; остальные секреты удобно задать **ссылками** на `vobla-bot`, например `BOT_TOKEN=${{vobla-bot.BOT_TOKEN}}` (см. [Variables](https://docs.railway.com/develop/variables)).
3. Публичный URL: `railway domain -s vobla-max` или UI **Networking → Generate Domain**.
4. Выставь **`MAX_WEBHOOK_PUBLIC_BASE`** на `https://<домен-vobla-max>` и снова выполни `scripts/max_subscribe_webhook.py`.

**Устаревший вариант** — один контейнер на оба процесса (фоновый `max_entrypoint` + `app.main`) — не используем.

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

- Гибридное меню MAX: команды + inline-кнопки. Поддерживаются **`/start`**, **`/help`**, **`/login`**, **`/ask`**, **`/kilbil`**, **`/admin`**.
- Callback-меню в MAX: `start_auth`, `qa_start`, `qa_exit`, `max:menu:help`, `max:menu:login`, `max:menu:kilbil`, `max:menu:admin`.
- После авторизации `qa_start` запускает упрощённый одношаговый RAG по Qdrant (см. `app/services/max_qa_simple.py`), `qa_exit` завершает сессию. Пока идёт поиск и генерация ответа, бот сразу шлёт короткое сообщение «ищу в базе знаний…» (и при необходимости индикатор набора), затем полный ответ отдельным сообщением — так видно, что бот не завис.
- **Группы MAX:** без `/ask` бот обрабатывает текстовые вопросы и после расшифровки голоса тем же групповым RAG, что и Telegram (`process_group_chat_question_with_sink`). Учитывайте **`RAG_TEST_CHAT_ID`** (см. выше).
- `kilbil` работает как отдельный вопрос в режиме знаний `help.kilbil.ru`; перед ответом также отправляется статус «ищу в базе Kilbil…».
- **`/admin`** в MAX открывает тот же набор функций, что в Telegram (рассылка, плановые рассылки, загрузка в базу знаний, генерация DOCX). Состояние шагов хранится в памяти процесса webhook (при нескольких репликах возможны расхождения — держите 1 реплику или sticky session).
- Рассылка из MAX: после теста — аудитория «пользователи / все чаты / оба», плюс **«Чаты и регионы»** (как в Telegram: **по регионам** или **мультивыбор чатов**). Учитываются только строки `recipients_chats` с **`platform=max`**; для регионов нужна колонка **`region`**. Плановые рассылки из MAX сохраняют выбор в `mode_extra` (`audience_platform=max`).
- Медиа кладётся в `media_json` формата **v2** (`{"version":2,"telegram":[],"max":[...]}`), чтобы для MAX-получателей использовались **file_id MAX**, для Telegram — свои вложения. См. `parse_broadcast_media_for_platform` в `app/services/broadcast_service.py`.
- Параллельность отправок в `execute_broadcast_multi` настраивается **`BROADCAST_SEND_CONCURRENCY`** (по умолчанию 10), если упираетесь в лимиты API.
- В листах **`recipients_users`** и **`recipients_chats`** (таблица `STATS_SHEET_ID`) добавьте колонку **`platform`**: значения `telegram` или `max`. Без колонки все строки считаются Telegram. Для MAX-рассылок пользователи должны попасть в таблицу (после `/start` у авторизованного бот делает upsert строки `platform=max`). **Групповые чаты MAX** попадают в `recipients_chats` автоматически при первом (и следующих) сообщениях в группе или нажатии inline-кнопки там же (`platform=max`, `chat_id` из MAX). Парсер учитывает `message.recipient.chat`, плоский **`recipient.chat_type`** (`chat` / `channel` / `dialog` и т.д.), **`update.chat`** на корне webhook, дублирование в **`message.chat`**, опционально **`body.recipient`**, верхнеуровневый **`recipient.chat_id`** без `user_id`, а также `participants_count > 2`. Если в webhook нет названия группы, после upsert вызывается **`GET /chats/{chatId}`** и колонка title обновляется (в логах: `title updated via API`). В логах: успех записи — `MAX recipients_chats: upsert scheduled`; если событие всё ещё считается личкой — строка `MAX message_created parsed as direct chat`.
- Команда **`/kb_migrate`** и тяжёлые обслуживающие сценарии по-прежнему удобнее запускать из Telegram.
- **`POST /answers`**: клиент перебирает варианты тела запроса (без тела, `{}`, `notification: ""` и т.д.) — у разных версий API MAX требования различаются.
- **Нижнее меню команд как в Telegram** (`setMyCommands` / reply keyboard) в MAX **нет** — только inline-кнопки у сообщений.

Если в логах всё ещё виден старый стек (`answer_callback` → `_request` с `json=body` на одной строке с прежним номером) — на Railway не задеплоен последний коммит; сделай **Redeploy**.

## Конфигурация

### Обязательные переменные (для основного бота)

- `BOT_TOKEN` — токен Telegram-бота  
- `TELEGRAM_POLLING_ENABLED` — по умолчанию `true`; `false` — не запускать long polling в этом процессе (второй сервис только под MAX webhook, чтобы не дублировать `getUpdates`)
- `OPENAI_API_KEY` — ключ OpenAI  
- `SHEET_ID`, `USERS_SHEET_ID`, `GOOGLE_SERVICE_ACCOUNT_JSON` — Google Sheets  
- При необходимости: `STATS_SHEET_ID`, `MANAGER_CHAT_ID`, `KB_MANAGERS_CHAT_ID` и др. (см. `app/config.py`)

### Qdrant (RAG / база знаний)

- **`QDRANT_URL`** — только **корень** REST API кластера из [Qdrant Cloud](https://cloud.qdrant.io/) (вида `https://<cluster-id>.<region>.gcp.cloud.qdrant.io`), **без** путей вроде `/collections`, `/dashboard`. Лишние хвосты код обрежет (см. `app/services/qdrant_service.py`), но хост должен совпадать с активным кластером.
- **`QDRANT_API_KEY`** — API key кластера (облако).
- **`QDRANT_COLLECTION_NAME`** — имя коллекции (по умолчанию `knowledge_base`).
- Если в логах **`404 page not found`** при вызовах Qdrant — чаще всего неверный **`QDRANT_URL`**, удалённый кластер или ключ не от того проекта; не путайте URL консоли и URL API.

### MAX (только при `ENABLE_MAX=true`)

- `MAX_BOT_TOKEN` — токен бота в MAX  
- `MAX_API_BASE_URL` — базовый URL API (по умолчанию `https://platform-api.max.ru`)  
- `MAX_WEBHOOK_PATH` — путь для webhook (по умолчанию `/webhook/max`)  
- `MAX_AUTH_BEARER_PREFIX` — `false` (по умолчанию): только токен в `Authorization`; `true` — префикс `Bearer ` (если без него API отвечает 401)  
- `MAX_WEBHOOK_SECRET` — если задан при подписке с `secret`, входящие запросы без совпадающего `X-Max-Bot-Api-Secret` отклоняются (403)  
- Для скрипта подписки: `MAX_WEBHOOK_PUBLIC_BASE` — HTTPS-база Railway без пути (к `MAX_WEBHOOK_PATH` он добавится)
- В листе `Пользователи` колонка MAX ID: **`max_user_id`** (если у вас было `max_id` — переименуйте в Sheets)

## Плановые рассылки

Запуск job для плановых рассылок (cron каждые 15–60 мин):

```bash
python -m app.jobs.scheduled_broadcasts
```

Используется тот же `execute_broadcast`, что и ручная рассылка; получатели берутся из Google Sheets по колонке `platform` (Telegram и MAX).

**Отчёты в чат менеджеров:**

- Ежедневный отчёт (за вчера по UTC): `python -m app.jobs.daily_report`
- Ежемесячный отчёт (за прошлый месяц): `python -m app.jobs.monthly_report`

## Ветка мультиплатформы

Работа с Telegram и MAX ведётся в ветке `feature/multi-messenger`. После проверки сценариев в Telegram и при необходимости в MAX изменения мержатся в `main`.
