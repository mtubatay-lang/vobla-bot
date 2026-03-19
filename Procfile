# Telegram (long polling) + MAX webhook в одном процессе Railway: uvicorn на $PORT, затем polling.
# Отдельный сервис только под MAX по-прежнему предпочтительнее — см. README.
worker: sh -c "python -m app.max_entrypoint & exec python -m app.main"




