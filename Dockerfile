# Сборка без Railpack: на Railway иногда архив распаковывается с одним вложенным каталогом
# (mtubatay-lang-vobla-bot-<hash>/), из‑за чего Railpack не находит requirements.txt в корне.
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

# Если в контексте только одна подпапка — поднимаем файлы проекта в /app
RUN set -eux; \
    cnt=$(find . -mindepth 1 -maxdepth 1 | wc -l); \
    if [ "$cnt" -eq 1 ]; then \
      sub=$(find . -mindepth 1 -maxdepth 1 -type d | head -1); \
      if [ -n "$sub" ]; then \
        mv "$sub"/* . 2>/dev/null || true; \
        mv "$sub"/.[!.]* . 2>/dev/null || true; \
        rmdir "$sub" 2>/dev/null || true; \
      fi; \
    fi

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

# Запуск как в Procfile (Telegram + MAX webhook)
CMD ["sh", "-c", "python -m app.max_entrypoint & exec python -m app.main"]
