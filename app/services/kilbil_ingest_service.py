"""Сервис индексации базы знаний help.kilbil.ru в Qdrant.

Используется скриптом scripts/ingest_kilbil_help.py и командой /kb_ingest_kilbil в боте.
"""

import asyncio
import json
import logging
import re
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Set, List, Dict, Any, Optional, Callable, Awaitable
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from app.services.chunking_service import semantic_chunk_text, extract_metadata_from_text
from app.services.context_enrichment import enrich_chunks_batch
from app.services.openai_client import create_embedding
from app.services.qdrant_service import get_qdrant_service

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent
BASE_URL = "https://help.kilbil.ru"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
REQUEST_DELAY = 0.8
PROGRESS_FILE = _ROOT / "data" / "kilbil_ingest_progress.json"
BATCH_UPLOAD_ARTICLES = 10
ARTICLE_PATH_RE = re.compile(r"^/(\d+-)+\d+--[a-z0-9-]+/?$", re.I)
SKIP_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".css", ".js", ".woff", ".ico"}
SKIP_PATHS = {"/api/", "/files/", "/static/", "/hc/", "/login", "/signin", "/search"}


def _is_article_path(path: str) -> bool:
    path = path.rstrip("/")
    if not path or path == "/":
        return False
    if path.startswith("/"):
        path = path[1:]
    return bool(ARTICLE_PATH_RE.match("/" + path))


def _normalize_path(href: str) -> str:
    parsed = urlparse(href)
    path = parsed.path or "/"
    path = path.rstrip("/") or "/"
    if path != "/":
        path = path + "/"
    return path


def _should_skip_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc and "help.kilbil.ru" not in parsed.netloc:
        return True
    path = (parsed.path or "").lower()
    for ext in SKIP_EXTENSIONS:
        if path.endswith(ext):
            return True
    for skip in SKIP_PATHS:
        if skip in path:
            return True
    return False


def load_progress() -> Optional[Dict[str, Any]]:
    if not PROGRESS_FILE.exists():
        return None
    try:
        data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        urls = data.get("article_urls", [])
        processed = set(data.get("processed_urls", []))
        if not urls:
            return None
        return {"article_urls": list(urls), "processed_urls": processed}
    except Exception as e:
        logger.warning(f"[KILBIL] Не удалось загрузить прогресс: {e}")
        return None


def save_progress(article_urls: List[str], processed_urls: Set[str]) -> None:
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "article_urls": article_urls,
        "processed_urls": list(processed_urls),
        "last_updated": datetime.now().isoformat(),
        "total": len(article_urls),
        "processed_count": len(processed_urls),
    }
    PROGRESS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"[KILBIL] Прогресс сохранён: {len(processed_urls)}/{len(article_urls)} статей")


def clear_progress() -> None:
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
        logger.info("[KILBIL] Прогресс очищен")


async def crawl_article_urls() -> Set[str]:
    seen_pages: Set[str] = set()
    article_urls: Set[str] = set()
    to_visit: deque = deque(["/"])

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=30.0,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "ru-RU,ru;q=0.9"},
    ) as client:
        while to_visit:
            path = to_visit.popleft()
            full_url = urljoin(BASE_URL, path)
            if full_url in seen_pages:
                continue
            seen_pages.add(full_url)
            if _is_article_path(path):
                article_urls.add(full_url)
                logger.info(f"[CRAWL] Найдена статья: {full_url}")
            await asyncio.sleep(REQUEST_DELAY)
            try:
                resp = await client.get(full_url)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"[CRAWL] Ошибка загрузки {full_url}: {e}")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.select('a[href]'):
                href = a.get("href", "").strip()
                if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
                    continue
                joined = urljoin(full_url, href)
                if _should_skip_url(joined):
                    continue
                parsed = urlparse(joined)
                if "help.kilbil.ru" not in parsed.netloc and parsed.netloc:
                    continue
                norm_path = _normalize_path(parsed.path)
                target_url = urljoin(BASE_URL, norm_path)
                if target_url not in seen_pages and norm_path not in (p for p in to_visit):
                    to_visit.append(norm_path)
    return article_urls


def _fetch_article_sync(url: str) -> Optional[Dict[str, Any]]:
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=30.0,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "ru-RU,ru;q=0.9"},
        ) as client:
            resp = client.get(url)
            resp.raise_for_status()
    except Exception as e:
        logger.warning(f"[PARSE] Ошибка загрузки {url}: {e}")
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    article_el = soup.select_one("article.kb-article") or soup.select_one(".kb-article") or soup.select_one("[itemprop='articleBody']")
    if not article_el:
        logger.warning(f"[PARSE] Не найден контент статьи: {url}")
        return None
    for nav in article_el.select(".article__navigation"):
        nav.decompose()
    text = article_el.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if len(text) < 50:
        logger.warning(f"[PARSE] Слишком короткий текст в {url}: {len(text)} символов")
        return None
    title_el = soup.select_one("title") or soup.select_one("h1") or article_el.select_one("h1")
    title = (title_el.get_text(strip=True) if title_el else "") or "Статья kilbil"
    if " | " in title:
        title = title.split(" | ", 1)[-1].strip()
    return {"url": url, "title": title, "text": text}


async def run_ingestion(
    fresh: bool = False,
    progress_callback: Optional[Callable[[str, str], Awaitable[None]]] = None,
) -> Dict[str, Any]:
    """Основная функция индексации help.kilbil.ru в Qdrant."""

    async def _progress(stage: str, detail: str) -> None:
        if progress_callback:
            await progress_callback(stage, detail)

    progress = None if fresh else load_progress()
    if progress:
        article_urls = progress["article_urls"]
        processed_urls = set(progress["processed_urls"])
        logger.info(f"[KILBIL] Продолжаю: {len(processed_urls)}/{len(article_urls)} обработано")
        await _progress("Продолжаю", f"{len(processed_urls)}/{len(article_urls)} статей")
    else:
        if fresh:
            clear_progress()
        await _progress("Обход сайта", "собираю URL статей...")
        article_urls = sorted(await crawl_article_urls())
        processed_urls = set()
        logger.info(f"[KILBIL] Найдено {len(article_urls)} статей")
        save_progress(article_urls, processed_urls)
        await _progress("Обход завершён", f"{len(article_urls)} статей")

    if not article_urls:
        return {"articles": 0, "chunks": 0, "success": True, "error": None}

    try:
        qdrant_service = get_qdrant_service()
    except Exception as e:
        logger.error(f"[KILBIL] Не удалось подключиться к Qdrant: {e}")
        return {"articles": len(article_urls), "chunks": 0, "success": False, "error": str(e)}

    is_resume = len(processed_urls) > 0
    if not is_resume:
        try:
            qdrant_service.delete_by_source("kilbil_help")
            logger.info("[KILBIL] Удалены старые данные source=kilbil_help")
        except Exception as e:
            logger.warning(f"[KILBIL] Не удалось удалить старые данные: {e}")

    to_process = [u for u in article_urls if u not in processed_urls]
    total_chunks = 0
    timestamp = datetime.now().isoformat()
    batch_chunks: List[Dict[str, Any]] = []
    batch_embeddings: List[List[float]] = []
    newly_processed: Set[str] = set()

    for idx, url in enumerate(to_process):
        await asyncio.sleep(REQUEST_DELAY)
        article = await asyncio.to_thread(_fetch_article_sync, url)
        if not article:
            continue

        full_text = f"Заголовок: {article['title']}\n\n{article['text']}"
        chunks = semantic_chunk_text(full_text)
        if not chunks:
            chunks = [{"text": full_text, "chunk_index": 0, "total_chunks": 1, "start_char": 0, "end_char": len(full_text)}]

        document_title = article["title"][:80] + ("..." if len(article["title"]) > 80 else "")
        try:
            enriched_chunks = await enrich_chunks_batch(chunks, document_title)
        except Exception as e:
            logger.warning(f"[KILBIL] Ошибка обогащения {url}: {e}")
            enriched_chunks = chunks

        extracted = extract_metadata_from_text(full_text, source="kilbil_help")
        for chunk in enriched_chunks:
            chunk_text = chunk.get("text", "")
            if not chunk_text.strip():
                continue
            try:
                emb = await asyncio.to_thread(create_embedding, chunk_text)
                batch_embeddings.append(emb)
                batch_chunks.append({
                    "text": chunk_text,
                    "metadata": {
                        "source": "kilbil_help",
                        "document_type": extracted.get("document_type", "help_article"),
                        "category": extracted.get("category", "kilbil"),
                        "tags": extracted.get("tags", []),
                        "document_title": document_title,
                        "article_url": article["url"],
                        "chunk_index": chunk.get("chunk_index", 0),
                        "total_chunks": chunk.get("total_chunks", len(enriched_chunks)),
                        "indexed_at": timestamp,
                    },
                })
            except Exception as e:
                logger.warning(f"[KILBIL] Ошибка эмбеддинга: {e}")

        newly_processed.add(url)

        if len(newly_processed) >= BATCH_UPLOAD_ARTICLES and batch_chunks:
            qdrant_service.add_documents(batch_chunks, batch_embeddings)
            total_chunks += len(batch_chunks)
            processed_urls.update(newly_processed)
            save_progress(article_urls, processed_urls)
            logger.info(f"[KILBIL] Загружено: {total_chunks} чанков, {len(processed_urls)}/{len(article_urls)} статей")
            await _progress("Загрузка", f"{len(processed_urls)}/{len(article_urls)} статей, {total_chunks} чанков")
            batch_chunks = []
            batch_embeddings = []
            newly_processed = set()

        if (idx + 1) % 5 == 0:
            logger.info(f"[KILBIL] Обработано {idx + 1}/{len(to_process)}")

    if batch_chunks:
        qdrant_service.add_documents(batch_chunks, batch_embeddings)
        total_chunks += len(batch_chunks)
        processed_urls.update(newly_processed)
    save_progress(article_urls, processed_urls)

    if len(processed_urls) >= len(article_urls):
        clear_progress()
        logger.info("[KILBIL] Индексация завершена")

    return {"articles": len(article_urls), "chunks": total_chunks, "success": True, "error": None}
