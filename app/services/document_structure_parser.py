"""Парсинг структуры документов (заголовки, разделы) для чанкинга."""

import io
import re
import logging
from typing import List, Dict, Any, Optional

try:
    from docx import Document
    from docx.enum.style import WD_STYLE_TYPE
except ImportError:
    Document = None

logger = logging.getLogger(__name__)

# Структура: {"section_path": str, "level": int, "content": str}
SectionInfo = Dict[str, Any]


def _parse_docx_structure(file_bytes: bytes) -> List[SectionInfo]:
    """Парсит DOCX с сохранением иерархии заголовков (Heading 1, 2, 3 и т.д.)."""
    if Document is None:
        return []
    try:
        doc = Document(io.BytesIO(file_bytes))
        sections: List[SectionInfo] = []
        path_stack: List[tuple[int, str]] = []  # (level, heading)

        for para in doc.paragraphs:
            text = para.text.strip()
            if not text:
                continue

            style_name = (para.style.name or "").lower()
            level = 0
            if "heading" in style_name:
                # Heading 1 -> level 1, Heading 2 -> level 2
                match = re.search(r"heading\s*(\d+)", style_name, re.I)
                if match:
                    level = int(match.group(1))
                else:
                    level = 1

            if level > 0:
                # Pop stack until we're at parent level
                while path_stack and path_stack[-1][0] >= level:
                    path_stack.pop()
                path_stack.append((level, text))
                section_path = " > ".join(h for _, h in path_stack)
                sections.append({
                    "section_path": section_path,
                    "level": level,
                    "content": text,
                    "heading": text,
                })
            else:
                # Body paragraph — привязываем к текущему разделу
                section_path = " > ".join(h for _, h in path_stack) if path_stack else ""
                sections.append({
                    "section_path": section_path,
                    "level": 0,
                    "content": text,
                    "heading": "",
                })

        return sections
    except Exception as e:
        logger.exception(f"[DOC_STRUCTURE] Ошибка парсинга DOCX: {e}")
        return []


def _parse_markdown_structure(text: str) -> List[SectionInfo]:
    """Парсит Markdown: # ## ### и т.д."""
    sections: List[SectionInfo] = []
    path_stack: List[tuple[int, str]] = []
    lines = text.split("\n")

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Markdown headers: # H1, ## H2, ### H3
        match = re.match(r"^(#{1,6})\s+(.+)$", stripped)
        if match:
            level = len(match.group(1))
            heading = match.group(2).strip()
            while path_stack and path_stack[-1][0] >= level:
                path_stack.pop()
            path_stack.append((level, heading))
            section_path = " > ".join(h for _, h in path_stack)
            sections.append({
                "section_path": section_path,
                "level": level,
                "content": stripped,
                "heading": heading,
            })
        else:
            section_path = " > ".join(h for _, h in path_stack) if path_stack else ""
            sections.append({
                "section_path": section_path,
                "level": 0,
                "content": stripped,
                "heading": "",
            })

    return sections


def _parse_txt_structure(text: str) -> List[SectionInfo]:
    """Эвристики для TXT: строки, оканчивающиеся на :, паттерны 1., 1.1., 2."""
    sections: List[SectionInfo] = []
    path_stack: List[tuple[int, str]] = []
    lines = text.split("\n")

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        level = 0
        # Нумерация: 1. 2. 3. или 1.1. 1.2. или 1) 2)
        num_match = re.match(r"^(\d+(?:\.\d+)*)[\.\)]\s*(.+)$", stripped)
        if num_match:
            num_part = num_match.group(1)
            rest = num_match.group(2).strip()
            # 1.2.3 -> level 3, 1.2 -> level 2, 1 -> level 1
            level = num_part.count(".") + 1 if "." in num_part else 1
            heading = rest.rstrip(":").strip()
            if heading.endswith(":"):
                heading = heading[:-1].strip()
            while path_stack and path_stack[-1][0] >= level:
                path_stack.pop()
            path_stack.append((level, heading))
            section_path = " > ".join(h for _, h in path_stack)
            sections.append({
                "section_path": section_path,
                "level": level,
                "content": stripped,
                "heading": heading,
            })
        elif stripped.endswith(":") and len(stripped) < 100:
            # Строка-заголовок без номера
            heading = stripped[:-1].strip()
            level = 1
            path_stack = [(1, heading)]
            section_path = heading
            sections.append({
                "section_path": section_path,
                "level": level,
                "content": stripped,
                "heading": heading,
            })
        else:
            section_path = " > ".join(h for _, h in path_stack) if path_stack else ""
            sections.append({
                "section_path": section_path,
                "level": 0,
                "content": stripped,
                "heading": "",
            })

    return sections


def parse_document_structure(
    file_bytes: bytes,
    filename: str,
    text: Optional[str] = None,
) -> List[SectionInfo]:
    """Парсит структуру документа (разделы, заголовки).

    Args:
        file_bytes: Байты файла (для DOCX)
        filename: Имя файла для определения формата
        text: Уже извлечённый текст (для MD/TXT, чтобы не декодировать повторно)

    Returns:
        Список {"section_path", "level", "content", "heading"}
    """
    fn = filename.lower()
    if fn.endswith(".docx") and Document:
        return _parse_docx_structure(file_bytes)
    if fn.endswith((".md", ".markdown")) or fn.endswith(".txt"):
        if text is None:
            try:
                text = file_bytes.decode("utf-8", errors="replace")
            except Exception:
                text = file_bytes.decode("latin-1", errors="replace")
        if fn.endswith((".md", ".markdown")):
            return _parse_markdown_structure(text)
        return _parse_txt_structure(text)
    # PDF и прочее — структура не извлекается
    return []


def structure_to_flat_sections(sections: List[SectionInfo]) -> List[Dict[str, Any]]:
    """Преобразует список секций в плоский список блоков с section_path и объединённым content.

    Объединяет последовательные body-параграфы под одним section_path.
    """
    if not sections:
        return []

    result: List[Dict[str, Any]] = []
    current_path = ""
    current_content: List[str] = []

    for s in sections:
        section_path = s.get("section_path", "")
        content = s.get("content", "")
        level = s.get("level", 0)

        if level > 0:
            # Заголовок — сохраняем накопленный контент, начинаем новый блок
            if current_content:
                result.append({
                    "section_path": current_path,
                    "content": "\n\n".join(current_content),
                })
            current_path = section_path
            current_content = [content]
        else:
            current_content.append(content)

    if current_content:
        result.append({
            "section_path": current_path,
            "content": "\n\n".join(current_content),
        })

    return result
