"""Обработка документов разных форматов для извлечения текста."""

import logging
import io
from typing import Optional

try:
    from pypdf import PdfReader
except ImportError:
    try:
        from PyPDF2 import PdfReader
    except ImportError:
        PdfReader = None

try:
    from docx import Document
except ImportError:
    Document = None

try:
    import markdown
except ImportError:
    markdown = None

logger = logging.getLogger(__name__)


def extract_text_from_pdf(file_bytes: bytes) -> str:
    """Извлекает текст из PDF файла.
    
    Args:
        file_bytes: Байты PDF файла
    
    Returns:
        Извлеченный текст
    """
    if PdfReader is None:
        raise ImportError("pypdf или PyPDF2 не установлен. Установите: pip install pypdf")
    
    try:
        pdf_file = io.BytesIO(file_bytes)
        reader = PdfReader(pdf_file)
        
        text_parts = []
        for page in reader.pages:
            text_parts.append(page.extract_text())
        
        return "\n\n".join(text_parts)
    except Exception as e:
        logger.exception(f"[DOC_PROCESSOR] Ошибка извлечения текста из PDF: {e}")
        raise


def extract_text_from_docx(file_bytes: bytes) -> str:
    """Извлекает текст из DOCX файла.
    
    Args:
        file_bytes: Байты DOCX файла
    
    Returns:
        Извлеченный текст
    """
    if Document is None:
        raise ImportError("python-docx не установлен. Установите: pip install python-docx")
    
    try:
        doc_file = io.BytesIO(file_bytes)
        doc = Document(doc_file)
        
        text_parts = []
        for paragraph in doc.paragraphs:
            if paragraph.text.strip():
                text_parts.append(paragraph.text)
        
        return "\n\n".join(text_parts)
    except Exception as e:
        logger.exception(f"[DOC_PROCESSOR] Ошибка извлечения текста из DOCX: {e}")
        raise


def extract_text_from_txt(file_bytes: bytes) -> str:
    """Извлекает текст из TXT файла.
    
    Args:
        file_bytes: Байты TXT файла
    
    Returns:
        Извлеченный текст
    """
    try:
        # Пробуем разные кодировки
        encodings = ['utf-8', 'cp1251', 'windows-1251', 'latin-1']
        
        for encoding in encodings:
            try:
                return file_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue
        
        # Если ничего не помогло, пробуем с ошибками
        return file_bytes.decode('utf-8', errors='ignore')
    except Exception as e:
        logger.exception(f"[DOC_PROCESSOR] Ошибка извлечения текста из TXT: {e}")
        raise


def extract_text_from_markdown(file_bytes: bytes) -> str:
    """Извлекает текст из Markdown файла.
    
    Args:
        file_bytes: Байты MD файла
    
    Returns:
        Извлеченный текст (plain text, без разметки)
    """
    try:
        # Сначала декодируем в строку
        text = extract_text_from_txt(file_bytes)
        
        # Если markdown установлен, можно конвертировать в HTML и извлечь текст
        # Но для простоты просто возвращаем текст как есть
        # (можно улучшить, если нужно убрать разметку)
        return text
    except Exception as e:
        logger.exception(f"[DOC_PROCESSOR] Ошибка извлечения текста из Markdown: {e}")
        raise


def extract_text(file_bytes: bytes, filename: str) -> str:
    """Универсальная функция для извлечения текста из файла.
    
    Автоматически определяет формат по расширению файла.
    
    Args:
        file_bytes: Байты файла
        filename: Имя файла (для определения формата)
    
    Returns:
        Извлеченный текст
    
    Raises:
        ValueError: Если формат файла не поддерживается
    """
    filename_lower = filename.lower()
    
    if filename_lower.endswith('.pdf'):
        return extract_text_from_pdf(file_bytes)
    elif filename_lower.endswith('.docx'):
        return extract_text_from_docx(file_bytes)
    elif filename_lower.endswith('.txt'):
        return extract_text_from_txt(file_bytes)
    elif filename_lower.endswith(('.md', '.markdown')):
        return extract_text_from_markdown(file_bytes)
    else:
        # Пробуем как TXT по умолчанию
        logger.warning(f"[DOC_PROCESSOR] Неизвестный формат файла {filename}, пробую как TXT")
        return extract_text_from_txt(file_bytes)
