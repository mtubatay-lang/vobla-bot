#!/usr/bin/env python3
"""
Подготовка регламента Воблаbeer для нарезки по чанкам RAG.
Использует app.services.document_preparation; результат пишется в data/knowledge_rag_ready.md.
"""

import sys
from pathlib import Path

# чтобы импорт app работал при запуске из любой директории
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from app.services.document_preparation import prepare_regulation_txt_to_md


def main():
    base = Path(__file__).resolve().parent.parent
    input_path = base / "data" / "knowledge.txt"
    if not input_path.exists():
        input_path = Path("/Users/adelfarhutdinov/Downloads/knowledge.txt")
    output_path = base / "data" / "knowledge_rag_ready.md"

    if not input_path.exists():
        print(f"Файл не найден: {input_path}")
        return

    text = input_path.read_text(encoding="utf-8")
    md_str = prepare_regulation_txt_to_md(text)
    if not md_str:
        print("Регламент не распознан (мало разделов). Проверьте формат файла.")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(md_str, encoding="utf-8")
    print(f"Готово. Подготовленный документ: {output_path}")
    print(f"Размер ~{output_path.stat().st_size // 1024} КБ")


if __name__ == "__main__":
    main()
