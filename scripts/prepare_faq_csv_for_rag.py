#!/usr/bin/env python3
"""
Подготовка CSV «Частые вопросы Франшиза» для нарезки по чанкам RAG.
Использует app.services.document_preparation; результат пишется в data/faq_franchise_rag_ready.md.
"""

import sys
from pathlib import Path

# чтобы импорт app работал при запуске из любой директории
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from app.services.document_preparation import prepare_faq_csv_to_md


def main():
    base = Path(__file__).resolve().parent.parent
    input_path = base / "data" / "faq_franchise.csv"
    if not input_path.exists():
        input_path = Path("/Users/adelfarhutdinov/Downloads/Частые вопросы Франшиза - Sheet1.csv")
    output_path = base / "data" / "faq_franchise_rag_ready.md"

    if not input_path.exists():
        print(f"Файл не найден: {input_path}")
        return

    file_content = input_path.read_bytes()
    md_str = prepare_faq_csv_to_md(file_content)
    if not md_str:
        print("CSV не распознан. Нужны колонки: «Вопросы из Телеграм», «Ответ в Телеграм» или «Ответы Финал», «Направление (бухгалтерия, маркетинг, технические вопросы)».")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(md_str, encoding="utf-8")
    print(f"Готово. Подготовленный документ: {output_path}")
    lines_count = len(md_str.splitlines())
    print(f"Строк: {lines_count}")


if __name__ == "__main__":
    main()
