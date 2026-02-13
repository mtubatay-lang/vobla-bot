#!/usr/bin/env python3
"""
Подготовка CSV «Частые вопросы Франшиза» для нарезки по чанкам RAG.
Строит .md с разметкой ## РАЗДЕЛ (по направлению) и ### ПОДРАЗДЕЛ (вопрос),
чтобы при загрузке в бота парсер давал section_path и чанки были по смыслу.
"""

import csv
import re
from pathlib import Path

# Нормализация направления из CSV в название раздела
DIRECTION_TO_SECTION = {
    "отдел запуска": "Отдел запуска",
    "отдеел запуска/сопровождения": "Отдел запуска",
    "отдел запска": "Отдел запуска",
    "отдл запуска": "Отдел запуска",
    "отдел запуска-сопровождени": "Отдел запуска",
    "отдел запуска/сопровождени": "Отдел запуска",
    "отдел запуска/сопровожденичя": "Отдел запуска",
    "отдел запуска/сопровождения": "Отдел запуска",
    "технический вопрос": "Технические вопросы",
    "маркетинг": "Маркетинг",
    "бухгалтерия": "Бухгалтерия",
    "отдел бухгалтерии": "Бухгалтерия",
    "отдел сопровождения": "Отдел сопровождения",
}


def normalize_direction(raw: str) -> str:
    """Возвращает нормализованное название раздела или пустую строку."""
    if not raw or not raw.strip():
        return ""
    # Если в ячейке попал JSON (media и т.п.), пропускаем
    s = raw.strip().lower()
    if s.startswith("[{") or s.startswith("{"):
        return ""
    return DIRECTION_TO_SECTION.get(s, raw.strip())


def main():
    base = Path(__file__).resolve().parent.parent
    input_path = Path("/Users/adelfarhutdinov/Downloads/Частые вопросы Франшиза - Sheet1.csv")
    output_path = base / "data" / "faq_franchise_rag_ready.md"

    if not input_path.exists():
        print(f"Файл не найден: {input_path}")
        return

    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=",", quotechar='"')
        rows = list(reader)

    if not rows:
        print("CSV пустой")
        return

    col_direction = "Направление (бухгалтерия, маркетинг, технические вопросы)"
    col_question = "Вопросы из Телеграм"
    col_answer_telegram = "Ответ в Телеграм"
    col_answer_final = "Ответы Финал"

    # Порядок разделов в документе
    section_order = ["Отдел запуска", "Технические вопросы", "Маркетинг", "Бухгалтерия", "Отдел сопровождения"]
    by_section: dict[str, list[dict]] = {}
    for row in rows:
        direction = normalize_direction(row.get(col_direction, ""))
        if not direction:
            direction = "Прочее"
        q = (row.get(col_question) or "").strip()
        a = (row.get(col_answer_final) or row.get(col_answer_telegram) or "").strip()
        if not q:
            continue
        if direction not in by_section:
            by_section[direction] = []
        by_section[direction].append({"question": q, "answer": a})

    # Порядок разделов: сначала фиксированный, потом остальные по алфавиту
    ordered_sections = [s for s in section_order if s in by_section]
    for s in sorted(by_section.keys()):
        if s not in ordered_sections:
            ordered_sections.append(s)

    out_lines = []
    out_lines.append("### Правила нарезки")
    out_lines.append("")
    out_lines.append("- Чанк = логически связный фрагмент внутри одного раздела.")
    out_lines.append("- Не разрывать нумерованные шаги, чек-листы и списки посередине.")
    out_lines.append("- В метаданных чанка указывать: раздел (section), section_path и при возможности идентификатор чанка.")
    out_lines.append("- Разделы помечены «## РАЗДЕЛ N. Название», подразделы — «### ПОДРАЗДЕЛ: Вопрос». Нарезку выполнять по смыслу по этим границам.")
    out_lines.append("")
    out_lines.append("## Частые вопросы по франшизе Воблаbeer")
    out_lines.append("")
    out_lines.append("Документ подготовлен для RAG из базы вопросов и ответов. Каждый блок — вопрос и ответ по направлению (отдел запуска, маркетинг, бухгалтерия, технические вопросы, отдел сопровождения).")
    out_lines.append("")

    for num, section_name in enumerate(ordered_sections, start=1):
        items = by_section[section_name]
        out_lines.append("")
        out_lines.append(f"## РАЗДЕЛ {num}. {section_name}")
        out_lines.append("")

        for item in items:
            q = item["question"]
            a = item["answer"]
            # Заголовок подраздела — укороченный вопрос (без переносов для одной строки)
            q_title = q.replace("\n", " ").strip()[:120]
            if len(q) > 120:
                q_title += "…"
            out_lines.append(f"### ПОДРАЗДЕЛ: {q_title}")
            out_lines.append("")
            out_lines.append("**Вопрос:** " + q)
            out_lines.append("")
            if a:
                out_lines.append("**Ответ:** " + a)
            else:
                out_lines.append("**Ответ:** (без текста)")
            out_lines.append("")
            out_lines.append("---")
            out_lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(out_lines), encoding="utf-8")
    print(f"Готово. Подготовленный документ: {output_path}")
    print(f"Строк: {len(out_lines)}, разделов: {len(ordered_sections)}, пар Q&A: {sum(len(by_section[s]) for s in ordered_sections)}")


if __name__ == "__main__":
    main()
