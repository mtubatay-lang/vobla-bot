"""
Предподготовка документов для RAG: преобразование CSV (FAQ) и текста (регламент)
в единый формат .md с разметкой ## РАЗДЕЛ / ### ПОДРАЗДЕЛ для корректной нарезки.
"""

import csv
import io
import re
from pathlib import Path
from typing import Optional

# --- FAQ CSV: нормализация направления в название раздела ---
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

FAQ_REQUIRED_COLUMNS = (
    "Вопросы из Телеграм",
    "Направление (бухгалтерия, маркетинг, технические вопросы)",
)
FAQ_ANSWER_COLUMNS = ("Ответы Финал", "Ответ в Телеграм")
SECTION_ORDER_FAQ = ["Отдел запуска", "Технические вопросы", "Маркетинг", "Бухгалтерия", "Отдел сопровождения"]

# --- Регламент: оглавление и паттерны разделов ---
SECTION_TITLES = [
    (1, "Введение. История компании"),
    (2, "Открытие ИП"),
    (3, "Поиск помещения для открытия магазина Воблаbeer"),
    (4, "Меркурий, ЕГАИС, Честный Знак"),
    (5, "Ремонтные работы помещения"),
    (6, "Требования по вывеске"),
    (7, "Мебель"),
    (8, "Оборудование"),
    (9, "Обучение партнера"),
    (10, "Подбор персонала"),
    (11, "Обучение персонала"),
    (12, "Инкассация"),
    (13, "Стандарты работы персонала магазина Воблаbeer"),
    (14, "Подлинность денежных купюр"),
    (15, "Необходимый пакет документов для трудоустройства сотрудников"),
    (16, "Работа с поставщиками"),
    (17, "Работа с подрядными организациями"),
    (18, "Маркетинг"),
    (19, "Инструкция по работе с отзывами"),
    (20, "Мерчандайзинг"),
    (21, "Уголок потребителя"),
    (22, "План проведения торжественного открытия точки"),
    (23, "Смета расходов на открытие и сопровождение"),
    (24, "Рекомендации по увеличению продаж магазина"),
    (25, "Чек-лист проверки магазина"),
    (26, "Общая информация"),
]

SECTION_START_PATTERNS = [
    (1, r"1\.\s+Введение\.\s+История компании"),
    (2, r"^Открытие юридического лица"),
    (3, r"^Поиск помещения для открытия магазина Воблаbeer"),
    (4, r"^4\.\s+Меркурий,?\s+ЕГАИС"),
    (5, r"^5\.\s+Ремонтные работы помещения"),
    (6, r"^6\.\s+Требования по вывеске"),
    (7, r"^7\.\s+Мебель\.?\s*$"),
    (8, r"^8\.\s+Оборудование\.?\s*$"),
    (9, r"^9\.\s+Обучение партнера\.?\s*$"),
    (10, r"^10\.\s+Подбор персонала\.?\s*$"),
    (11, r"^11\.\s+Обучение персонала\.?\s*$"),
    (12, r"^12\.\s+Инкассация\.?\s*$"),
    (13, r"^13\.\s+Стандарты работы персонала"),
    (14, r"^14\.\s+Подлинность (купюр|денежных купюр)"),
    (15, r"^15\.\s+Необходимый пакет документов"),
    (16, r"^16\.\s+Работа с поставщиками"),
    (17, r"^17\.\s+Работа с подрядными"),
    (18, r"^18\.\s+Маркетинг\.?\s*$"),
    (19, r"^19\.\s+Инструкция по работе с\s*$"),
    (20, r"^20\.\s+Мерчандайзинг\.?\s*$"),
    (21, r"^21\.\s+Уголок потребителя\.?\s*$"),
    (22, r"^22\.\s+План проведения (торжественного )?открытия точки"),
    (23, r"^23\.\s+Смета расходов на открытие"),
    (24, r"^24\.\s+Рекомендации по увеличению"),
    (25, r"^25\.\s+Чек-лист проверки магазина"),
    (26, r"^26\.\s+Общая информация\.?\s*$"),
]

SUBSECTION_HEADERS = [
    r"^История и философия бренда",
    r"^Формат и развитие",
    r"^Технологии как основа",
    r"^Ассортимент и предложение",
    r"^Ценности и цели",
    r"^Вместо вывода — приглашение",
    r"^Открытие юридического лица",
    r"^Эквайринг",
    r"^Чек-лист выбора месторасположения",
    r"^Особое внимание нужно обратить на Федеральный закон",
    r"^Список документов на проверку",
    r"^Пошаговые действия:",
    r"^Патентная система налогообложения",
    r"^Розничная продажа алкогольной продукции не допускаются",
    r"^Вы нашли помещение",
    r"^Совместно с согласованием договора аренды",
    r"^ФГИС \"Меркурий\"",
    r"^До пробития первого чека",
    r"^ЕГАИС \(Единая государственная",
    r"^Что такое ключ Рутокен",
    r"^\"Честный знак\"",
    r"^Как работает маркировка",
    r"^Инструкция по регистрации участника",
    r"^Ответы на самые распространенные вопросы",
    r"^Правила продажи пива",
    r"^Как принять пивные кеги",
    r"^Продажа разливного пива в ПЭТ-упаковке",
    r"^Учет потерь",
    r"^Оборудование и программы для учета",
    r"^Отчетность при продаже пива",
    r"^Ремонтные работы в помещении",
    r"^Осматриваем напольное покрытие",
    r"^Потолок:",
    r"^Самое важное, с чего начинается процесс ремонта",
    r"^Этапы устройства холодильной камеры",
    r"^Размер холодильной камеры",
    r"^Холодильные камеры утепляют",
    r"^Технологии\s*$",
    r"^Идет ГКЛ",
    r"^Монтаж пивного трубопровода",
    r"^Электрика:",
    r"^Далее идет процесс покраски стен",
    r"^Стены торговый зал",
    r"^Требования по вывескам",
    r"^Мебель\.\s*$",
    r"^Стена под краны",
    r"^Стол-ресепшен",
    r"^Для размещения снеков",
    r"^Оборудование\.\s*$",
    r"^Схема устройства пивной системы",
    r"^Устройство пивной системы",
    r"^Для чего используется газ",
    r"^Для размещение напитков в торговом зале",
    r"^Инструкция по правильной эксплуатации холодильного оборудования",
    r"^Выкладка продукции",
    r"^Уход, чистка холодильного оборудования",
    r"^Обучение партнера\.\s*$",
    r"^Подбор персонала\.\s*$",
    r"^Обучение персонала\.\s*$",
    r"^Инкассация\.\s*$",
    r"^Стандарты работы персонала",
    r"^Подлинность купюр",
    r"^Действия продавца в случае возникновения сомнений",
    r"^Признаки подлинности денежных купюр",
    r"^Необходимый пакет документов для трудоустройства",
    r"^Работа с поставщиками\.\s*$",
    r"^Работа с подрядными организациями",
    r"^Маркетинговые активности",
    r"^Бонусная карта Воблаbeer",
    r"^Инструкция по работе с отзывами",
    r"^Мерчандайзинг\s*$",
    r"^Принципы выкладки товара в пивной горке",
    r"^Инструкция по оформлению товара на паллетной выкладке",
    r"^Принципы выкладки рыбы",
    r"^Уголок потребителя",
    r"^План проведения открытия точки",
    r"^План проведения торжественного открытия точки",
    r"^Смета расходов",
    r"^Рекомендации по увеличению продаж магазина",
    r"^Чек-лист проверки магазина",
    r"^Общая информация\.\s*$",
    r"^Каплесборник",
    r"^Кабельные бирки",
    r"^Температурный режим",
    r"^Код доступа для работы на кассе",
    r"^Важные действия по работе с Честным знаком",
]

REGULATION_MIN_SECTIONS = 5

RULES_OF_CHUNKING = [
    "### Правила нарезки",
    "",
    "- Чанк = логически связный фрагмент внутри одного раздела.",
    "- Не разрывать нумерованные шаги, чек-листы и списки посередине.",
    "- В метаданных чанка указывать: раздел (section), section_path и при возможности идентификатор чанка.",
    "- Разделы помечены «## РАЗДЕЛ N. Название», подразделы — «### ПОДРАЗДЕЛ: Название». Нарезку выполнять по смыслу по этим границам.",
    "",
]


def _normalize_direction(raw: str) -> str:
    if not raw or not raw.strip():
        return ""
    s = raw.strip().lower()
    if s.startswith("[{") or s.startswith("{"):
        return ""
    return DIRECTION_TO_SECTION.get(s, raw.strip())


def prepare_faq_csv_to_md(file_content: bytes) -> Optional[str]:
    """
    Преобразует CSV с FAQ (вопросы/ответы по направлениям) в RAG-ready Markdown.
    Возвращает None, если в CSV нет нужных колонок.
    """
    try:
        text = file_content.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = file_content.decode("cp1251")
        except Exception:
            return None
    stream = io.StringIO(text)
    try:
        reader = csv.DictReader(stream, delimiter=",", quotechar='"')
        rows = list(reader)
    except Exception:
        return None
    if not rows:
        return None
    first = rows[0]
    for col in FAQ_REQUIRED_COLUMNS:
        if col not in first:
            return None
    col_direction = FAQ_REQUIRED_COLUMNS[1]
    col_question = FAQ_REQUIRED_COLUMNS[0]
    col_answer_final = FAQ_ANSWER_COLUMNS[0]
    col_answer_telegram = FAQ_ANSWER_COLUMNS[1]

    by_section: dict[str, list[dict]] = {}
    for row in rows:
        direction = _normalize_direction(row.get(col_direction, ""))
        if not direction:
            direction = "Прочее"
        q = (row.get(col_question) or "").strip()
        a = (row.get(col_answer_final) or row.get(col_answer_telegram) or "").strip()
        if not q:
            continue
        if direction not in by_section:
            by_section[direction] = []
        by_section[direction].append({"question": q, "answer": a})

    ordered_sections = [s for s in SECTION_ORDER_FAQ if s in by_section]
    for s in sorted(by_section.keys()):
        if s not in ordered_sections:
            ordered_sections.append(s)

    out_lines = list(RULES_OF_CHUNKING)
    out_lines.append("## Частые вопросы по франшизе Воблаbeer")
    out_lines.append("")
    out_lines.append(
        "Документ подготовлен для RAG из базы вопросов и ответов. Каждый блок — вопрос и ответ по направлению (отдел запуска, маркетинг, бухгалтерия, технические вопросы, отдел сопровождения)."
    )
    out_lines.append("")

    for num, section_name in enumerate(ordered_sections, start=1):
        items = by_section[section_name]
        out_lines.append("")
        out_lines.append(f"## РАЗДЕЛ {num}. {section_name}")
        out_lines.append("")
        for item in items:
            q = item["question"]
            a = item["answer"]
            q_title = q.replace("\n", " ").strip()[:120]
            if len(q) > 120:
                q_title += "…"
            out_lines.append(f"### ПОДРАЗДЕЛ: {q_title}")
            out_lines.append("")
            out_lines.append("**Вопрос:** " + q)
            out_lines.append("")
            out_lines.append("**Ответ:** " + (a if a else "(без текста)"))
            out_lines.append("")
            out_lines.append("---")
            out_lines.append("")

    return "\n".join(out_lines)


def _find_section_starts(lines: list[str]) -> list[tuple[int, int, int]]:
    results = []
    toc_end = 31
    for num, pattern in SECTION_START_PATTERNS:
        compiled = re.compile(pattern, re.MULTILINE | re.IGNORECASE)
        for i in range(toc_end, len(lines)):
            line = lines[i]
            if compiled.search(line.strip()):
                if not results or results[-1][0] != num:
                    results.append((num, i, len(line.strip())))
                break
    return results


def prepare_regulation_txt_to_md(text: str) -> Optional[str]:
    """
    Преобразует текст регламента (с оглавлением и нумерованными разделами) в RAG-ready Markdown.
    Возвращает None, если найдено меньше REGULATION_MIN_SECTIONS разделов.
    """
    if not text or not text.strip():
        return None
    lines = text.split("\n")
    section_starts = _find_section_starts(lines)
    if len(section_starts) < REGULATION_MIN_SECTIONS:
        return None
    section_titles_dict = dict(SECTION_TITLES)
    first_body = section_starts[0][1]

    out_lines = list(RULES_OF_CHUNKING)
    for i in range(first_body):
        out_lines.append(lines[i])

    for idx, (num, line_idx, _) in enumerate(section_starts):
        title = section_titles_dict.get(num, f"Раздел {num}")
        out_lines.append("")
        out_lines.append(f"## РАЗДЕЛ {num}. {title}")
        out_lines.append("")
        next_start = section_starts[idx + 1][1] if idx + 1 < len(section_starts) else len(lines)
        for j in range(line_idx, next_start):
            line = lines[j]
            stripped = line.strip()
            is_sub = (
                len(stripped) > 5
                and len(stripped) < 120
                and not re.match(r"^\d+[\.\)]\s+", stripped)
                and not re.match(r"^[-●•]\s", stripped)
                and any(re.match(p, stripped) for p in SUBSECTION_HEADERS)
            )
            if is_sub and j > line_idx:
                out_lines.append("")
                out_lines.append(f"### ПОДРАЗДЕЛ: {stripped}")
                out_lines.append("")
            else:
                out_lines.append(line)

    return "\n".join(out_lines)


def prepare_for_rag(file_content: bytes, filename: str) -> Optional[tuple[bytes, str]]:
    """
    Единая точка входа: для CSV — FAQ-преп, для TXT/PDF/DOCX/MD — регламент-преп по извлечённому тексту.
    Возвращает (md_bytes, new_filename) при успехе, иначе None.
    """
    name_lower = filename.lower()
    base_name = Path(filename).stem
    new_name = base_name + "_prepared.md"

    if name_lower.endswith(".csv"):
        md_str = prepare_faq_csv_to_md(file_content)
        if not md_str:
            return None
        return (md_str.encode("utf-8"), new_name)

    if name_lower.endswith(".txt"):
        try:
            text = file_content.decode("utf-8", errors="replace")
        except Exception:
            return None
        md_str = prepare_regulation_txt_to_md(text)
        if not md_str:
            return None
        return (md_str.encode("utf-8"), new_name)

    if name_lower.endswith(".pdf") or name_lower.endswith(".docx") or name_lower.endswith((".md", ".markdown")):
        from app.services.document_processor import extract_text
        try:
            extracted = extract_text(file_content, filename)
        except Exception:
            return None
        if not extracted or not extracted.strip():
            return None
        md_str = prepare_regulation_txt_to_md(extracted)
        if not md_str:
            return None
        return (md_str.encode("utf-8"), new_name)

    return None
