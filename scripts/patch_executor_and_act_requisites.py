#!/usr/bin/env python3
"""
Извлекает реквизиты Исполнителя из шаблона в config, вносит в DOCX плейсхолдеры:
- раздел 8: после «Исполнитель:» — {{executor_requisites}};
- акт: под «Заказчик» — {{customer_requisites}}; блок Исполнителя — {{executor_requisites}}.
Запуск из корня проекта: python scripts/patch_executor_and_act_requisites.py
"""

import json
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from docx import Document

from app.config import TEMPLATES_DIR


def main():
    config_path = TEMPLATES_DIR / "config.json"
    doc_path = TEMPLATES_DIR / "accounting_contract_template.docx"
    if not doc_path.is_file():
        print(f"Файл не найден: {doc_path}")
        return
    if not config_path.is_file():
        print(f"Конфиг не найден: {config_path}")
        return

    doc = Document(doc_path)
    paragraphs = doc.paragraphs

    # Реквизиты Исполнителя берём из акта (параграфы 172–181 до подписей), без заголовка «Исполнитель:»
    executor_lines = []
    for i in range(172, min(183, len(paragraphs))):
        t = paragraphs[i].text
        if t.strip().startswith("_______/"):
            break
        if t.strip():
            executor_lines.append(t.strip())
    executor_requisites_text = "\n".join(executor_lines) if executor_lines else ""
    if executor_requisites_text.startswith("Исполнитель:"):
        executor_requisites_text = executor_requisites_text.split("\n", 1)[-1].strip()

    # Обновить config.json: добавить executor_requisites в шаблон accounting_contract
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    for t in config.get("templates", []):
        if t.get("id") == "accounting_contract":
            t["executor_requisites"] = executor_requisites_text
            if "executor_requisites" not in [f.get("key") for f in t.get("fields", [])]:
                t.setdefault("fields", []).append({
                    "key": "executor_requisites",
                    "label": "Реквизиты Исполнителя",
                })
            break
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print("config.json: добавлен executor_requisites для accounting_contract")

    # Раздел 8: после «Исполнитель:» (индекс 107) — один параграф с плейсхолдером
    if len(paragraphs) > 108:
        paragraphs[108].text = "{{executor_requisites}}"
        if len(paragraphs) > 109 and not paragraphs[109].text.strip().startswith("_______"):
            paragraphs[109].text = ""

    # Акт: под «Заказчик» (без двоеточия, в акте индекс > 169) следующий параграф — customer_requisites
    for i in range(170, len(paragraphs) - 1):
        if paragraphs[i].text.strip() == "Заказчик":
            paragraphs[i + 1].text = "{{customer_requisites}}"
            break

    # Акт: блок Исполнителя (с 172) заменить на «Исполнитель:» + один параграф {{executor_requisites}}
    for i in range(len(paragraphs)):
        if "Исполнитель:" in paragraphs[i].text and i >= 170:
            paragraphs[i].text = "Исполнитель:\n{{executor_requisites}}"
            j = i + 1
            while j < len(paragraphs) and paragraphs[j].text.strip() and not paragraphs[j].text.strip().startswith("_______/"):
                paragraphs[j].text = ""
                j += 1
            break

    doc.save(doc_path)
    print("accounting_contract_template.docx: обновлён (раздел 8 и акт)")


if __name__ == "__main__":
    main()
