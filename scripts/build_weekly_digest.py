from __future__ import annotations

import json
import logging
from pathlib import Path

from common import ROOT_DIR, load_json, load_settings, setup_logging


def load_analysis_files(directory: Path) -> list[dict]:
    items = []
    for path in sorted(directory.glob("*.json")):
        try:
            items.append(load_json(path, default={}))
        except json.JSONDecodeError as exc:
            logging.exception("Не вдалося прочитати %s: %s", path.name, exc)
    return items


def build_markdown(items: list[dict], title: str) -> str:
    lines = [f"# {title}", ""]
    lines.append(f"За період опрацьовано {len(items)} постанов(и) Великої Палати Верховного Суду.")
    lines.append("")
    for idx, item in enumerate(items, start=1):
        lines.extend([
            f"## {idx}. Справа № {item.get('cause_num', 'н/д')}",
            f"- Дата ухвалення: {item.get('adjudication_date', 'н/д')}",
            f"- Дата оприлюднення: {item.get('date_publ', 'н/д')}",
            f"- Коротко: {item.get('short_summary', '')}",
            f"- Ключова позиція: {item.get('key_position', '')}",
            f"- Для практики: {item.get('practical_value', '')}",
            f"- Суспільне значення: {item.get('public_value', '')}",
            f"- Теги: {', '.join(item.get('topic_tags', []))}",
            f"- Текст: {item.get('doc_url', '')}",
            "",
        ])
    return "\n".join(lines).strip() + "\n"


def build_telegram_post(items: list[dict], title: str) -> str:
    lines = [title, ""]
    lines.append(f"За тиждень оприлюднено {len(items)} постанов(и) Великої Палати Верховного Суду.")
    lines.append("")
    for idx, item in enumerate(items, start=1):
        lines.append(f"{idx}) Справа № {item.get('cause_num', 'н/д')}")
        telegram_line = item.get("telegram_line", "").strip()
        if telegram_line:
            lines.append(telegram_line)
        key_position = item.get("key_position", "").strip()
        if key_position:
            lines.append(f"Ключова позиція: {key_position}")
        doc_url = item.get("doc_url", "").strip()
        if doc_url:
            lines.append(f"Текст: {doc_url}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    setup_logging()
    settings = load_settings()

    analysis_dir = ROOT_DIR / "data" / "processed" / "analysis"
    output_dir = ROOT_DIR / "outputs" / "digest"
    output_dir.mkdir(parents=True, exist_ok=True)

    items = load_analysis_files(analysis_dir)
    items.sort(key=lambda x: (x.get("date_publ", ""), x.get("cause_num", "")))

    title_template = settings.get(
        "telegram_post_title_template",
        "Дайджест постанов Великої Палати ВС за {date_from}–{date_to}",
    )
    if items:
        dates = sorted([item.get("date_publ", "") for item in items if item.get("date_publ")])
        date_from = dates[0] if dates else ""
        date_to = dates[-1] if dates else ""
    else:
        date_from = ""
        date_to = ""
    title = title_template.replace("{date_from}", date_from).replace("{date_to}", date_to)

    markdown_text = build_markdown(items, title)
    telegram_text = build_telegram_post(items, title)

    (output_dir / "weekly_digest.md").write_text(markdown_text, encoding="utf-8")
    (output_dir / "telegram_post.txt").write_text(telegram_text, encoding="utf-8")
    logging.info("Згенеровано weekly_digest.md і telegram_post.txt")


if __name__ == "__main__":
    main()
