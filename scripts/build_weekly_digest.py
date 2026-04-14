from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from common import ROOT_DIR, iter_tsv_rows, load_json, load_settings, parse_date, setup_logging


def format_date_for_display(value: str) -> str:
    date_obj = parse_date(value)
    if not date_obj:
        return value or "—"
    return date_obj.strftime("%d.%m.%Y")


def safe_text(value: object, fallback: str = "—") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def build_period(settings: dict) -> tuple[str, str]:
    tz_name = settings.get("timezone", "Europe/Kyiv")
    lookback_days = int(settings.get("lookback_days", 7))
    today = datetime.now(ZoneInfo(tz_name)).date()
    date_from = today - timedelta(days=lookback_days - 1)
    date_to = today
    return date_from.strftime("%d.%m.%Y"), date_to.strftime("%d.%m.%Y")


def load_current_rows(interim_path: Path) -> list[dict]:
    if not interim_path.exists():
        return []
    return list(iter_tsv_rows(interim_path))


def build_markdown_digest(
    analyses: list[dict],
    total_found: int,
    period_from: str,
    period_to: str,
) -> str:
    lines: list[str] = []
    lines.append(f"# Дайджест постанов Великої Палати ВС за {period_from}–{period_to}")
    lines.append("")

    if not analyses:
        lines.append(
            "Аналітичні матеріали не сформовані: постанови були знайдені, але Gemini-аналіз не згенеровано."
            if total_found > 0
            else "За вказаний період постанов Великої Палати ВС не знайдено."
        )
        lines.append("")
        lines.append(f"- Знайдено постанов у вибірці: **{total_found}**")
        lines.append(f"- Проаналізовано постанов: **{len(analyses)}**")
        return "\n".join(lines)

    lines.append(f"За період знайдено **{total_found}** постанов, з них проаналізовано **{len(analyses)}**.")
    lines.append("")

    for idx, item in enumerate(analyses, start=1):
        lines.append(f"## {idx}. Справа № {safe_text(item.get('cause_num'))}")
        lines.append("")
        lines.append(f"- **Дата ухвалення:** {format_date_for_display(str(item.get('adjudication_date', '')))}")
        lines.append(f"- **Дата оприлюднення:** {format_date_for_display(str(item.get('date_publ', '')))}")
        lines.append(f"- **Коротко:** {safe_text(item.get('short_summary'))}")
        lines.append(f"- **Ключова позиція:** {safe_text(item.get('key_position'))}")
        lines.append(f"- **Практичне значення:** {safe_text(item.get('practical_value'))}")
        lines.append(f"- **Суспільне значення:** {safe_text(item.get('public_value'))}")

        tags = item.get("topic_tags", [])
        tag_line = ", ".join(str(tag).strip() for tag in tags if str(tag).strip())
        if tag_line:
            lines.append(f"- **Теги:** {tag_line}")

        lines.append(f"- **Текст постанови:** {safe_text(item.get('doc_url'))}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def build_telegram_post(
    analyses: list[dict],
    total_found: int,
    period_from: str,
    period_to: str,
) -> str:
    lines: list[str] = []
    lines.append(f"Дайджест постанов Великої Палати ВС за {period_from}–{period_to}")
    lines.append("")

    if not analyses:
        if total_found > 0:
            lines.append(
                "За цей період постанови знайдено, але аналітичні блоки ще не сформовані через технічну помилку під час Gemini-аналізу."
            )
            lines.append(f"Кількість знайдених постанов: {total_found}.")
        else:
            lines.append("За цей період нових постанов Великої Палати ВС не знайдено.")
        return "\n".join(lines).strip() + "\n"

    lines.append(f"За тиждень оприлюднено {total_found} постанов Великої Палати Верховного Суду.")
    lines.append("")

    for idx, item in enumerate(analyses, start=1):
        lines.append(f"{idx}) Справа № {safe_text(item.get('cause_num'))}")
        lines.append(safe_text(item.get("telegram_line")))
        doc_url = safe_text(item.get("doc_url"), "")
        if doc_url:
            lines.append(f"Текст: {doc_url}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def main() -> None:
    setup_logging()
    settings = load_settings()

    interim_path = ROOT_DIR / "data" / "interim" / "vp_last7.csv"
    analysis_dir = ROOT_DIR / "data" / "processed" / "analysis"
    output_dir = ROOT_DIR / "outputs" / "digest"
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = load_current_rows(interim_path)
    total_found = len(rows)

    analyses: list[dict] = []
    for row in rows:
        doc_id = str(row.get("doc_id", "")).strip()
        if not doc_id:
            continue

        analysis_path = analysis_dir / f"{doc_id}.json"
        if not analysis_path.exists():
            continue

        item = load_json(analysis_path, default={})
        if not item:
            continue

        analyses.append(item)

    analyses.sort(
        key=lambda x: (
            parse_date(str(x.get("date_publ", ""))) or datetime.min.date(),
            str(x.get("cause_num", "")),
        ),
        reverse=True,
    )

    period_from, period_to = build_period(settings)

    digest_md = build_markdown_digest(
        analyses=analyses,
        total_found=total_found,
        period_from=period_from,
        period_to=period_to,
    )
    telegram_post = build_telegram_post(
        analyses=analyses,
        total_found=total_found,
        period_from=period_from,
        period_to=period_to,
    )

    digest_path = output_dir / "weekly_digest.md"
    telegram_path = output_dir / "telegram_post.txt"

    digest_path.write_text(digest_md, encoding="utf-8")
    telegram_path.write_text(telegram_post, encoding="utf-8")

    logging.info("Знайдено постанов у vp_last7.csv: %s", total_found)
    logging.info("Знайдено JSON-аналізів для дайджесту: %s", len(analyses))
    logging.info("Сформовано %s", digest_path)
    logging.info("Сформовано %s", telegram_path)


if __name__ == "__main__":
    main()
