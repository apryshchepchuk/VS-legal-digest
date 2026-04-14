from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from common import ROOT_DIR, load_json, load_settings, parse_date, setup_logging


def safe_text(value: object, fallback: str = "—") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def format_date_only(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    date_obj = parse_date(raw)
    if not date_obj:
        return raw

    return date_obj.strftime("%d.%m.%Y")


def format_iso_datetime(value: str, timezone_name: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return raw

    try:
        tz = ZoneInfo(timezone_name)
        if dt.tzinfo is not None:
            dt = dt.astimezone(tz)
    except Exception:
        pass

    return dt.strftime("%d.%m.%Y %H:%M")


def parse_datetime_safe(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def main() -> None:
    setup_logging()
    settings = load_settings()

    if not settings.get("telegram_enabled", False):
        logging.info("Telegram вимкнено в settings.json")
        return

    timezone_name = settings.get("timezone", "Europe/Kyiv")

    analysis_dir = ROOT_DIR / "data" / "processed" / "analysis"
    output_dir = ROOT_DIR / "outputs" / "digest"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "daily_telegram_post.txt"

    if not analysis_dir.exists():
        logging.info("Папка %s відсутня", analysis_dir)
        output_path.write_text("", encoding="utf-8")
        return

    items: list[dict] = []
    for path in analysis_dir.glob("*.json"):
        item = load_json(path, default={})
        if item:
            items.append(item)

    if not items:
        logging.info("Немає JSON-аналізів для архівного Telegram-поста")
        output_path.write_text("", encoding="utf-8")
        return

    items.sort(
        key=lambda x: (
            parse_datetime_safe(str(x.get("analyzed_at", ""))) or datetime.min.replace(tzinfo=ZoneInfo(timezone_name)),
            parse_date(str(x.get("date_publ", ""))) or datetime.min.date(),
            str(x.get("cause_num", "")),
        ),
        reverse=True,
    )

    now_display = datetime.now(ZoneInfo(timezone_name)).strftime("%d.%m.%Y %H:%M")

    lines: list[str] = []
    lines.append("Тестовий архівний дайджест постанов Великої Палати ВС")
    lines.append(f"Згенеровано: {now_display}")
    lines.append("")
    lines.append(f"Усього включено проаналізованих постанов: {len(items)}")
    lines.append("")

    for idx, item in enumerate(items, start=1):
        cause_num = safe_text(item.get("cause_num"))
        adjudication_date = format_date_only(str(item.get("adjudication_date", "")))
        date_publ = format_date_only(str(item.get("date_publ", "")))
        analyzed_at = format_iso_datetime(str(item.get("analyzed_at", "")), timezone_name)
        telegram_line = safe_text(item.get("telegram_line"))
        doc_url = safe_text(item.get("doc_url"), "")

        lines.append(f"{idx}) Справа № {cause_num}")

        if adjudication_date:
            lines.append(f"Дата постанови: {adjudication_date}")
        if date_publ:
            lines.append(f"Дата публікації: {date_publ}")
        if analyzed_at:
            lines.append(f"Проаналізовано: {analyzed_at}")

        lines.append(telegram_line)

        if doc_url:
            lines.append(f"Текст: {doc_url}")

        lines.append("")

    text = "\n".join(lines).strip() + "\n"
    output_path.write_text(text, encoding="utf-8")

    logging.info("Сформовано %s", output_path)
    logging.info("У тестовий архівний пост включено %s постанов", len(items))


if __name__ == "__main__":
    main()
