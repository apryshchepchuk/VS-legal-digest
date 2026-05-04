from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from common import ROOT_DIR, load_json, load_settings, parse_date, setup_logging


def safe_text(value: object, fallback: str = "—") -> str:
    text = str(value or "").strip()
    return text if text else fallback


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


def format_date_only(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    date_obj = parse_date(raw)
    if not date_obj:
        return raw

    return date_obj.strftime("%d.%m.%Y")


def get_source_stream_label(source_stream: str) -> str:
    if source_stream == "vs_fallback":
        return "Верховний Суд (fallback)"
    return "Велика Палата Верховного Суду"


def build_post(
    items: list[dict],
    run_at: str,
    timezone_name: str,
    source_stream: str,
    source_stream_label: str,
) -> str:
    lines: list[str] = []

    run_at_display = format_iso_datetime(run_at, timezone_name)

    if source_stream == "vs_fallback":
        title = "Щоденний дайджест постанов Верховного Суду"
        intro = (
            "Сьогодні нових постанов Великої Палати Верховного Суду не виявлено. "
            "Нижче — добірка постанов Верховного Суду."
        )
    else:
        title = "Щоденний дайджест постанов Великої Палати ВС"
        intro = None

    lines.append(title)
    if run_at_display:
        lines.append(f"Оновлення: {run_at_display}")
    lines.append("")

    if intro:
        lines.append(intro)
        lines.append("")

    lines.append(f"Джерело добірки: {source_stream_label}")
    lines.append(f"Сьогодні підготовлено {len(items)} нових аналізів.")
    lines.append("")

    for idx, item in enumerate(items, start=1):
        cause_num = safe_text(item.get("cause_num"))
        telegram_line = safe_text(item.get("telegram_line"))
        doc_url = safe_text(item.get("doc_url"), "")
        adjudication_date = format_date_only(str(item.get("adjudication_date", "")))
        date_publ = format_date_only(str(item.get("date_publ", "")))

        lines.append(f"{idx}) Справа № {cause_num}")

        if source_stream == "vs_fallback":
            court_name = safe_text(item.get("court_name"), "")
            if court_name:
                lines.append(f"Суд: {court_name}")

        if adjudication_date:
            lines.append(f"Дата постанови: {adjudication_date}")
        if date_publ:
            lines.append(f"Дата публікації: {date_publ}")

        lines.append(telegram_line)

        if doc_url:
            lines.append(f"Текст: {doc_url}")

        lines.append("")

    return "\n".join(lines).strip() + "\n"


def main() -> None:
    setup_logging()
    settings = load_settings()

    if not settings.get("telegram_enabled", False):
        logging.info("Telegram вимкнено в settings.json")
        return

    timezone_name = settings.get("timezone", "Europe/Kyiv")

    state_path = ROOT_DIR / "data" / "state" / "last_daily_analyzed_doc_ids.json"
    analysis_dir = ROOT_DIR / "data" / "processed" / "analysis"
    output_dir = ROOT_DIR / "outputs" / "digest"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "daily_telegram_post.txt"

    if not state_path.exists():
        logging.info("Файл %s відсутній, формувати Telegram-пост нічого", state_path)
        output_path.write_text("", encoding="utf-8")
        return

    state = load_json(state_path, default={})
    doc_ids = state.get("doc_ids", []) or []
    run_at = str(state.get("run_at", "")).strip()
    source_stream = str(state.get("source_stream", "")).strip() or "vp"
    source_stream_label = str(state.get("source_stream_label", "")).strip() or get_source_stream_label(source_stream)

    if not doc_ids:
        logging.info("Немає нових doc_id для Telegram-поста")
        output_path.write_text("", encoding="utf-8")
        return

    items: list[dict] = []

    for doc_id in doc_ids:
        doc_id_str = str(doc_id).strip()
        if not doc_id_str:
            continue

        path = analysis_dir / f"{doc_id_str}.json"
        item = load_json(path, default={})
        if not item:
            logging.warning("Не знайдено JSON-аналіз для doc_id=%s", doc_id_str)
            continue

        items.append(item)

    if not items:
        logging.info("Немає JSON-аналізів для побудови щоденного Telegram-поста")
        output_path.write_text("", encoding="utf-8")
        return

    # Якщо state не містив stream label, а item уже містить — беремо з першого item
    if not source_stream_label:
        source_stream_label = safe_text(items[0].get("source_stream_label"), get_source_stream_label(source_stream))

    post_text = build_post(
        items=items,
        run_at=run_at,
        timezone_name=timezone_name,
        source_stream=source_stream,
        source_stream_label=source_stream_label,
    )
    output_path.write_text(post_text, encoding="utf-8")

    logging.info("Сформовано %s", output_path)
    logging.info("У пост включено %s нових аналізів", len(items))
    logging.info("source_stream для Telegram-поста: %s", source_stream)


if __name__ == "__main__":
    main()
