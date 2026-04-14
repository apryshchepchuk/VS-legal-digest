from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from common import ROOT_DIR, load_json, load_settings, parse_date, setup_logging


def format_date_for_display(value: str) -> str:
    date_obj = parse_date(value)
    if not date_obj:
        return value or "—"
    return date_obj.strftime("%d.%m.%Y")


def safe_text(value: object, fallback: str = "—") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def parse_datetime_safe(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def build_period(settings: dict) -> tuple[str, str, datetime]:
    tz_name = settings.get("timezone", "Europe/Kyiv")
    digest_lookback_days = int(settings.get("digest_lookback_days", 7))

    now = datetime.now(ZoneInfo(tz_name))
    cutoff = now - timedelta(days=digest_lookback_days)

    return (
        cutoff.date().strftime("%d.%m.%Y"),
        now.date().strftime("%d.%m.%Y"),
        cutoff,
    )


def build_markdown_digest(
    analyses: list[dict],
    period_from: str,
    period_to: str,
) -> str:
    lines: list[str] = []
    lines.append(f"# Дайджест постанов Великої Палати ВС за {period_from}–{period_to}")
    lines.append("")

    if not analyses:
        lines.append("За цей період нових готових аналізів постанов не сформовано.")
        return "\n".join(lines) + "\n"

    lines.append(f"За період сформовано **{len(analyses)}** нових аналізів повних текстів постанов.")
    lines.append("")

    for idx, item in enumerate(analyses, start=1):
        lines.append(f"## {idx}. Справа № {safe_text(item.get('cause_num'))}")
        lines.append("")
        lines.append(f"- **Дата ухвалення:** {format_date_for_display(str(item.get('adjudication_date', '')))}")
        lines.append(f"- **Дата публікації:** {format_date_for_display(str(item.get('date_publ', '')))}")
        lines.append(f"- **Проаналізовано:** {safe_text(item.get('analyzed_at'))}")
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
    period_from: str,
    period_to: str,
) -> str:
    lines: list[str] = []
    lines.append(f"Дайджест постанов Великої Палати ВС за {period_from}–{period_to}")
    lines.append("")

    if not analyses:
        lines.append("За цей період нових готових аналізів повних текстів постанов не сформовано.")
        return "\n".join(lines).strip() + "\n"

    lines.append(f"За тиждень підготовлено {len(analyses)} нових аналізів повних текстів постанов Великої Палати Верховного Суду.")
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

    analysis_dir = ROOT_DIR / "data" / "processed" / "analysis"
    output_dir = ROOT_DIR / "outputs" / "digest"
    output_dir.mkdir(parents=True, exist_ok=True)

    period_from, period_to, cutoff = build_period(settings)

    analyses: list[dict] = []

    if analysis_dir.exists():
        for path in analysis_dir.glob("*.json"):
            item = load_json(path, default={})
            if not item:
                continue

            analyzed_at = parse_datetime_safe(str(item.get("analyzed_at", "")))
            if not analyzed_at:
                continue
            if analyzed_at < cutoff:
                continue

            analyses.append(item)

    analyses.sort(
        key=lambda x: (
            parse_datetime_safe(str(x.get("analyzed_at", ""))) or datetime.min.replace(tzinfo=cutoff.tzinfo),
            parse_date(str(x.get("date_publ", ""))) or datetime.min.date(),
            str(x.get("cause_num", "")),
        ),
        reverse=True,
    )

    digest_md = build_markdown_digest(
        analyses=analyses,
        period_from=period_from,
        period_to=period_to,
    )
    telegram_post = build_telegram_post(
        analyses=analyses,
        period_from=period_from,
        period_to=period_to,
    )

    digest_path = output_dir / "weekly_digest.md"
    telegram_path = output_dir / "telegram_post.txt"

    digest_path.write_text(digest_md, encoding="utf-8")
    telegram_path.write_text(telegram_post, encoding="utf-8")

    logging.info("Знайдено JSON-аналізів за digest window: %s", len(analyses))
    logging.info("Сформовано %s", digest_path)
    logging.info("Сформовано %s", telegram_path)


if __name__ == "__main__":
    main()
