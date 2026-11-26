from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from common import ROOT_DIR, load_json, load_settings, parse_date, save_json, setup_logging


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


def build_post(
    items: list[dict],
    run_at: str,
    timezone_name: str,
    source_stream: str,
) -> str:
    lines: list[str] = []

    run_at_display = format_iso_datetime(run_at, timezone_name)

    if source_stream == "vs_fallback":
        title = "Щоденний дайджест постанов Верховного Суду"
        intro = (
            "Добірка постанов Верховного Суду з пріоритетних категорій."
        )
    else:
        title = "Щоденний дайджест постанов Великої Палати ВС"
        intro = None

    lines.append(title)
    if run_at_display:
        lines.append(f"Підготовлено: {run_at_display}")
    lines.append("")

    if intro:
        lines.append(intro)
        lines.append("")

    lines.append(f"Постанов у добірці: {len(items)}")
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


def publication_path() -> Path:
    return ROOT_DIR / "data" / "state" / "posted_doc_ids.json"


def processed_ids() -> set[str]:
    state = load_json(ROOT_DIR / "data" / "state" / "processed_doc_ids.json", default={})
    return read_ids(state.get("processed_doc_ids", []), "processed_doc_ids")


def read_ids(values, name: str) -> set[str]:
    if not isinstance(values, list) or any(not isinstance(value, str) or not value.isascii() or not value.isdecimal() for value in values):
        raise RuntimeError(f"Некоректне поле {name}")
    return {str(value).strip() for value in values}


def save_publication_state(state: dict) -> None:
    path = publication_path()
    temp = path.with_suffix(".tmp")
    save_json(temp, state)
    temp.replace(path)


def load_publication_state(*, initialize: bool = False) -> dict:
    state = load_json(publication_path(), default={})
    if not isinstance(state, dict):
        raise RuntimeError("Некоректний posted_doc_ids.json")
    posted = read_ids(state.get("posted_doc_ids", []), "posted_doc_ids")
    flight = state.get("in_flight", [])
    if len(read_ids(flight, "in_flight")) != len(flight):
        raise RuntimeError("Дублікати у in_flight")
    if posted.intersection(flight):
        raise RuntimeError("in_flight перетинається з posted_doc_ids")
    if state.get("initialized") is not True:
        if not initialize or flight:
            raise RuntimeError("Стан публікації не ініціалізовано")
        # Старий state був порожнім. Історичні аналізи не публікуємо повторно.
        if not posted:
            posted = processed_ids()
            logging.warning("Міграція: %s історичних аналізів вважаємо опублікованими; фактичну доставку не перевірено", len(posted))
        state = {"initialized": True, "posted_doc_ids": sorted(posted), "in_flight": []}
        save_publication_state(state)
    if "posted_doc_ids" not in state or "in_flight" not in state:
        raise RuntimeError("Неповний стан публікації")
    return state


def load_analysis(doc_id: str) -> dict:
    path = ROOT_DIR / "data" / "processed" / "analysis" / f"{doc_id}.json"
    item = load_json(path, default=None)
    if not isinstance(item, dict) or str(item.get("doc_id", "")) != doc_id or not str(item.get("telegram_line") or "").strip():
        raise RuntimeError(f"Відсутній або некоректний аналіз doc_id={doc_id}")
    return item


def stream_of(item: dict) -> str:
    stream = item.get("source_stream")
    if stream in ("vp", "vs_fallback"):
        return stream
    if str(item.get("court_code", "")) == "9951":
        return "vp"
    if str(item.get("court_code", "")) in ("9911", "9921", "9931"):
        return "vs_fallback"
    raise RuntimeError(f"Невідомий source_stream для doc_id={item.get('doc_id')}")


def select_pending(state: dict, settings: dict) -> list[str]:
    pending = processed_ids() - read_ids(state["posted_doc_ids"], "posted_doc_ids")
    if not pending:
        return []
    latest = load_json(ROOT_DIR / "data" / "state" / "last_daily_analyzed_doc_ids.json", default={})
    order = {str(doc_id): i for i, doc_id in enumerate(latest.get("doc_ids", []))}
    items = [load_analysis(doc_id) for doc_id in pending]
    # Найстаріша непублікована добірка першою; порядок останнього run зберігаємо.
    items.sort(key=lambda item: (str(item.get("analyzed_at") or ""), order.get(str(item["doc_id"]), len(order)), str(item["doc_id"])))
    stream = stream_of(items[0])
    analyzed_at = str(items[0].get("analyzed_at") or "")
    key = "vs_fallback_max_docs_per_run" if stream == "vs_fallback" else "max_docs_per_run"
    limit = min(3, max(1, int(settings.get(key, 3))))
    return [str(item["doc_id"]) for item in items if stream_of(item) == stream and str(item.get("analyzed_at") or "") == analyzed_at][:limit]


def render_batch(doc_ids: list[str], settings: dict) -> str:
    items = [load_analysis(doc_id) for doc_id in doc_ids]
    if not items:
        return ""
    stream = stream_of(items[0])
    if any(stream_of(item) != stream for item in items):
        raise RuntimeError("У добірці змішані джерела")
    return build_post(items, str(items[0].get("analyzed_at") or ""), settings.get("timezone", "Europe/Kyiv"), stream)


def set_pending_output(has_pending: bool) -> None:
    path = os.getenv("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"has_pending={str(has_pending).lower()}\n")


def should_skip_scheduled_run(settings: dict, *, now: datetime | None = None) -> bool:
    """Skip only scheduled runs after a confirmed publication on the local date."""
    if os.getenv("GITHUB_EVENT_NAME") != "schedule":
        return False

    state = load_json(publication_path(), default={})
    if not isinstance(state, dict):
        raise RuntimeError("Некоректний posted_doc_ids.json")
    # Не приховуємо неоднозначний результат навіть після іншої публікації сьогодні.
    if state.get("in_flight"):
        raise RuntimeError("Невизначений результат попередньої публікації. Перевірте in_flight у posted_doc_ids.json")
    if state.get("initialized") is not True:
        return False
    last_published_at = state.get("last_published_at")
    if not last_published_at:
        return False  # Сумісність зі старим state: дату не вигадуємо.
    try:
        published = datetime.fromisoformat(last_published_at)
        if published.tzinfo is None:
            raise ValueError("timezone required")
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Некоректне поле last_published_at у posted_doc_ids.json") from exc
    tz = ZoneInfo(settings.get("timezone", "Europe/Kyiv"))
    local_now = datetime.now(tz) if now is None else now.astimezone(tz)
    if published > local_now:
        raise RuntimeError("last_published_at містить майбутню дату публікації")
    return published.astimezone(tz).date() == local_now.date()


def check_daily_guard() -> None:
    setup_logging()
    skip = should_skip_scheduled_run(load_settings())
    path = os.getenv("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"skip_daily={str(skip).lower()}\n")
    if skip:
        logging.info("Дайджест сьогодні вже опубліковано. Автоматичний запуск пропущено.")
    else:
        logging.info("Обмеження щоденної публікації не блокує цей запуск")


def main() -> None:
    setup_logging()
    settings = load_settings()
    state = load_publication_state(initialize=True)
    output_path = ROOT_DIR / "outputs" / "digest" / "daily_telegram_post.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("", encoding="utf-8")

    if state.get("in_flight"):
        raise RuntimeError("Невизначений результат попередньої публікації. Перевірте Telegram та in_flight у posted_doc_ids.json: " + ", ".join(state["in_flight"]))
    if not settings.get("telegram_enabled", False):
        logging.info("Telegram вимкнено; нові аналізи залишатимуться у черзі")
        set_pending_output(False)
        return

    doc_ids = select_pending(state, settings)
    if not doc_ids:
        logging.info("Немає неопублікованих аналізів")
        set_pending_output(False)
        return

    output_path.write_text(render_batch(doc_ids, settings), encoding="utf-8")
    # Workflow зобов'язаний закомітити цей стан ДО виклику Telegram.
    state["in_flight"] = doc_ids
    save_publication_state(state)
    set_pending_output(True)
    logging.info("Підготовлено %s аналізів до публікації: %s", len(doc_ids), ", ".join(doc_ids))


if __name__ == "__main__":
    if sys.argv[1:] == ["--check-daily-guard"]:
        check_daily_guard()
    elif not sys.argv[1:]:
        main()
    else:
        raise SystemExit("Usage: build_daily_telegram_post.py [--check-daily-guard]")
