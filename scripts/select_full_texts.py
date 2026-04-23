from __future__ import annotations

import csv
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from common import ROOT_DIR, load_settings, parse_date, setup_logging


def read_tsv(path: Path) -> list[dict]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader)


def write_tsv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            delimiter="\t",
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def safe_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def load_char_count(text_path: Path) -> int:
    if not text_path.exists():
        return 0
    text = text_path.read_text(encoding="utf-8", errors="ignore")
    return len(text.strip())


def build_group_key(row: dict) -> tuple[str, str]:
    cause_num = str(row.get("cause_num", "")).strip()
    adjudication_date = str(row.get("adjudication_date", "")).strip()
    return cause_num, adjudication_date


def sort_group_rows(rows: list[dict]) -> list[dict]:
    def sort_key(row: dict):
        date_publ_raw = str(row.get("date_publ", "")).strip()
        date_publ = parse_date(date_publ_raw)
        return (
            date_publ or datetime.min.date(),
            safe_int(row.get("doc_id", 0)),
        )

    return sorted(rows, key=sort_key, reverse=True)


def should_select_single(row: dict, min_chars_single_full: int) -> bool:
    return safe_int(row.get("char_count", 0)) >= min_chars_single_full


def should_select_latest_from_group(
    latest_row: dict,
    previous_rows: list[dict],
    min_chars_any_text: int,
    min_chars_single_full: int,
    min_growth_ratio_for_later_version: float,
) -> bool:
    latest_chars = safe_int(latest_row.get("char_count", 0))

    if latest_chars < min_chars_any_text:
        return False

    if latest_chars >= min_chars_single_full:
        return True

    if not previous_rows:
        return False

    previous_max_chars = max(safe_int(r.get("char_count", 0)) for r in previous_rows)

    if previous_max_chars <= 0:
        return latest_chars >= min_chars_any_text

    growth_ratio = latest_chars / previous_max_chars

    return growth_ratio >= min_growth_ratio_for_later_version


def main() -> None:
    setup_logging()
    settings = load_settings()

    tz_name = settings.get("timezone", "Europe/Kyiv")
    digest_lookback_days = int(settings.get("digest_lookback_days", 7))

    min_chars_any_text = int(settings.get("min_chars_any_text", 6000))
    min_chars_single_full = int(settings.get("min_chars_single_full", 12000))
    min_growth_ratio_for_later_version = float(
        settings.get("min_growth_ratio_for_later_version", 1.5)
    )

    input_path = ROOT_DIR / "data" / "interim" / "vp_candidates.csv"
    text_dir = ROOT_DIR / "data" / "processed" / "text"

    selected_path = ROOT_DIR / "data" / "interim" / "vp_selected_for_analysis.csv"
    weekly_path = ROOT_DIR / "data" / "interim" / "vp_last7.csv"

    fieldnames = [
        "doc_id",
        "cause_num",
        "adjudication_date",
        "receipt_date",
        "date_publ",
        "judge",
        "doc_url",
        "court_code",
        "judgment_code",
        "justice_kind",
        "category_code",
        "status",
        "char_count",
        "selection_reason",
    ]

    if not input_path.exists():
        raise FileNotFoundError(f"Не знайдено {input_path}")

    rows = read_tsv(input_path)
    if not rows:
        logging.info("У %s немає записів", input_path)
        write_tsv(selected_path, [], fieldnames)
        write_tsv(weekly_path, [], fieldnames)
        return

    # Додаємо char_count для кожного doc_id
    enriched_rows: list[dict] = []
    for row in rows:
        doc_id = str(row.get("doc_id", "")).strip()
        text_path = text_dir / f"{doc_id}.txt"
        char_count = load_char_count(text_path)

        enriched = dict(row)
        enriched["char_count"] = str(char_count)
        enriched_rows.append(enriched)

    # Групуємо за cause_num + adjudication_date
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in enriched_rows:
        group_key = build_group_key(row)
        groups[group_key].append(row)

    selected_rows: list[dict] = []
    skipped_groups = 0

    for _group_key, group_rows in groups.items():
        sorted_rows = sort_group_rows(group_rows)

        if len(sorted_rows) == 1:
            only_row = dict(sorted_rows[0])
            if should_select_single(
                only_row,
                min_chars_single_full=min_chars_single_full,
            ):
                only_row["selection_reason"] = "single_record_large_text"
                selected_rows.append(only_row)
            else:
                skipped_groups += 1
            continue

        latest_row = dict(sorted_rows[0])
        previous_rows = sorted_rows[1:]

        if should_select_latest_from_group(
            latest_row=latest_row,
            previous_rows=previous_rows,
            min_chars_any_text=min_chars_any_text,
            min_chars_single_full=min_chars_single_full,
            min_growth_ratio_for_later_version=min_growth_ratio_for_later_version,
        ):
            latest_row["selection_reason"] = "latest_publication_and_longer_text"
            selected_rows.append(latest_row)
        else:
            skipped_groups += 1

    # Відбираємо тільки ті selected_rows, у яких date_publ входить у останні digest_lookback_days днів
    today = datetime.now(ZoneInfo(tz_name)).date()
    weekly_cutoff = today - timedelta(days=digest_lookback_days - 1)

    weekly_rows: list[dict] = []
    for row in selected_rows:
        date_publ = parse_date(str(row.get("date_publ", "")).strip())
        if date_publ and date_publ >= weekly_cutoff:
            weekly_rows.append(row)

    # Сортування для стабільного виводу
    selected_rows = sort_group_rows(selected_rows)
    weekly_rows = sort_group_rows(weekly_rows)

    write_tsv(selected_path, selected_rows, fieldnames)
    write_tsv(weekly_path, weekly_rows, fieldnames)

    logging.info("Усього записів у vp_candidates.csv: %s", len(rows))
    logging.info("Усього груп cause_num+adjudication_date: %s", len(groups))
    logging.info("Відібрано записів для analysis pool: %s", len(selected_rows))
    logging.info("Відібрано записів для last7/digest: %s", len(weekly_rows))
    logging.info("Пропущено груп: %s", skipped_groups)
    logging.info("Сформовано %s", selected_path)
    logging.info("Сформовано %s", weekly_path)


if __name__ == "__main__":
    main()
