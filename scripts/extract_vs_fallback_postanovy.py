from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from common import ROOT_DIR, load_settings, parse_date, setup_logging


def read_tsv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Не знайдено {path}")

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader)


def normalize_string_list(values: object, default: list[str]) -> list[str]:
    if not isinstance(values, list) or not values:
        return default

    result: list[str] = []
    seen: set[str] = set()

    for value in values:
        item = str(value).strip()
        if not item:
            continue
        if item not in seen:
            seen.add(item)
            result.append(item)

    return result or default


def normalize_category_codes(values: object) -> set[str]:
    if not isinstance(values, list):
        return set()

    result: set[str] = set()
    for value in values:
        item = str(value).strip()
        if item:
            result.add(item)

    return result


def build_courts_map(courts_rows: list[dict]) -> dict[str, str]:
    result: dict[str, str] = {}

    for row in courts_rows:
        court_code = str(row.get("court_code", "")).strip()
        court_name = str(row.get("name", "")).strip()

        if court_code:
            result[court_code] = court_name

    return result


def main() -> None:
    setup_logging()
    settings = load_settings()

    tz_name = settings.get("timezone", "Europe/Kyiv")
    lookback_days = int(
        os.getenv(
            "LOOKBACK_DAYS_OVERRIDE",
            str(settings.get("selection_lookback_days", 60)),
        )
    )

    target_judgment_code = str(settings.get("target_judgment_code", "2")).strip()

    vs_fallback_court_codes = set(
        normalize_string_list(
            settings.get("vs_fallback_court_codes", ["9911", "9921", "9931"]),
            ["9911", "9921", "9931"],
        )
    )
    vs_extra_category_codes = normalize_category_codes(
        settings.get("vs_extra_category_codes", [])
    )

    if not vs_extra_category_codes:
        raise RuntimeError(
            "У settings.json не задано vs_extra_category_codes для fallback-підбору постанов ВС"
        )

    archives_dir = ROOT_DIR / "data" / "raw" / "archives"
    input_path = archives_dir / "documents.csv"
    courts_path = archives_dir / "courts.csv"

    output_path = ROOT_DIR / "data" / "interim" / "vp_candidates.csv"
    debug_output_path = ROOT_DIR / "data" / "interim" / "vs_fallback_candidates.csv"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Не знайдено {input_path}")
    if not courts_path.exists():
        raise FileNotFoundError(f"Не знайдено {courts_path}")

    courts_rows = read_tsv(courts_path)
    courts_map = build_courts_map(courts_rows)

    today = datetime.now(ZoneInfo(tz_name)).date()
    cutoff = today - timedelta(days=lookback_days - 1)

    total_rows = 0
    matched_court_rows = 0
    excluded_nonwhitelist_category = 0
    selected_rows: list[dict] = []

    with input_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")

        for row in reader:
            total_rows += 1

            court_code = str(row.get("court_code", "")).strip()
            judgment_code = str(row.get("judgment_code", "")).strip()
            status = str(row.get("status", "")).strip()
            category_code = str(row.get("category_code", "")).strip()
            date_publ_raw = str(row.get("date_publ", "")).strip()
            date_publ = parse_date(date_publ_raw)

            if judgment_code != target_judgment_code:
                continue
            if status != "1":
                continue
            if not date_publ:
                continue
            if date_publ < cutoff:
                continue
            if court_code not in vs_fallback_court_codes:
                continue

            matched_court_rows += 1

            if category_code not in vs_extra_category_codes:
                excluded_nonwhitelist_category += 1
                continue

            selected_rows.append(
                {
                    "doc_id": str(row.get("doc_id", "")).strip(),
                    "cause_num": str(row.get("cause_num", "")).strip(),
                    "adjudication_date": str(row.get("adjudication_date", "")).strip(),
                    "receipt_date": str(row.get("receipt_date", "")).strip(),
                    "date_publ": date_publ_raw,
                    "judge": str(row.get("judge", "")).strip(),
                    "doc_url": str(row.get("doc_url", "")).strip(),
                    "court_code": court_code,
                    "court_name": courts_map.get(court_code, ""),
                    "judgment_code": judgment_code,
                    "justice_kind": str(row.get("justice_kind", "")).strip(),
                    "category_code": category_code,
                    "status": status,
                }
            )

    fieldnames = [
        "doc_id",
        "cause_num",
        "adjudication_date",
        "receipt_date",
        "date_publ",
        "judge",
        "doc_url",
        "court_code",
        "court_name",
        "judgment_code",
        "justice_kind",
        "category_code",
        "status",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in selected_rows:
            writer.writerow(row)

    with debug_output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in selected_rows:
            writer.writerow(row)

    logging.info("Перевірено рядків у documents.csv: %s", total_rows)
    logging.info("Період fallback-відбору постанов ВС: %s днів", lookback_days)
    logging.info("Fallback court codes: %s", ", ".join(sorted(vs_fallback_court_codes)))
    logging.info("Whitelist категорій для fallback: %s кодів", len(vs_extra_category_codes))
    logging.info("Рядків, що пройшли court-level fallback-фільтр: %s", matched_court_rows)
    logging.info("Виключено через category_code поза whitelist: %s", excluded_nonwhitelist_category)
    logging.info("Відібрано fallback-постанов ВС до candidate pool: %s", len(selected_rows))
    logging.info("Сформовано %s", output_path)
    logging.info("Сформовано %s", debug_output_path)


if __name__ == "__main__":
    main()
