from __future__ import annotations

import logging
from pathlib import Path

from common import ROOT_DIR, get_date_range, iter_tsv_rows, load_settings, parse_date, setup_logging, write_tsv


FIELDNAMES = [
    "doc_id",
    "cause_num",
    "adjudication_date",
    "receipt_date",
    "date_publ",
    "judge",
    "doc_url",
    "court_code",
    "judgment_code",
    "status",
]


def main() -> None:
    setup_logging()
    settings = load_settings()

    source_path = ROOT_DIR / "data" / "raw" / "archives" / "documents.csv"
    output_path = ROOT_DIR / "data" / "interim" / "vp_last30.csv"

    if not source_path.exists():
        raise FileNotFoundError(f"Не знайдено {source_path}. Спершу запустіть fetch_dataset.py")

    lookback_days = int(settings.get("lookback_days", 7))
    target_court_code = str(settings.get("target_court_code", "9951"))
    target_judgment_code = str(settings.get("target_judgment_code", "2"))
    date_from, date_to = get_date_range(lookback_days)

    matched_rows = []
    total = 0

    for row in iter_tsv_rows(source_path):
        total += 1
        if row.get("court_code") != target_court_code:
            continue
        if row.get("judgment_code") != target_judgment_code:
            continue
        if row.get("status") != "1":
            continue

        publ_date = parse_date(row.get("date_publ"))
        if publ_date is None:
            continue
        if not (date_from <= publ_date <= date_to):
            continue

        matched_rows.append({key: row.get(key, "") for key in FIELDNAMES})

    write_tsv(output_path, matched_rows, FIELDNAMES)
    logging.info("Перевірено рядків: %s", total)
    logging.info("Відібрано постанов ВП: %s", len(matched_rows))
    logging.info("Файл збережено: %s", output_path)


if __name__ == "__main__":
    main()
