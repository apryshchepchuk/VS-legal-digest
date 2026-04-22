from __future__ import annotations

import csv
import logging
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from common import ROOT_DIR, load_settings, parse_date, setup_logging


def main() -> None:
    setup_logging()
    settings = load_settings()

    tz_name = settings.get("timezone", "Europe/Kyiv")
    lookback_days = int(settings.get("selection_lookback_days", 30))
    target_court_code = str(settings.get("target_court_code", "9951"))
    target_judgment_code = str(settings.get("target_judgment_code", "2"))

    input_path = ROOT_DIR / "data" / "raw" / "archives" / "documents.csv"
    output_path = ROOT_DIR / "data" / "interim" / "vp_candidates.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Не знайдено {input_path}")

    today = datetime.now(ZoneInfo(tz_name)).date()
    cutoff = today - timedelta(days=lookback_days - 1)

    total_rows = 0
    selected_rows: list[dict] = []

    with input_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = reader.fieldnames or []

        for row in reader:
            total_rows += 1

            court_code = str(row.get("court_code", "")).strip()
            judgment_code = str(row.get("judgment_code", "")).strip()
            status = str(row.get("status", "")).strip()
            date_publ_raw = str(row.get("date_publ", "")).strip()
            date_publ = parse_date(date_publ_raw)

            if court_code != target_court_code:
                continue
            if judgment_code != target_judgment_code:
                continue
            if status != "1":
                continue
            if not date_publ:
                continue
            if date_publ < cutoff:
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
                    "judgment_code": judgment_code,
                }
            )

    out_fields = [
        "doc_id",
        "cause_num",
        "adjudication_date",
        "receipt_date",
        "date_publ",
        "judge",
        "doc_url",
        "court_code",
        "judgment_code",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, delimiter="\t")
        writer.writeheader()
        for row in selected_rows:
            writer.writerow(row)

    logging.info("Перевірено рядків у documents.csv: %s", total_rows)
    logging.info("Відібрано постанов ВП за %s днів: %s", lookback_days, len(selected_rows))
    logging.info("Сформовано %s", output_path)


if __name__ == "__main__":
    main()
