from __future__ import annotations

import csv
import logging
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from common import ROOT_DIR, load_settings, parse_date, setup_logging


def normalize_cause_num(value: str) -> str:
    """
    Нормалізує номер справи для перевірки префіксів.

    Потрібно для випадків типу:
    - 990SCGC/
    - 990SСGС/
    де частина літер може бути латиницею, а частина кирилицею.
    """
    raw = str(value or "").strip().upper()

    replacements = {
        "А": "A",
        "В": "B",
        "С": "C",
        "Е": "E",
        "Н": "H",
        "І": "I",
        "К": "K",
        "М": "M",
        "О": "O",
        "Р": "P",
        "Т": "T",
        "У": "Y",
        "Х": "X",
        "Ґ": "G",
    }

    for src, dst in replacements.items():
        raw = raw.replace(src, dst)

    raw = raw.replace(" ", "")
    return raw


def normalize_prefixes(prefixes: object) -> list[str]:
    if not isinstance(prefixes, list):
        return ["990/", "9901/", "800/", "990SCGC/"]

    normalized: list[str] = []
    for prefix in prefixes:
        value = normalize_cause_num(str(prefix))
        if value:
            normalized.append(value)

    return normalized or ["990/", "9901/", "800/", "990SCGC/"]


def is_likely_vp_appellate_admin_case(row: dict, normalized_prefixes: list[str]) -> bool:
    """
    Визначає очевидні адміністративні справи, де Велика Палата ВС
    зазвичай діє як апеляційна інстанція щодо рішень КАС ВС
    як суду першої інстанції.

    Це не пряме поле з джерела, а обережний metadata-фільтр:
    justice_kind = 4 + характерний префікс номера справи.
    """
    justice_kind = str(row.get("justice_kind", "")).strip()
    cause_num = normalize_cause_num(str(row.get("cause_num", "")))

    if justice_kind != "4":
        return False

    return cause_num.startswith(tuple(normalized_prefixes))


def main() -> None:
    setup_logging()
    settings = load_settings()

    tz_name = settings.get("timezone", "Europe/Kyiv")
    lookback_days = int(settings.get("selection_lookback_days", 60))
    target_court_code = str(settings.get("target_court_code", "9951"))
    target_judgment_code = str(settings.get("target_judgment_code", "2"))

    exclude_vp_appellate_admin_cases = bool(
        settings.get("exclude_vp_appellate_admin_cases", True)
    )
    vp_appellate_admin_prefixes = normalize_prefixes(
        settings.get(
            "vp_appellate_admin_prefixes",
            ["990/", "9901/", "800/", "990SCGC/"],
        )
    )

    input_path = ROOT_DIR / "data" / "raw" / "archives" / "documents.csv"
    output_path = ROOT_DIR / "data" / "interim" / "vp_candidates.csv"
    excluded_path = ROOT_DIR / "data" / "interim" / "vp_excluded_appellate_admin.csv"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Не знайдено {input_path}")

    today = datetime.now(ZoneInfo(tz_name)).date()
    cutoff = today - timedelta(days=lookback_days - 1)

    total_rows = 0
    selected_rows: list[dict] = []
    excluded_rows: list[dict] = []

    with input_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")

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

            out_row = {
                "doc_id": str(row.get("doc_id", "")).strip(),
                "cause_num": str(row.get("cause_num", "")).strip(),
                "adjudication_date": str(row.get("adjudication_date", "")).strip(),
                "receipt_date": str(row.get("receipt_date", "")).strip(),
                "date_publ": date_publ_raw,
                "judge": str(row.get("judge", "")).strip(),
                "doc_url": str(row.get("doc_url", "")).strip(),
                "court_code": court_code,
                "judgment_code": judgment_code,
                "justice_kind": str(row.get("justice_kind", "")).strip(),
                "category_code": str(row.get("category_code", "")).strip(),
                "status": status,
            }

            if (
                exclude_vp_appellate_admin_cases
                and is_likely_vp_appellate_admin_case(row, vp_appellate_admin_prefixes)
            ):
                excluded_rows.append(
                    {
                        **out_row,
                        "exclude_reason": "likely_vp_appellate_admin_case",
                    }
                )
                continue

            selected_rows.append(out_row)

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
        "justice_kind",
        "category_code",
        "status",
    ]

    excluded_fields = out_fields + ["exclude_reason"]

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, delimiter="\t")
        writer.writeheader()
        for row in selected_rows:
            writer.writerow(row)

    with excluded_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=excluded_fields, delimiter="\t")
        writer.writeheader()
        for row in excluded_rows:
            writer.writerow(row)

    logging.info("Перевірено рядків у documents.csv: %s", total_rows)
    logging.info("Період відбору постанов ВП: %s днів", lookback_days)
    logging.info("Виключення апеляційних адмінсправ ВП: %s", exclude_vp_appellate_admin_cases)
    logging.info("Префікси для виключення: %s", ", ".join(vp_appellate_admin_prefixes))
    logging.info("Відібрано постанов ВП до candidate pool: %s", len(selected_rows))
    logging.info("Виключено очевидних апеляційних адмінсправ ВП: %s", len(excluded_rows))
    logging.info("Сформовано %s", output_path)
    logging.info("Сформовано %s", excluded_path)


if __name__ == "__main__":
    main()
