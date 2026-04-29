from __future__ import annotations

import csv
import logging
import os
from datetime import date
from pathlib import Path
from typing import Iterable

from common import ROOT_DIR, parse_date, setup_logging


def read_tsv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Не знайдено {path}")

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


def normalize_case_number(value: str) -> str:
    return str(value or "").strip().replace(" ", "")


def normalize_text(value: str) -> str:
    return str(value or "").strip().lower()


def build_courts_map(courts_rows: Iterable[dict]) -> dict[str, dict]:
    result: dict[str, dict] = {}

    for row in courts_rows:
        court_code = str(row.get("court_code", "")).strip()
        if not court_code:
            continue

        result[court_code] = {
            "court_name": str(row.get("name", "")).strip(),
            "instance_code": str(row.get("instance_code", "")).strip(),
            "region_code": str(row.get("region_code", "")).strip(),
        }

    return result


def build_instances_map(instances_rows: Iterable[dict]) -> dict[str, str]:
    result: dict[str, str] = {}

    for row in instances_rows:
        instance_code = str(row.get("instance_code", "")).strip()
        if not instance_code:
            continue

        result[instance_code] = str(row.get("name", "")).strip()

    return result


def instance_matches(
    instance_code: str,
    instance_name: str,
    instance_filter: str | None,
) -> bool:
    if not instance_filter:
        return True

    wanted = normalize_text(instance_filter)

    if normalize_text(instance_code) == wanted:
        return True

    if wanted in normalize_text(instance_name):
        return True

    return False


def date_in_range(
    value: str,
    date_from: str | None,
    date_to: str | None,
) -> bool:
    date_obj = parse_date(str(value or "").strip())
    if not date_obj:
        return False

    if date_from:
        start = parse_date(date_from)
        if not start:
            raise ValueError(f"Невірний DATE_FROM: {date_from}")
        if date_obj < start:
            return False

    if date_to:
        end = parse_date(date_to)
        if not end:
            raise ValueError(f"Невірний DATE_TO: {date_to}")
        if date_obj > end:
            return False

    return True


def find_case_decision_links(
    case_number: str,
    date_from: str | None = None,
    date_to: str | None = None,
    instance_filter: str | None = None,
    period_field: str = "adjudication_date",
    only_active: bool = True,
) -> list[dict]:
    """
    Шукає всі рішення по заданому номеру справи + періоду + інстанції.

    Параметри:
    - case_number: номер справи, напр. "910/2517/24"
    - date_from/date_to: межі періоду у форматі YYYY-MM-DD
    - instance_filter:
        * код інстанції, якщо ви його знаєте
        * або частина назви, напр. "касац", "апеляц", "перша"
    - period_field:
        * "adjudication_date" — фільтрувати за датою ухвалення
        * "date_publ" — фільтрувати за датою публікації
    - only_active: якщо True, брати тільки status=1
    """
    if period_field not in {"adjudication_date", "date_publ"}:
        raise ValueError("PERIOD_FIELD має бути 'adjudication_date' або 'date_publ'")

    documents_path = ROOT_DIR / "data" / "raw" / "archives" / "documents.csv"
    courts_path = ROOT_DIR / "data" / "raw" / "archives" / "courts.csv"
    instances_path = ROOT_DIR / "data" / "raw" / "archives" / "instances.csv"

    documents_rows = read_tsv(documents_path)
    courts_rows = read_tsv(courts_path)
    instances_rows = read_tsv(instances_path)

    courts_map = build_courts_map(courts_rows)
    instances_map = build_instances_map(instances_rows)

    wanted_case = normalize_case_number(case_number)
    results: list[dict] = []

    for row in documents_rows:
        cause_num = normalize_case_number(str(row.get("cause_num", "")))
        if cause_num != wanted_case:
            continue

        status = str(row.get("status", "")).strip()
        if only_active and status != "1":
            continue

        period_value = str(row.get(period_field, "")).strip()
        if not date_in_range(period_value, date_from, date_to):
            continue

        court_code = str(row.get("court_code", "")).strip()
        court_info = courts_map.get(court_code, {})
        instance_code = str(court_info.get("instance_code", "")).strip()
        instance_name = instances_map.get(instance_code, "")

        if not instance_matches(instance_code, instance_name, instance_filter):
            continue

        results.append(
            {
                "doc_id": str(row.get("doc_id", "")).strip(),
                "cause_num": str(row.get("cause_num", "")).strip(),
                "adjudication_date": str(row.get("adjudication_date", "")).strip(),
                "receipt_date": str(row.get("receipt_date", "")).strip(),
                "date_publ": str(row.get("date_publ", "")).strip(),
                "court_code": court_code,
                "court_name": str(court_info.get("court_name", "")).strip(),
                "instance_code": instance_code,
                "instance_name": instance_name,
                "judgment_code": str(row.get("judgment_code", "")).strip(),
                "justice_kind": str(row.get("justice_kind", "")).strip(),
                "category_code": str(row.get("category_code", "")).strip(),
                "judge": str(row.get("judge", "")).strip(),
                "status": status,
                "doc_url": str(row.get("doc_url", "")).strip(),
            }
        )

    def sort_key(item: dict) -> tuple[date, date, int]:
        adjudication_date = parse_date(str(item.get("adjudication_date", "")).strip())
        date_publ = parse_date(str(item.get("date_publ", "")).strip())
        try:
            doc_id = int(str(item.get("doc_id", "0")).strip() or "0")
        except ValueError:
            doc_id = 0

        return (
            adjudication_date or date.min,
            date_publ or date.min,
            doc_id,
        )

    results.sort(key=sort_key, reverse=True)
    return results


def save_case_decision_links(
    case_number: str,
    date_from: str | None = None,
    date_to: str | None = None,
    instance_filter: str | None = None,
    period_field: str = "adjudication_date",
    only_active: bool = True,
    output_path: Path | None = None,
) -> Path:
    results = find_case_decision_links(
        case_number=case_number,
        date_from=date_from,
        date_to=date_to,
        instance_filter=instance_filter,
        period_field=period_field,
        only_active=only_active,
    )

    if output_path is None:
        safe_case = normalize_case_number(case_number).replace("/", "_")
        output_path = ROOT_DIR / "outputs" / "case_search" / f"{safe_case}_decisions.tsv"

    fieldnames = [
        "doc_id",
        "cause_num",
        "adjudication_date",
        "receipt_date",
        "date_publ",
        "court_code",
        "court_name",
        "instance_code",
        "instance_name",
        "judgment_code",
        "justice_kind",
        "category_code",
        "judge",
        "status",
        "doc_url",
    ]

    write_tsv(output_path, results, fieldnames)
    return output_path


def main() -> None:
    setup_logging()

    case_number = os.getenv("CASE_NUMBER", "").strip()
    date_from = os.getenv("DATE_FROM", "").strip() or None
    date_to = os.getenv("DATE_TO", "").strip() or None
    instance_filter = os.getenv("INSTANCE_FILTER", "").strip() or None
    period_field = os.getenv("PERIOD_FIELD", "adjudication_date").strip()
    only_active_raw = os.getenv("ONLY_ACTIVE", "true").strip().lower()
    only_active = only_active_raw in {"1", "true", "yes", "y"}

    if not case_number:
        raise ValueError("Не задано CASE_NUMBER")

    output_path = save_case_decision_links(
        case_number=case_number,
        date_from=date_from,
        date_to=date_to,
        instance_filter=instance_filter,
        period_field=period_field,
        only_active=only_active,
    )

    results = read_tsv(output_path)

    logging.info("Номер справи: %s", case_number)
    logging.info("Період: %s .. %s", date_from or "—", date_to or "—")
    logging.info("Фільтр інстанції: %s", instance_filter or "—")
    logging.info("Поле дати: %s", period_field)
    logging.info("Лише активні: %s", only_active)
    logging.info("Знайдено рішень: %s", len(results))
    logging.info("Сформовано файл результатів: %s", output_path)


if __name__ == "__main__":
    main()
