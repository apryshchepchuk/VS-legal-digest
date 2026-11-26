from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import shutil
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import requests

from common import ROOT_DIR, load_settings, parse_date, setup_logging

DOCUMENT_FIELDS = [
    "doc_id", "court_code", "judgment_code", "justice_kind", "category_code",
    "cause_num", "adjudication_date", "receipt_date", "judge", "doc_url",
    "status", "date_publ",
]
DICTIONARIES = {
    "courts.csv": ("court_code", ["court_code", "name", "instance_code", "region_code"]),
    "instances.csv": ("instance_code", ["instance_code", "name"]),
}


def archives_dir() -> Path:
    return ROOT_DIR / "data" / "raw" / "archives"


def run_dir(mode: str) -> Path:
    return archives_dir() / f".{mode}_run"


def state_path() -> Path:
    return ROOT_DIR / "data" / "state" / "last_successful_dataset_urls.json"


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as f:
        value = json.load(f)
    if not isinstance(value, dict):
        raise ValueError(f"Очікував JSON-об'єкт: {path}")
    return value


def configured_datasets(settings: dict) -> dict[int, dict]:
    raw = settings.get("datasets")
    if not isinstance(raw, dict) or not raw:
        raise ValueError("У settings.json потрібно задати datasets за роками")
    result = {}
    passport_urls = set()
    for key, value in raw.items():
        year = int(key)
        if not 2000 <= year <= 2100 or not isinstance(value, dict):
            raise ValueError(f"Некоректний запис datasets[{key}]")
        passport = str(value.get("passport_url", "")).strip()
        zip_name = str(value.get("zip_name", f"edrsr_data_{year}.zip")).strip()
        fallback = str(value.get("fallback_zip_url", "")).strip()
        if not passport or urlparse(passport).scheme != "https":
            raise ValueError(f"Некоректний passport_url для {year}")
        if passport in passport_urls:
            raise ValueError(f"Один паспорт задано для декількох років: {passport}")
        passport_urls.add(passport)
        if zip_name != f"edrsr_data_{year}.zip":
            raise ValueError(f"Неправильна назва ZIP для {year}: {zip_name}")
        if fallback and urlparse(fallback).scheme != "https":
            raise ValueError(f"Некоректний fallback_zip_url для {year}")
        result[year] = {"passport_url": passport, "zip_name": zip_name, "fallback_zip_url": fallback}
    return result


def local_today(settings: dict) -> date:
    return datetime.now(ZoneInfo(settings.get("timezone", "Europe/Kyiv"))).date()


def daily_years(settings: dict, today: date, window: int) -> list[int]:
    datasets = configured_datasets(settings)
    grace = int(settings.get("dataset_rollover_grace_days", 14))
    overlap = int(settings.get("dataset_previous_year_overlap_days", 60))
    elapsed = (today - date(today.year, 1, 1)).days
    if today.year not in datasets:
        if elapsed >= grace or today.year - 1 not in datasets:
            raise RuntimeError(f"Не налаштовано набір {today.year}. Додайте його паспорт до datasets.")
        logging.warning("ПЕРЕХІД РОКУ: набір %s ще не налаштовано. Тимчасово працюємо з %s; "
                        "актуальні рішення нового року не охоплені.", today.year, today.year - 1)
    cutoff = today - timedelta(days=max(1, window) - 1)
    first_year = cutoff.year
    if elapsed < overlap or today.year not in datasets:
        first_year = min(first_year, today.year - 1)
    requested = list(range(first_year, today.year + 1))
    missing = [y for y in requested if y not in datasets and y != today.year]
    if missing:
        logging.warning("Не налаштовано історичні набори %s; пошук обмежений наявними роками", missing)
    return [y for y in requested if y in datasets]


def case_years(settings: dict, today: date, date_from: date, date_to: date, period_field: str) -> list[int]:
    if date_from > date_to:
        raise ValueError("DATE_FROM пізніше DATE_TO")
    if date_from > today:
        raise ValueError("DATE_FROM у майбутньому")
    if period_field not in {"adjudication_date", "date_publ"}:
        raise ValueError("Непідтримуваний PERIOD_FIELD")
    datasets = configured_datasets(settings)
    # Рішення може надійти/бути опубліковане в іншому році.
    required = set(range(date_from.year, min(date_to, today).year + 1))
    missing = sorted(required - datasets.keys())
    if missing:
        raise RuntimeError(f"Ручний пошук неповний: не налаштовано набори за {missing}")
    if period_field == "date_publ":
        # Публікація може бути пізніше надходження; потрібні також старші архіви.
        years = sorted(y for y in datasets if y <= min(date_to, today).year)
    else:
        # Надходження може бути пізніше ухвалення; потрібні також новіші архіви.
        years = sorted(y for y in datasets if date_from.year <= y <= today.year)
        later_missing = sorted(set(range(date_from.year, today.year + 1)) - datasets.keys())
        if later_missing:
            logging.warning("Не налаштовано набори пізнішого надходження %s; можливі пропуски", later_missing)
    if not years:
        raise RuntimeError("Немає налаштованих наборів для ручного пошуку")
    logging.info("Ручний пошук охоплює налаштовані річні набори %s", years)
    return years


def http_status(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    return response.status_code if response is not None else None


def passport_zip_url(spec: dict, timeout: int, ua: str, attempts: int = 3) -> str:
    for i in range(attempts):
        try:
            with requests.get(spec["passport_url"], timeout=timeout,
                              headers={"User-Agent": ua, "Cache-Control": "no-cache"}) as r:
                r.raise_for_status()
                passport = r.json()
            files = passport.get("Файли")
            if not isinstance(files, list):
                raise ValueError("У паспорті відсутній список Файли")
            urls = {str(item[spec["zip_name"]]).strip() for item in files
                    if isinstance(item, dict) and spec["zip_name"] in item}
            if len(urls) != 1:
                raise ValueError(f"Не знайдено однозначний {spec['zip_name']} у паспорті")
            url = urls.pop()
            if urlparse(url).scheme not in {"http", "https"} or not urlparse(url).netloc:
                raise ValueError("Некоректний URL ZIP у паспорті")
            logging.info("Паспорт: %s -> %s", spec["zip_name"], url)
            return url
        except (requests.RequestException, ValueError, AttributeError) as exc:
            logging.warning("Паспорт, спроба %s/%s: %s", i + 1, attempts, exc)
            if i + 1 == attempts or http_status(exc) in {404, 410}:
                raise
            time.sleep(15)
    raise AssertionError("unreachable")


def download_zip(url: str, path: Path, timeout: int, ua: str, attempts: int = 3) -> None:
    tmp = path.with_suffix(".zip.part")
    for i in range(attempts):
        try:
            logging.info("ZIP, спроба %s/%s: %s", i + 1, attempts, url)
            with requests.get(url, stream=True, timeout=timeout, headers={"User-Agent": ua}) as r:
                r.raise_for_status()
                with tmp.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            if not zipfile.is_zipfile(tmp):
                raise zipfile.BadZipFile("Відповідь не є ZIP")
            tmp.replace(path)
            return
        except (requests.RequestException, zipfile.BadZipFile) as exc:
            logging.warning("Завантаження ZIP не вдалося: %s", exc)
            if i + 1 == attempts or http_status(exc) in {400, 401, 403, 404, 410}:
                raise
            time.sleep(20)
        finally:
            tmp.unlink(missing_ok=True)


def zip_member(zf: zipfile.ZipFile, name: str) -> str:
    matches = [i.filename for i in zf.infolist() if not i.is_dir()
               and i.filename.replace("\\", "/").rsplit("/", 1)[-1] == name]
    if len(matches) != 1:
        raise ValueError(f"У ZIP потрібен рівно один {name}; знайдено {len(matches)}")
    return matches[0]


def read_zip_dict(zf: zipfile.ZipFile, name: str, fields: list[str]) -> list[dict]:
    with zf.open(zip_member(zf, name)) as raw, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        if not set(fields).issubset(reader.fieldnames or []):
            raise ValueError(f"Невідомі колонки {name}: {reader.fieldnames}")
        rows = [{k: (row.get(k) or "").strip() for k in fields} for row in reader]
        if not rows:
            raise ValueError(f"Порожній довідник {name}")
        return rows


def write_tsv(path: Path, rows, fields: list[str]) -> None:
    tmp = path.with_suffix(path.suffix + ".part")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def fast_date(value: str) -> date | None:
    value = value.strip()
    if not value:
        return None
    # Формат ДСА: YYYY-MM-DD або YYYY-MM-DD HH:MM:SS+02.
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return parse_date(value)


def scan_zip(path: Path, target: Path, year: int, settings: dict, today: date,
             mode: str, case_number: str = "") -> dict:
    """Один потоковий прохід: дата свіжості ВСЬОГО набору та компактна вибірка."""
    latest = None
    total = selected = expected_receipts = other_receipts = future_dates = 0
    wanted_case = case_number.strip().replace(" ", "")
    court_codes = {str(settings.get("target_court_code", "9951"))}
    if settings.get("vs_fallback_enabled", False):
        court_codes.update(str(c) for c in settings.get("vs_fallback_court_codes", ["9911", "9921", "9931"]))
    judgment = str(settings.get("target_judgment_code", "2"))
    target.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path) as zf:
        dictionaries = {name: read_zip_dict(zf, name, fields) for name, (_, fields) in DICTIONARIES.items()}
        with zf.open(zip_member(zf, "documents.csv")) as raw, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as f, \
             (target / "documents.csv.part").open("w", encoding="utf-8", newline="") as out:
            reader = csv.DictReader(f, delimiter="\t")
            if not set(DOCUMENT_FIELDS).issubset(reader.fieldnames or []):
                raise ValueError(f"Невідомі колонки documents.csv: {reader.fieldnames}")
            writer = csv.DictWriter(out, fieldnames=DOCUMENT_FIELDS, delimiter="\t", extrasaction="ignore")
            writer.writeheader()
            for row in reader:
                total += 1
                receipt = (row.get("receipt_date") or "").strip()[:4]
                if receipt.isdigit():
                    if int(receipt) == year:
                        expected_receipts += 1
                    else:
                        other_receipts += 1
                pub = fast_date(row.get("date_publ") or "")
                if pub:
                    if pub > today:
                        future_dates += 1
                    elif latest is None or pub > latest:
                        latest = pub
                if mode == "case":
                    keep = (row.get("cause_num") or "").strip().replace(" ", "") == wanted_case
                else:
                    keep = ((row.get("court_code") or "").strip() in court_codes
                            and (row.get("judgment_code") or "").strip() == judgment
                            and (pub is None or pub <= today))
                # Не відсіюємо status=0: новіший запис може скасувати старий дублікат.
                if keep:
                    writer.writerow({k: (row.get(k) or "").strip() for k in DOCUMENT_FIELDS})
                    selected += 1
    if not total or not expected_receipts or expected_receipts <= other_receipts:
        raise ValueError(f"ZIP не відповідає року {year}: receipt_date {expected_receipts} очікуваних, {other_receipts} інших")
    if other_receipts or future_dates:
        logging.warning("Набір %s: receipt_date інших років=%s, date_publ у майбутньому=%s", year, other_receipts, future_dates)
    if latest is None:
        raise ValueError(f"Набір {year} не має валідної date_publ")
    # Для завершених років порівнюємо з кінцем відповідного року, не з сьогодні.
    reference = min(today, date(year, 12, 31))
    age = max(0, (reference - latest).days)
    max_age = int(settings.get("dataset_max_age_days", 5))
    logging.info("Dataset %s: latest date_publ=%s; контрольна дата=%s; відставання=%s дн.; рядків=%s; відібрано=%s",
                 year, latest, reference, age, total, selected)
    if age > max_age:
        raise ValueError(f"Застарілий dataset {year}: відставання {age} днів > {max_age}")
    (target / "documents.csv.part").replace(target / "documents.csv")
    for name, (_, fields) in DICTIONARIES.items():
        write_tsv(target / name, dictionaries[name], fields)
    return {"latest_date_publ": latest.isoformat(), "age_days": age, "total_rows": total, "selected_rows": selected}


def last_good_urls(settings: dict) -> dict:
    path = state_path()
    if path.exists():
        return read_json(path)
    # Одноразова міграція старого state; лише для відомого паспорта 2026.
    legacy_path = path.with_name("last_successful_dataset_url.json")
    old = read_json(legacy_path)
    spec = configured_datasets(settings).get(2026, {})
    passport_id = parse_qs(urlparse(spec.get("passport_url", "")).query).get("json", [])
    url = str(old.get("url", ""))
    if passport_id == ["7636"] and urlparse(url).hostname == "court.gov.ua" and "/7636/" in urlparse(url).path:
        return {"2026": {**old, "passport_url": spec["passport_url"], "zip_name": spec["zip_name"]}}
    return {}


def fetch_year(year: int, settings: dict, today: date, mode: str, case_number: str = "") -> dict:
    spec = configured_datasets(settings)[year]
    target = run_dir(mode) / str(year)
    target.mkdir(parents=True, exist_ok=True)
    zip_path = target / spec["zip_name"]
    timeout = int(settings.get("request_timeout_seconds", 120))
    ua = str(settings.get("user_agent", "vp-vs-digest/0.1"))
    saved = last_good_urls(settings)
    previous = saved.get(str(year), {})
    if previous.get("passport_url") != spec["passport_url"] or previous.get("zip_name") != spec["zip_name"]:
        previous = {}
    attempted = set()
    errors = []

    def attempt(url: str, source: str, allow_repeat: bool = False, attempts: int = 3) -> dict | None:
        if not url or (url in attempted and not allow_repeat):
            return None
        attempted.add(url)
        try:
            download_zip(url, zip_path, timeout, ua, attempts=attempts)
            metadata = scan_zip(zip_path, target, year, settings, today, mode, case_number)
        except (requests.RequestException, ValueError, zipfile.BadZipFile, UnicodeError, EOFError, RuntimeError) as exc:
            errors.append(f"{source}: {exc}")
            logging.warning("Набір %s, %s не прийнято: %s", year, source, exc)
            return None
        finally:
            # Після сканування повний ZIP більше не потрібен у цьому run.
            zip_path.unlink(missing_ok=True)
        entry = {"url": url, "source": source, "saved_at": datetime.now(timezone.utc).isoformat(),
                 "passport_url": spec["passport_url"], "zip_name": spec["zip_name"], **metadata}
        saved[str(year)] = entry
        atomic_json(state_path(), saved)
        logging.info("Набір %s прийнято через %s", year, source)
        return entry

    first_url = ""
    try:
        first_url = passport_zip_url(spec, timeout, ua)
    except (requests.RequestException, ValueError, AttributeError) as exc:
        errors.append(f"passport: {exc}")
    if first_url:
        result = attempt(first_url, "passport")
        if result:
            return result
        logging.info("Оновлення паспорта %s через 30 секунд", year)
        time.sleep(30)
        try:
            refreshed = passport_zip_url(spec, timeout, ua, attempts=2)
            # Навіть незмінний URL може вже запрацювати після синхронізації ДСА.
            result = attempt(refreshed, "refreshed_passport", allow_repeat=True,
                             attempts=1 if refreshed == first_url else 3)
            if result:
                return result
        except (requests.RequestException, ValueError, AttributeError) as exc:
            errors.append(f"refreshed_passport: {exc}")
    for url, source in [(str(previous.get("url", "")), "last_successful_url"),
                        (spec["fallback_zip_url"], "settings_fallback")]:
        result = attempt(url, source)
        if result:
            return result
    raise RuntimeError(f"Не вдалося отримати валідний і достатньо свіжий dataset {year}: " + "; ".join(errors))


def merge_view(mode: str, years: list[int]) -> None:
    """Об'єднуємо компактні таблиці. Новіший річний набір має пріоритет за doc_id."""
    base = run_dir(mode)
    out = archives_dir() if mode == "daily" else archives_dir() / "case_search"
    out.mkdir(parents=True, exist_ok=True)
    docs = {}
    maps: dict[str, dict] = {name: {} for name in DICTIONARIES}
    duplicates = 0
    for year in sorted(years):
        with (base / str(year) / "documents.csv").open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                doc_id = row.get("doc_id", "").strip()
                if not doc_id:
                    continue
                if doc_id in docs:
                    duplicates += 1
                docs[doc_id] = row
        for name, (key, _) in DICTIONARIES.items():
            with (base / str(year) / name).open(encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f, delimiter="\t"):
                    maps[name][row[key]] = row
    write_tsv(out / "documents.csv", docs.values(), DOCUMENT_FIELDS)
    for name, (_, fields) in DICTIONARIES.items():
        write_tsv(out / name, maps[name].values(), fields)
    logging.info("Об'єднана вибірка %s: роки=%s, doc_id=%s, дублікатів=%s", mode, years, len(docs), duplicates)


def ensure_years(settings: dict, mode: str, years: list[int], today: date, case_number: str = "") -> dict:
    if not years:
        raise RuntimeError("Не визначено жодного річного набору")
    manifest_path = run_dir(mode) / "manifest.json"
    manifest = read_json(manifest_path)
    if manifest.get("today") != today.isoformat() or manifest.get("case_number", "") != case_number:
        raise RuntimeError("Не ініціалізовано свіжий dataset run; спершу запустіть fetch_dataset.py")
    loaded = manifest.get("datasets", {})
    changed = False
    for year in years:
        if str(year) not in loaded:
            loaded[str(year)] = fetch_year(year, settings, today, mode, case_number)
            changed = True
    merged_years = sorted(int(y) for y in loaded)
    if changed or not manifest.get("merged") or not ((archives_dir() if mode == "daily" else archives_dir() / "case_search") / "documents.csv").exists():
        merge_view(mode, merged_years)
        manifest.update({"datasets": loaded, "merged": True, "years": merged_years})
        atomic_json(manifest_path, manifest)
    return manifest


def ensure_daily_window(settings: dict, window: int) -> dict:
    manifest = read_json(run_dir("daily") / "manifest.json")
    if not manifest.get("today"):
        raise RuntimeError("Спершу запустіть fetch_dataset.py")
    today = date.fromisoformat(manifest["today"])
    return ensure_years(settings, "daily", daily_years(settings, today, window), today)


def start_run(settings: dict, mode: str, today: date, case_number: str = "") -> None:
    base = run_dir(mode)
    if base.exists():
        shutil.rmtree(base)
    base.mkdir(parents=True)
    atomic_json(base / "manifest.json", {"today": today.isoformat(), "case_number": case_number, "datasets": {}})


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--case-search", action="store_true")
    args = parser.parse_args()
    settings = load_settings()
    today = local_today(settings)
    if args.case_search:
        case = os.environ.get("CASE_NUMBER", "").strip()
        start = parse_date(os.environ.get("DATE_FROM"))
        end = parse_date(os.environ.get("DATE_TO"))
        if not case or not start or not end:
            raise ValueError("Ручний пошук потребує CASE_NUMBER, DATE_FROM, DATE_TO")
        years = case_years(settings, today, start, end, os.environ.get("PERIOD_FIELD", "adjudication_date"))
        start_run(settings, "case", today, case)
        ensure_years(settings, "case", years, today, case)
    else:
        windows = settings.get("selection_lookback_days_sequence") or [settings.get("selection_lookback_days", 60)]
        first = next((int(w) for w in windows if str(w).isdigit() and int(w) > 0), 60)
        start_run(settings, "daily", today)
        ensure_daily_window(settings, first)


if __name__ == "__main__":
    main()
