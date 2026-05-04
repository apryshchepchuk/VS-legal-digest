from __future__ import annotations

import json
import logging
import shutil
import time
import zipfile
from pathlib import Path
from typing import Any

import requests

from common import ROOT_DIR, load_settings, setup_logging


def download_file(url: str, destination: Path, timeout: int, user_agent: str) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)

    logging.info("Завантаження: %s", url)

    with requests.get(
        url,
        stream=True,
        timeout=timeout,
        headers={"User-Agent": user_agent},
    ) as response:
        response.raise_for_status()

        with destination.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    logging.info("Збережено: %s", destination)


def load_passport(passport_url: str, timeout: int, user_agent: str) -> dict[str, Any]:
    logging.info("Завантаження паспорта набору даних: %s", passport_url)

    response = requests.get(
        passport_url,
        timeout=timeout,
        headers={"User-Agent": user_agent},
    )
    response.raise_for_status()

    try:
        passport = response.json()
    except json.JSONDecodeError as exc:
        snippet = response.text[:1000]
        raise RuntimeError(f"Паспорт набору даних не є валідним JSON: {snippet}") from exc

    return passport


def load_passport_with_retry(
    passport_url: str,
    timeout: int,
    user_agent: str,
    attempts: int = 3,
    sleep_seconds: float = 15.0,
) -> dict[str, Any]:
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            logging.info("Спроба завантаження паспорта %s/%s", attempt, attempts)
            return load_passport(
                passport_url=passport_url,
                timeout=timeout,
                user_agent=user_agent,
            )
        except Exception as exc:
            last_error = exc
            logging.warning(
                "Не вдалося завантажити паспорт набору даних на спробі %s/%s: %s",
                attempt,
                attempts,
                exc,
            )
            if attempt < attempts:
                logging.info("Пауза %.1f сек перед повтором", sleep_seconds)
                time.sleep(sleep_seconds)

    assert last_error is not None
    raise last_error


def find_zip_url_from_passport(passport: dict[str, Any], expected_zip_name: str) -> str:
    files = passport.get("Файли")

    if not isinstance(files, list):
        raise RuntimeError("У паспорті немає поля 'Файли' або воно не є списком")

    # Очікуваний формат:
    # "Файли": [
    #   {"readme_2026.pdf": "..."},
    #   {"edrsr_data_2026.zip": "..."}
    # ]
    for item in files:
        if not isinstance(item, dict):
            continue

        for filename, url in item.items():
            filename_str = str(filename).strip()
            url_str = str(url).strip()

            if filename_str == expected_zip_name and url_str:
                logging.info("Знайдено актуальний ZIP у паспорті: %s -> %s", filename_str, url_str)
                return url_str

    # fallback: якщо точна назва раптом змінилась, але в паспорті є один .zip
    zip_candidates: list[tuple[str, str]] = []

    for item in files:
        if not isinstance(item, dict):
            continue

        for filename, url in item.items():
            filename_str = str(filename).strip()
            url_str = str(url).strip()

            if filename_str.lower().endswith(".zip") and url_str:
                zip_candidates.append((filename_str, url_str))

    if len(zip_candidates) == 1:
        filename, url = zip_candidates[0]
        logging.warning(
            "Точний файл %s не знайдено, але знайдено єдиний ZIP у паспорті: %s",
            expected_zip_name,
            filename,
        )
        return url

    if zip_candidates:
        candidates = ", ".join(name for name, _ in zip_candidates)
        raise RuntimeError(
            f"У паспорті кілька ZIP-кандидатів, але немає точного {expected_zip_name}: {candidates}"
        )

    raise RuntimeError(f"У паспорті не знайдено ZIP-файл {expected_zip_name}")


def resolve_dataset_zip_url(settings: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    timeout = int(settings.get("request_timeout_seconds", 120))
    user_agent = settings.get("user_agent", "vp-vs-digest/0.1")

    passport_url = str(settings.get("dataset_passport_url", "")).strip()
    expected_zip_name = str(settings.get("dataset_zip_name", "edrsr_data_2026.zip")).strip()
    fallback_zip_url = str(settings.get("dataset_zip_url", "")).strip()

    if passport_url:
        try:
            passport = load_passport_with_retry(
                passport_url=passport_url,
                timeout=timeout,
                user_agent=user_agent,
                attempts=3,
                sleep_seconds=15.0,
            )
            zip_url = find_zip_url_from_passport(
                passport=passport,
                expected_zip_name=expected_zip_name,
            )
            return zip_url, passport
        except Exception as exc:
            logging.warning(
                "Не вдалося отримати актуальний ZIP із паспорта набору даних: %s",
                exc,
            )
            if fallback_zip_url:
                logging.warning(
                    "Використовую fallback dataset_zip_url із settings.json: %s",
                    fallback_zip_url,
                )
                return fallback_zip_url, None
            raise RuntimeError(
                "Паспорт недоступний, а fallback dataset_zip_url не задано"
            ) from exc

    if fallback_zip_url:
        logging.warning(
            "dataset_passport_url не задано. Використовую fallback dataset_zip_url: %s",
            fallback_zip_url,
        )
        return fallback_zip_url, None

    raise RuntimeError("У settings.json не задано ні dataset_passport_url, ні dataset_zip_url")


def clear_old_csv_files(extract_dir: Path) -> None:
    if not extract_dir.exists():
        return

    for path in extract_dir.glob("*.csv"):
        try:
            path.unlink()
        except Exception as exc:
            logging.warning("Не вдалося видалити старий CSV %s: %s", path, exc)


def extract_zip(zip_path: Path, extract_dir: Path) -> None:
    extract_dir.mkdir(parents=True, exist_ok=True)

    clear_old_csv_files(extract_dir)

    logging.info("Розпакування ZIP: %s -> %s", zip_path, extract_dir)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    logging.info("ZIP розпаковано")


def ensure_documents_csv(extract_dir: Path) -> None:
    documents_path = extract_dir / "documents.csv"
    if documents_path.exists():
        logging.info("Знайдено documents.csv: %s", documents_path)
        return

    candidates = list(extract_dir.rglob("documents.csv"))
    if not candidates:
        raise FileNotFoundError("Після розпакування не знайдено documents.csv")

    source = candidates[0]
    shutil.copy2(source, documents_path)
    logging.info("documents.csv скопійовано з %s до %s", source, documents_path)


def main() -> None:
    setup_logging()
    settings = load_settings()

    timeout = int(settings.get("request_timeout_seconds", 120))
    user_agent = settings.get("user_agent", "vp-vs-digest/0.1")

    archives_dir = ROOT_DIR / "data" / "raw" / "archives"
    archives_dir.mkdir(parents=True, exist_ok=True)

    zip_path = archives_dir / "edrsr_data_2026.zip"
    passport_path = archives_dir / "dataset_passport.json"
    resolved_url_path = archives_dir / "resolved_dataset_zip_url.txt"

    dataset_zip_url, passport = resolve_dataset_zip_url(settings)

    if passport is not None:
        passport_path.write_text(
            json.dumps(passport, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logging.info("Паспорт набору даних збережено: %s", passport_path)
    else:
        logging.info("Паспорт набору даних не збережено, бо використано fallback ZIP URL")

    resolved_url_path.write_text(dataset_zip_url, encoding="utf-8")
    logging.info("Актуальний URL ZIP збережено: %s", resolved_url_path)

    download_file(
        url=dataset_zip_url,
        destination=zip_path,
        timeout=timeout,
        user_agent=user_agent,
    )

    extract_zip(
        zip_path=zip_path,
        extract_dir=archives_dir,
    )

    ensure_documents_csv(archives_dir)


if __name__ == "__main__":
    main()
