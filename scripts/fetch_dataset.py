from __future__ import annotations

import json
import logging
import shutil
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from common import ROOT_DIR, load_settings, setup_logging


# ---------------------------------------------------------------------------
# HTTP / DOWNLOAD
# ---------------------------------------------------------------------------

def get_http_status(exc: Exception) -> int | None:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code
    return None


def download_file_once(
    url: str,
    destination: Path,
    timeout: int,
    user_agent: str,
) -> None:
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

    logging.info("Збережено тимчасовий файл: %s", destination)


def download_zip(
    url: str,
    destination: Path,
    timeout: int,
    user_agent: str,
    attempts: int = 3,
    sleep_seconds: float = 20.0,
) -> None:
    """
    Завантажує ZIP у тимчасовий .part файл.

    404 / 410:
      URL уже неактуальний -> повторювати той самий URL немає сенсу.

    Інші помилки:
      timeout, 5xx, connection errors тощо -> retry.

    Старий destination не видаляється, поки новий ZIP
    не буде повністю завантажений та перевірений.
    """

    temp_path = destination.with_name(destination.name + ".part")
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            if temp_path.exists():
                temp_path.unlink()

            logging.info("Спроба завантаження ZIP %s/%s", attempt, attempts)

            download_file_once(
                url=url,
                destination=temp_path,
                timeout=timeout,
                user_agent=user_agent,
            )

            if not zipfile.is_zipfile(temp_path):
                raise RuntimeError(
                    f"Завантажений файл не є валідним ZIP: {url}"
                )

            # Замінюємо основний ZIP лише після успішної перевірки.
            temp_path.replace(destination)

            logging.info("ZIP успішно перевірено та збережено: %s", destination)
            return

        except Exception as exc:
            last_error = exc
            status = get_http_status(exc)

            logging.warning(
                "Не вдалося завантажити ZIP на спробі %s/%s: %s",
                attempt,
                attempts,
                exc,
            )

            try:
                if temp_path.exists():
                    temp_path.unlink()
            except Exception as cleanup_exc:
                logging.warning(
                    "Не вдалося видалити неповний файл %s: %s",
                    temp_path,
                    cleanup_exc,
                )

            # Для 404/410 повтор того самого hash-URL практично не має сенсу.
            if status in {404, 410}:
                logging.warning(
                    "ZIP URL повернув HTTP %s. "
                    "Не повторюю цей URL; потрібен інший URL.",
                    status,
                )
                break

            if attempt < attempts:
                logging.info(
                    "Пауза %.1f сек перед повтором завантаження ZIP",
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

    if last_error is None:
        raise RuntimeError(f"Не вдалося завантажити ZIP: {url}")

    raise last_error


# ---------------------------------------------------------------------------
# PASSPORT
# ---------------------------------------------------------------------------

def load_passport(
    passport_url: str,
    timeout: int,
    user_agent: str,
) -> dict[str, Any]:
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
        raise RuntimeError(
            f"Паспорт набору даних не є валідним JSON: {snippet}"
        ) from exc

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
            logging.info(
                "Спроба завантаження паспорта %s/%s",
                attempt,
                attempts,
            )

            return load_passport(
                passport_url=passport_url,
                timeout=timeout,
                user_agent=user_agent,
            )

        except Exception as exc:
            last_error = exc

            logging.warning(
                "Не вдалося завантажити паспорт набору даних "
                "на спробі %s/%s: %s",
                attempt,
                attempts,
                exc,
            )

            if attempt < attempts:
                logging.info(
                    "Пауза %.1f сек перед повтором паспорта",
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

    assert last_error is not None
    raise last_error


def find_zip_url_from_passport(
    passport: dict[str, Any],
    expected_zip_name: str,
) -> str:
    files = passport.get("Файли")

    if not isinstance(files, list):
        raise RuntimeError(
            "У паспорті немає поля 'Файли' або воно не є списком"
        )

    # Спочатку шукаємо точну назву файлу.
    for item in files:
        if not isinstance(item, dict):
            continue

        for filename, url in item.items():
            filename_str = str(filename).strip()
            url_str = str(url).strip()

            if filename_str == expected_zip_name and url_str:
                logging.info(
                    "Знайдено актуальний ZIP у паспорті: %s -> %s",
                    filename_str,
                    url_str,
                )
                return url_str

    # Fallback: якщо точна назва змінилась,
    # але у паспорті є лише один ZIP.
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
            "Точний файл %s не знайдено, "
            "але знайдено єдиний ZIP у паспорті: %s",
            expected_zip_name,
            filename,
        )

        return url

    if zip_candidates:
        candidates = ", ".join(name for name, _ in zip_candidates)

        raise RuntimeError(
            f"У паспорті кілька ZIP-кандидатів, "
            f"але немає точного {expected_zip_name}: {candidates}"
        )

    raise RuntimeError(
        f"У паспорті не знайдено ZIP-файл {expected_zip_name}"
    )


def save_passport(
    passport: dict[str, Any],
    passport_path: Path,
) -> None:
    passport_path.parent.mkdir(parents=True, exist_ok=True)

    passport_path.write_text(
        json.dumps(passport, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logging.info(
        "Паспорт набору даних збережено: %s",
        passport_path,
    )


# ---------------------------------------------------------------------------
# LAST SUCCESSFUL DATASET URL
# ---------------------------------------------------------------------------

def load_last_successful_dataset_url(state_path: Path) -> str:
    if not state_path.exists():
        return ""

    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))

        if not isinstance(data, dict):
            return ""

        url = str(data.get("url", "")).strip()

        if url:
            logging.info(
                "Знайдено останній успішний URL набору даних: %s",
                url,
            )

        return url

    except Exception as exc:
        logging.warning(
            "Не вдалося прочитати %s: %s",
            state_path,
            exc,
        )
        return ""


def save_last_successful_dataset_url(
    state_path: Path,
    url: str,
    source: str,
) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "url": url,
        "source": source,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }

    state_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logging.info(
        "Останній успішний URL набору даних збережено: %s",
        state_path,
    )


# ---------------------------------------------------------------------------
# DATASET RESOLUTION
# ---------------------------------------------------------------------------

def try_dataset_url(
    *,
    url: str,
    source: str,
    zip_path: Path,
    timeout: int,
    user_agent: str,
    attempted_urls: set[str],
) -> bool:
    url = str(url or "").strip()

    if not url:
        return False

    if url in attempted_urls:
        logging.info(
            "URL уже перевірявся в цьому run, пропускаю: %s",
            url,
        )
        return False

    attempted_urls.add(url)

    logging.info(
        "Перевіряю dataset ZIP. Джерело URL: %s",
        source,
    )

    try:
        download_zip(
            url=url,
            destination=zip_path,
            timeout=timeout,
            user_agent=user_agent,
        )

        logging.info(
            "Dataset ZIP успішно отримано. Джерело: %s",
            source,
        )

        return True

    except Exception as exc:
        logging.warning(
            "Dataset ZIP із джерела '%s' не спрацював: %s",
            source,
            exc,
        )

        return False


def resolve_and_download_dataset(
    *,
    settings: dict[str, Any],
    zip_path: Path,
    passport_path: Path,
    last_successful_url_path: Path,
) -> tuple[str, str]:
    """
    Порядок fallback:

    1. Поточний URL із паспорта.
    2. Повторне читання паспорта після невдалого ZIP.
    3. Останній реально успішний URL із state.
    4. dataset_zip_url із settings.json.
    5. Помилка.
    """

    timeout = int(settings.get("request_timeout_seconds", 120))
    user_agent = str(
        settings.get("user_agent", "vp-vs-digest/0.1")
    ).strip()

    passport_url = str(
        settings.get("dataset_passport_url", "")
    ).strip()

    expected_zip_name = str(
        settings.get(
            "dataset_zip_name",
            "edrsr_data_2026.zip",
        )
    ).strip()

    fallback_zip_url = str(
        settings.get("dataset_zip_url", "")
    ).strip()

    last_successful_url = load_last_successful_dataset_url(
        last_successful_url_path
    )

    attempted_urls: set[str] = set()

    # ------------------------------------------------------------------
    # 1. Основний варіант — URL із поточного паспорта
    # ------------------------------------------------------------------

    if passport_url:
        try:
            passport = load_passport_with_retry(
                passport_url=passport_url,
                timeout=timeout,
                user_agent=user_agent,
                attempts=3,
                sleep_seconds=15.0,
            )

            save_passport(
                passport=passport,
                passport_path=passport_path,
            )

            passport_zip_url = find_zip_url_from_passport(
                passport=passport,
                expected_zip_name=expected_zip_name,
            )

            if try_dataset_url(
                url=passport_zip_url,
                source="passport",
                zip_path=zip_path,
                timeout=timeout,
                user_agent=user_agent,
                attempted_urls=attempted_urls,
            ):
                return passport_zip_url, "passport"

            # ----------------------------------------------------------
            # 2. URL із паспорта не спрацював.
            # Даємо джерелу трохи часу та читаємо паспорт повторно.
            # Це корисно, коли паспорт і файлове сховище оновлюються
            # неодночасно.
            # ----------------------------------------------------------

            refresh_sleep_seconds = 30.0

            logging.warning(
                "URL із паспорта не спрацював. "
                "Чекаю %.1f сек і повторно читаю паспорт.",
                refresh_sleep_seconds,
            )

            time.sleep(refresh_sleep_seconds)

            try:
                refreshed_passport = load_passport_with_retry(
                    passport_url=passport_url,
                    timeout=timeout,
                    user_agent=user_agent,
                    attempts=2,
                    sleep_seconds=15.0,
                )

                save_passport(
                    passport=refreshed_passport,
                    passport_path=passport_path,
                )

                refreshed_zip_url = find_zip_url_from_passport(
                    passport=refreshed_passport,
                    expected_zip_name=expected_zip_name,
                )

                if refreshed_zip_url != passport_zip_url:
                    logging.info(
                        "Після оновлення паспорта ZIP URL змінився:\n"
                        "старий: %s\n"
                        "новий:  %s",
                        passport_zip_url,
                        refreshed_zip_url,
                    )
                else:
                    logging.warning(
                        "Після повторного читання паспорта "
                        "ZIP URL не змінився"
                    )

                if try_dataset_url(
                    url=refreshed_zip_url,
                    source="refreshed_passport",
                    zip_path=zip_path,
                    timeout=timeout,
                    user_agent=user_agent,
                    attempted_urls=attempted_urls,
                ):
                    return refreshed_zip_url, "refreshed_passport"

            except Exception as refresh_exc:
                logging.warning(
                    "Не вдалося повторно отримати ZIP URL "
                    "із паспорта: %s",
                    refresh_exc,
                )

        except Exception as passport_exc:
            logging.warning(
                "Не вдалося отримати актуальний ZIP "
                "із паспорта набору даних: %s",
                passport_exc,
            )

    else:
        logging.warning(
            "dataset_passport_url не задано в settings.json"
        )

    # ------------------------------------------------------------------
    # 3. Останній URL, який реально працював у попередньому run
    # ------------------------------------------------------------------

    if last_successful_url:
        logging.warning(
            "Пробую останній успішний URL із попереднього run"
        )

        if try_dataset_url(
            url=last_successful_url,
            source="last_successful_url",
            zip_path=zip_path,
            timeout=timeout,
            user_agent=user_agent,
            attempted_urls=attempted_urls,
        ):
            return last_successful_url, "last_successful_url"

    # ------------------------------------------------------------------
    # 4. Статичний fallback із settings.json
    # ------------------------------------------------------------------

    if fallback_zip_url:
        logging.warning(
            "Пробую fallback dataset_zip_url із settings.json"
        )

        if try_dataset_url(
            url=fallback_zip_url,
            source="settings_fallback",
            zip_path=zip_path,
            timeout=timeout,
            user_agent=user_agent,
            attempted_urls=attempted_urls,
        ):
            return fallback_zip_url, "settings_fallback"

    # ------------------------------------------------------------------
    # Нічого не спрацювало
    # ------------------------------------------------------------------

    raise RuntimeError(
        "Не вдалося завантажити dataset ZIP жодним способом: "
        "passport -> refreshed passport -> "
        "last successful URL -> settings fallback"
    )


# ---------------------------------------------------------------------------
# ZIP EXTRACTION
# ---------------------------------------------------------------------------

def clear_old_csv_files(extract_dir: Path) -> None:
    if not extract_dir.exists():
        return

    for path in extract_dir.glob("*.csv"):
        try:
            path.unlink()
        except Exception as exc:
            logging.warning(
                "Не вдалося видалити старий CSV %s: %s",
                path,
                exc,
            )


def extract_zip(
    zip_path: Path,
    extract_dir: Path,
) -> None:
    extract_dir.mkdir(parents=True, exist_ok=True)

    if not zipfile.is_zipfile(zip_path):
        raise RuntimeError(
            f"Файл не є валідним ZIP: {zip_path}"
        )

    clear_old_csv_files(extract_dir)

    logging.info(
        "Розпакування ZIP: %s -> %s",
        zip_path,
        extract_dir,
    )

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    logging.info("ZIP розпаковано")


def ensure_documents_csv(extract_dir: Path) -> None:
    documents_path = extract_dir / "documents.csv"

    if documents_path.exists():
        logging.info(
            "Знайдено documents.csv: %s",
            documents_path,
        )
        return

    candidates = list(
        extract_dir.rglob("documents.csv")
    )

    if not candidates:
        raise FileNotFoundError(
            "Після розпакування не знайдено documents.csv"
        )

    source = candidates[0]

    shutil.copy2(
        source,
        documents_path,
    )

    logging.info(
        "documents.csv скопійовано з %s до %s",
        source,
        documents_path,
    )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging()
    settings = load_settings()

    archives_dir = ROOT_DIR / "data" / "raw" / "archives"
    archives_dir.mkdir(parents=True, exist_ok=True)

    state_dir = ROOT_DIR / "data" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)

    zip_name = str(
        settings.get(
            "dataset_zip_name",
            "edrsr_data_2026.zip",
        )
    ).strip()

    zip_path = archives_dir / zip_name
    passport_path = archives_dir / "dataset_passport.json"
    resolved_url_path = archives_dir / "resolved_dataset_zip_url.txt"

    last_successful_url_path = (
        state_dir / "last_successful_dataset_url.json"
    )

    # ------------------------------------------------------------------
    # Resolve + download
    # ------------------------------------------------------------------

    successful_url, successful_source = resolve_and_download_dataset(
        settings=settings,
        zip_path=zip_path,
        passport_path=passport_path,
        last_successful_url_path=last_successful_url_path,
    )

    # Записуємо саме URL, який реально спрацював,
    # а не просто URL, знайдений у паспорті.
    resolved_url_path.write_text(
        successful_url,
        encoding="utf-8",
    )

    logging.info(
        "Робочий URL ZIP збережено: %s",
        resolved_url_path,
    )

    # Запам'ятовуємо URL лише після успішного завантаження
    # та перевірки ZIP.
    save_last_successful_dataset_url(
        state_path=last_successful_url_path,
        url=successful_url,
        source=successful_source,
    )

    logging.info(
        "Dataset отримано через: %s",
        successful_source,
    )

    # ------------------------------------------------------------------
    # Extract
    # ------------------------------------------------------------------

    extract_zip(
        zip_path=zip_path,
        extract_dir=archives_dir,
    )

    ensure_documents_csv(
        archives_dir
    )


if __name__ == "__main__":
    main()
