from __future__ import annotations

import logging
from pathlib import Path

import requests

from common import ROOT_DIR, iter_tsv_rows, load_settings, setup_logging


def main() -> None:
    setup_logging()
    settings = load_settings()

    input_path = ROOT_DIR / "data" / "interim" / "vp_last30.csv"
    output_dir = ROOT_DIR / "data" / "raw" / "docs_rtf"
    output_dir.mkdir(parents=True, exist_ok=True)
    timeout = int(settings.get("request_timeout_seconds", 60))
    user_agent = settings.get("user_agent", "vp-vs-digest/0.1")

    if not input_path.exists():
        raise FileNotFoundError(f"Не знайдено {input_path}. Спершу запустіть extract_vp_postanovy.py")

    rows = list(iter_tsv_rows(input_path))
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    downloaded = 0
    skipped = 0

    for row in rows:
        doc_id = row.get("doc_id")
        doc_url = row.get("doc_url")

        if not doc_id:
            logging.warning("Пропуск рядка без doc_id")
            continue
        if not doc_url:
            logging.warning("Пропуск doc_id=%s: порожній doc_url", doc_id)
            continue

        suffix = ".rtf" if doc_url.lower().endswith(".rtf") else ".html"
        target_path = output_dir / f"{doc_id}{suffix}"
        if target_path.exists():
            skipped += 1
            continue

        try:
            response = session.get(doc_url, timeout=timeout)
            response.raise_for_status()
            target_path.write_bytes(response.content)
            downloaded += 1
        except requests.RequestException as exc:
            logging.exception("Не вдалося завантажити doc_id=%s: %s", doc_id, exc)

    logging.info("Завантажено: %s", downloaded)
    logging.info("Пропущено як уже існуючі: %s", skipped)


if __name__ == "__main__":
    main()
