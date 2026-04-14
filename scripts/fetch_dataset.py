from __future__ import annotations

import logging
import zipfile
from pathlib import Path

import requests

from common import ROOT_DIR, load_settings, setup_logging, ensure_parent


def download_file(url: str, dest_path: Path, timeout: int, user_agent: str) -> None:
    ensure_parent(dest_path)
    with requests.get(url, stream=True, timeout=timeout, headers={"User-Agent": user_agent}) as response:
        response.raise_for_status()
        with dest_path.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def extract_selected(zip_path: Path, extract_dir: Path, filenames: list[str]) -> None:
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = set(zf.namelist())
        missing = [name for name in filenames if name not in names]
        if missing:
            raise FileNotFoundError(f"У ZIP не знайдено файли: {missing}")

        for name in filenames:
            target = extract_dir / name
            ensure_parent(target)
            with zf.open(name) as src, target.open("wb") as dst:
                dst.write(src.read())


def main() -> None:
    setup_logging()
    settings = load_settings()

    dataset_zip_url = settings["dataset_zip_url"]
    timeout = int(settings.get("request_timeout_seconds", 60))
    user_agent = settings.get("user_agent", "vp-vs-digest/0.1")

    archives_dir = ROOT_DIR / "data" / "raw" / "archives"
    zip_name = dataset_zip_url.rstrip("/").split("/")[-1] or "edrsr_data.zip"
    zip_path = archives_dir / zip_name

    logging.info("Завантаження архіву: %s", dataset_zip_url)
    download_file(dataset_zip_url, zip_path, timeout=timeout, user_agent=user_agent)

    logging.info("Розпакування потрібних файлів")
    extract_selected(
        zip_path=zip_path,
        extract_dir=archives_dir,
        filenames=["documents.csv", "courts.csv", "judgment_forms.csv"],
    )
    logging.info("Готово")


if __name__ == "__main__":
    main()
