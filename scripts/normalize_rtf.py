from __future__ import annotations

import logging
import re
from pathlib import Path

from striprtf.striprtf import rtf_to_text

from common import ROOT_DIR, setup_logging


def normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def convert_one(file_path: Path, out_dir: Path) -> Path:
    raw = file_path.read_text(encoding="utf-8", errors="ignore")
    plain = rtf_to_text(raw) if file_path.suffix.lower() == ".rtf" else raw
    cleaned = normalize_text(plain)

    out_path = out_dir / f"{file_path.stem}.txt"
    out_path.write_text(cleaned, encoding="utf-8")
    return out_path


def main() -> None:
    setup_logging()

    source_dir = ROOT_DIR / "data" / "raw" / "docs_rtf"
    out_dir = ROOT_DIR / "data" / "processed" / "text"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not source_dir.exists():
    logging.info("Папка %s відсутня, конвертувати нічого", source_dir)
    return

    files = sorted([p for p in source_dir.iterdir() if p.is_file() and p.suffix.lower() in {".rtf", ".html"}])
    converted = 0

    for file_path in files:
        out_path = out_dir / f"{file_path.stem}.txt"
        if out_path.exists():
            continue
        try:
            convert_one(file_path, out_dir)
            converted += 1
        except Exception as exc:  # noqa: BLE001
            logging.exception("Помилка під час конвертації %s: %s", file_path.name, exc)

    logging.info("Сконвертовано файлів: %s", converted)


if __name__ == "__main__":
    main()
