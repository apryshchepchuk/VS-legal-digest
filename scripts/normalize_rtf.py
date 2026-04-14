from __future__ import annotations

import logging
from pathlib import Path

from striprtf.striprtf import rtf_to_text

from common import ROOT_DIR, setup_logging


def main() -> None:
    setup_logging()

    source_dir = ROOT_DIR / "data" / "raw" / "docs_rtf"
    out_dir = ROOT_DIR / "data" / "processed" / "text"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not source_dir.exists():
        logging.info("Папка %s відсутня, конвертувати нічого", source_dir)
        return

    converted = 0

    for path in source_dir.iterdir():
        if path.suffix.lower() != ".rtf":
            continue

        raw = path.read_text(encoding="utf-8", errors="ignore")
        text = rtf_to_text(raw)
        text = "\n".join(line.rstrip() for line in text.splitlines())
        text = "\n".join(line for line in text.splitlines() if line.strip()).strip()

        out_path = out_dir / f"{path.stem}.txt"
        out_path.write_text(text, encoding="utf-8")
        converted += 1

    logging.info("Сконвертовано RTF у TXT: %s", converted)


if __name__ == "__main__":
    main()
