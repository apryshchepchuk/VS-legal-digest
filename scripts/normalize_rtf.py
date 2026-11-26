from __future__ import annotations

import logging

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
    reused = 0
    failed = 0

    for path in source_dir.iterdir():
        if path.suffix.lower() != ".rtf":
            continue

        out_path = out_dir / f"{path.stem}.txt"
        if (
            out_path.is_file()
            and out_path.stat().st_size > 0
            and out_path.stat().st_mtime_ns >= path.stat().st_mtime_ns
        ):
            reused += 1
            continue

        try:
            # Remove stale or empty text so a failed conversion cannot be selected.
            out_path.unlink(missing_ok=True)
            raw = path.read_text(encoding="utf-8", errors="ignore")
            text = rtf_to_text(raw)
            text = "\n".join(line.rstrip() for line in text.splitlines())
            text = "\n".join(line for line in text.splitlines() if line.strip()).strip()
            if not text:
                raise ValueError("Порожній текст після конвертації")
            out_path.write_text(text, encoding="utf-8")
            converted += 1
        except Exception as exc:
            out_path.unlink(missing_ok=True)
            failed += 1
            logging.warning("Не вдалося конвертувати %s: %s", path.name, exc)

    logging.info("TXT: конвертовано=%s | повторно використано=%s | помилок=%s", converted, reused, failed)


if __name__ == "__main__":
    main()
