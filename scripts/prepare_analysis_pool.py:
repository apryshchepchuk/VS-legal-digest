from __future__ import annotations

import csv
import logging
import os
import subprocess
import sys
from pathlib import Path

from common import ROOT_DIR, load_json, load_settings, setup_logging


def read_tsv(path: Path) -> list[dict]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader)


def run_script(script_name: str, env: dict[str, str]) -> None:
    script_path = ROOT_DIR / "scripts" / script_name
    logging.info("Запуск %s", script_path.name)

    subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT_DIR),
        env=env,
        check=True,
    )


def count_pending_docs(selected_path: Path, processed_doc_ids: set[str]) -> tuple[int, int]:
    rows = read_tsv(selected_path)
    pending = 0

    for row in rows:
        doc_id = str(row.get("doc_id", "")).strip()
        if not doc_id:
            continue
        if doc_id not in processed_doc_ids:
            pending += 1

    return len(rows), pending


def main() -> None:
    setup_logging()
    settings = load_settings()

    windows = settings.get("selection_lookback_days_sequence", [60, 120, 180, 365])
    if not isinstance(windows, list) or not windows:
        windows = [60, 120, 180, 365]

    windows = [int(x) for x in windows]
    min_pending_docs_to_stop = int(settings.get("min_pending_docs_to_stop", 1))

    state_path = ROOT_DIR / "data" / "state" / "processed_doc_ids.json"
    selected_path = ROOT_DIR / "data" / "interim" / "vp_selected_for_analysis.csv"

    state = load_json(state_path, default={"processed_doc_ids": []})
    processed_doc_ids = set(state.get("processed_doc_ids", []))

    base_env = os.environ.copy()

    chosen_window: int | None = None
    chosen_selected_count = 0
    chosen_pending_count = 0

    for window in windows:
        env = base_env.copy()
        env["LOOKBACK_DAYS_OVERRIDE"] = str(window)

        logging.info("Пробую selection window = %s днів", window)

        run_script("extract_vp_postanovy.py", env=env)
        run_script("fetch_decision_texts.py", env=env)
        run_script("normalize_rtf.py", env=env)
        run_script("select_full_texts.py", env=env)

        selected_count, pending_count = count_pending_docs(selected_path, processed_doc_ids)

        logging.info(
            "Window %s днів: selected=%s, pending_new=%s",
            window,
            selected_count,
            pending_count,
        )

        chosen_window = window
        chosen_selected_count = selected_count
        chosen_pending_count = pending_count

        if pending_count >= min_pending_docs_to_stop:
            logging.info(
                "Знайдено достатньо нових постанов для аналізу на window=%s днів",
                window,
            )
            break

    logging.info(
        "Підсумок prepare_analysis_pool: window=%s, selected=%s, pending_new=%s",
        chosen_window,
        chosen_selected_count,
        chosen_pending_count,
    )


if __name__ == "__main__":
    main()
