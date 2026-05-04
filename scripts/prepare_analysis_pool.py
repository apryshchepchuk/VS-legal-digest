from __future__ import annotations

import csv
import logging
import os
import subprocess
import sys
from pathlib import Path

from common import ROOT_DIR, load_json, load_settings, save_json, setup_logging


def read_tsv(path: Path) -> list[dict]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader)


def run_script(script_name: str, env: dict[str, str]) -> None:
    script_path = ROOT_DIR / "scripts" / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Не знайдено {script_path}")

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


def normalize_windows(windows: object) -> list[int]:
    if not isinstance(windows, list) or not windows:
        return [60, 120, 180, 365]

    result: list[int] = []
    seen: set[int] = set()

    for item in windows:
        try:
            value = int(item)
        except Exception:
            continue

        if value <= 0:
            continue

        if value not in seen:
            seen.add(value)
            result.append(value)

    return result or [60, 120, 180, 365]


def run_selection_pipeline(
    extract_script_name: str,
    window: int,
    processed_doc_ids: set[str],
    selected_path: Path,
    base_env: dict[str, str],
) -> tuple[int, int]:
    env = base_env.copy()
    env["LOOKBACK_DAYS_OVERRIDE"] = str(window)

    logging.info("Пробую stream=%s, selection window=%s днів", extract_script_name, window)

    run_script(extract_script_name, env=env)
    run_script("fetch_decision_texts.py", env=env)
    run_script("normalize_rtf.py", env=env)
    run_script("select_full_texts.py", env=env)

    selected_count, pending_count = count_pending_docs(selected_path, processed_doc_ids)

    logging.info(
        "Stream=%s | window=%s днів | selected=%s | pending_new=%s",
        extract_script_name,
        window,
        selected_count,
        pending_count,
    )

    return selected_count, pending_count


def main() -> None:
    setup_logging()
    settings = load_settings()

    windows = normalize_windows(settings.get("selection_lookback_days_sequence", [60, 120, 180, 365]))
    min_pending_docs_to_stop = int(settings.get("min_pending_docs_to_stop", 1))

    vs_fallback_enabled = bool(settings.get("vs_fallback_enabled", False))
    vs_fallback_only_if_no_new_vp = bool(settings.get("vs_fallback_only_if_no_new_vp", True))

    state_path = ROOT_DIR / "data" / "state" / "processed_doc_ids.json"
    selected_path = ROOT_DIR / "data" / "interim" / "vp_selected_for_analysis.csv"
    pool_state_path = ROOT_DIR / "data" / "state" / "selected_analysis_pool.json"

    state = load_json(state_path, default={"processed_doc_ids": []})
    processed_doc_ids = set(state.get("processed_doc_ids", []))

    base_env = os.environ.copy()

    chosen_stream = "vp"
    chosen_window: int | None = None
    chosen_selected_count = 0
    chosen_pending_count = 0

    # 1. Спершу пробуємо ВП
    for window in windows:
        selected_count, pending_count = run_selection_pipeline(
            extract_script_name="extract_vp_postanovy.py",
            window=window,
            processed_doc_ids=processed_doc_ids,
            selected_path=selected_path,
            base_env=base_env,
        )

        chosen_stream = "vp"
        chosen_window = window
        chosen_selected_count = selected_count
        chosen_pending_count = pending_count

        if pending_count >= min_pending_docs_to_stop:
            logging.info(
                "Знайдено достатньо нових постанов ВП для аналізу на window=%s днів",
                window,
            )
            break

    vp_pending_found = chosen_pending_count >= min_pending_docs_to_stop

    # 2. Якщо для ВП нових постанов немає — пробуємо fallback на ВС
    should_try_vs_fallback = (
        vs_fallback_enabled
        and (
            (vs_fallback_only_if_no_new_vp and not vp_pending_found)
            or (not vs_fallback_only_if_no_new_vp)
        )
    )

    fallback_script_name = "extract_vs_fallback_postanovy.py"
    fallback_script_path = ROOT_DIR / "scripts" / fallback_script_name

    if should_try_vs_fallback:
        if not fallback_script_path.exists():
            logging.warning(
                "Увімкнено fallback на ВС, але не знайдено %s. Пропускаю fallback.",
                fallback_script_path,
            )
        else:
            logging.info("Нових постанов ВП недостатньо. Переходжу до fallback-пошуку по ВС.")

            for window in windows:
                selected_count, pending_count = run_selection_pipeline(
                    extract_script_name=fallback_script_name,
                    window=window,
                    processed_doc_ids=processed_doc_ids,
                    selected_path=selected_path,
                    base_env=base_env,
                )

                chosen_stream = "vs_fallback"
                chosen_window = window
                chosen_selected_count = selected_count
                chosen_pending_count = pending_count

                if pending_count >= min_pending_docs_to_stop:
                    logging.info(
                        "Знайдено достатньо нових постанов ВС fallback для аналізу на window=%s днів",
                        window,
                    )
                    break

    save_json(
        pool_state_path,
        {
            "source_stream": chosen_stream,
            "window_days": chosen_window,
            "selected_count": chosen_selected_count,
            "pending_new_count": chosen_pending_count,
            "min_pending_docs_to_stop": min_pending_docs_to_stop,
            "windows_tried": windows,
            "vs_fallback_enabled": vs_fallback_enabled,
            "vs_fallback_only_if_no_new_vp": vs_fallback_only_if_no_new_vp,
        },
    )

    logging.info(
        "Підсумок prepare_analysis_pool: stream=%s, window=%s, selected=%s, pending_new=%s",
        chosen_stream,
        chosen_window,
        chosen_selected_count,
        chosen_pending_count,
    )
    logging.info("Стан обраного пулу збережено: %s", pool_state_path)


if __name__ == "__main__":
    main()
