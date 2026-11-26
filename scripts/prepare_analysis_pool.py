from __future__ import annotations

import csv
import logging
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

from common import ROOT_DIR, load_json, load_settings, save_json, setup_logging
from fetch_dataset import ensure_daily_window, run_dir
from select_full_texts import build_group_key, sort_group_rows, write_tsv


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
    pending_ids: set[str] = set()

    for row in rows:
        doc_id = str(row.get("doc_id", "")).strip()
        if not doc_id:
            continue
        if doc_id not in processed_doc_ids:
            pending_ids.add(doc_id)

    return len(rows), len(pending_ids)


def exclude_processed_groups(path: Path, processed_doc_ids: set[str]) -> None:
    """Skip a group only if its latest version has already been analyzed.

    Keep all versions of a new latest document for full-text comparison.
    Reuse the exact grouping and ordering rules of select_full_texts.py.
    """
    if not processed_doc_ids:
        return

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = reader.fieldnames
        rows = list(reader)

    if not fieldnames or not {"doc_id", "cause_num", "adjudication_date", "date_publ"}.issubset(fieldnames):
        raise ValueError(f"Некоректний заголовок файлу кандидатів: {path}")

    groups = defaultdict(list)
    for row in rows:
        groups[build_group_key(row)].append(row)

    excluded_groups = {
        key for key, group in groups.items()
        if str(sort_group_rows(group)[0].get("doc_id", "")).strip() in processed_doc_ids
    }
    remaining = [row for row in rows if build_group_key(row) not in excluded_groups]
    if excluded_groups:
        write_tsv(path, remaining, fieldnames)

    logging.info(
        "Кандидатів: %s | опрацьованих груп: %s | виключено записів: %s | залишилось: %s",
        len(rows), len(excluded_groups), len(rows) - len(remaining), len(remaining),
    )


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
    settings: dict,
) -> tuple[int, int]:
    env = base_env.copy()
    env["LOOKBACK_DAYS_OVERRIDE"] = str(window)

    logging.info("Пробую stream=%s, selection window=%s днів", extract_script_name, window)

    ensure_daily_window(settings, window)
    run_script(extract_script_name, env=env)
    exclude_processed_groups(
        ROOT_DIR / "data" / "interim" / "vp_candidates.csv", processed_doc_ids
    )
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
    min_pending_docs_to_stop = max(1, int(settings.get("min_pending_docs_to_stop", 1)))
    fallback_max_docs = max(1, int(settings.get("vs_fallback_max_docs_per_run", 3)))
    # Резерв двох кандидатів на випадок невдалого Gemini-аналізу.
    fallback_min_pending = fallback_max_docs + 2
    logging.info(
        "Ціль пулу: ВП — %s, ВС fallback — %s (ціль аналізу — %s)",
        min_pending_docs_to_stop, fallback_min_pending, fallback_max_docs,
    )

    vs_fallback_enabled = bool(settings.get("vs_fallback_enabled", False))
    vs_fallback_only_if_no_new_vp = bool(settings.get("vs_fallback_only_if_no_new_vp", True))

    state_path = ROOT_DIR / "data" / "state" / "processed_doc_ids.json"
    selected_path = ROOT_DIR / "data" / "interim" / "vp_selected_for_analysis.csv"
    pool_state_path = ROOT_DIR / "data" / "state" / "selected_analysis_pool.json"

    state = load_json(state_path, default={"processed_doc_ids": []})
    processed_doc_ids = {str(doc_id).strip() for doc_id in state.get("processed_doc_ids", [])}

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
            settings=settings,
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
                    settings=settings,
                )

                chosen_stream = "vs_fallback"
                chosen_window = window
                chosen_selected_count = selected_count
                chosen_pending_count = pending_count

                if pending_count >= fallback_min_pending:
                    logging.info(
                        "Знайдено достатньо нових постанов ВС fallback на window=%s днів: %s/%s",
                        window, pending_count, fallback_min_pending,
                    )
                    break

    if chosen_stream == "vs_fallback" and chosen_pending_count < fallback_min_pending:
        logging.info(
            "Вичерпано вікна ВС fallback: доступно %s із бажаних %s кандидатів. "
            "Аналізуємо наявні.", chosen_pending_count, fallback_min_pending,
        )

    dataset_manifest = load_json(run_dir("daily") / "manifest.json", default={})
    save_json(
        pool_state_path,
        {
            "source_stream": chosen_stream,
            "dataset_years": dataset_manifest.get("years", []),
            "dataset_latest_publications": {
                year: item.get("latest_date_publ")
                for year, item in dataset_manifest.get("datasets", {}).items()
            },
            "window_days": chosen_window,
            "selected_count": chosen_selected_count,
            "pending_new_count": chosen_pending_count,
            "min_pending_docs_to_stop": (
                fallback_min_pending if chosen_stream == "vs_fallback" else min_pending_docs_to_stop
            ),
            "vs_fallback_min_pending_docs_to_stop": fallback_min_pending,
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
