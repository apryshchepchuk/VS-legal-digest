from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from google import genai
from google.genai import types
from jsonschema import ValidationError, validate

from common import ROOT_DIR, iter_tsv_rows, load_json, load_settings, save_json, setup_logging


PROMPT_TEMPLATE = """Проаналізуй повний текст постанови Великої Палати Верховного Суду України.

Поверни результат виключно як один валідний JSON-об’єкт.
Не додавай markdown.
Не додавай пояснення.
Не додавай ```json.
Не додавай жодного тексту до або після JSON.
Не вигадуй фактів. Не виходь за межі тексту постанови.
Аналіз має бути завершеним і спиратися на повний текст постанови.
Пиши стисло, чітко, українською мовою.

JSON має містити поля:
- short_summary: 2-4 короткі речення простою мовою, до 400 символів
- key_position: одна головна правова позиція, до 300 символів
- practical_value: чим це важливо для правозастосовної практики, до 300 символів
- public_value: чи має це суспільне значення; якщо ні — прямо зазнач, до 220 символів
- topic_tags: масив із 2-5 коротких тегів
- telegram_line: короткий блок для щоденного Telegram-дайджесту, до 700 символів
- needs_review: true або false

Текст постанови:
"""


class RateLimitError(Exception):
    pass


class TemporaryUnavailableError(Exception):
    pass


class IncompleteJsonError(Exception):
    pass


def build_prompt(text: str) -> str:
    return f"{PROMPT_TEMPLATE}\n\n{text}"


def classify_model_error(exc: Exception) -> Exception:
    message = str(exc).lower()

    rate_limit_markers = [
        "429",
        "resource_exhausted",
        "rate limit",
        "quota",
        "too many requests",
    ]
    if any(marker in message for marker in rate_limit_markers):
        return RateLimitError(str(exc))

    temporary_markers = [
        "503",
        "500",
        "502",
        "504",
        "unavailable",
        "temporarily unavailable",
        "internal",
        "deadline exceeded",
    ]
    if any(marker in message for marker in temporary_markers):
        return TemporaryUnavailableError(str(exc))

    return exc


def call_gemini_once(
    client: genai.Client,
    model: str,
    prompt_text: str,
) -> dict:
    try:
        response = client.models.generate_content(
            model=model,
            contents=prompt_text,
            config=types.GenerateContentConfig(
                temperature=0.1,
                top_p=0.8,
                top_k=20,
                max_output_tokens=900,
            ),
        )
    except Exception as exc:
        raise classify_model_error(exc)

    response_text = (response.text or "").strip()
    if not response_text:
        raise IncompleteJsonError("INCOMPLETE_JSON: порожня відповідь")

    if response_text.startswith("```"):
        response_text = response_text.strip("`").strip()
        if response_text.lower().startswith("json"):
            response_text = response_text[4:].strip()

    try:
        parsed = json.loads(response_text)
    except json.JSONDecodeError as exc:
        snippet = response_text[:800]
        raise IncompleteJsonError(f"INCOMPLETE_JSON: {snippet}") from exc

    return parsed


def post_validate_result(result: dict) -> dict:
    required_text_fields = [
        "short_summary",
        "key_position",
        "practical_value",
        "public_value",
        "telegram_line",
    ]

    needs_review = bool(result.get("needs_review", False))

    for field in required_text_fields:
        value = str(result.get(field, "")).strip()
        if not value:
            needs_review = True
        result[field] = value

    tags = result.get("topic_tags", [])
    if not isinstance(tags, list):
        tags = []
        needs_review = True

    cleaned_tags: list[str] = []
    for tag in tags:
        tag_value = str(tag).strip()
        if tag_value:
            cleaned_tags.append(tag_value)

    if len(cleaned_tags) < 2:
        needs_review = True

    result["topic_tags"] = cleaned_tags[:5]
    result["needs_review"] = needs_review
    return result


def append_unique(values: list[str], value: str) -> None:
    if value and value not in values:
        values.append(value)


def main() -> None:
    setup_logging()
    settings = load_settings()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError("Не задано GEMINI_API_KEY")

    tz_name = settings.get("timezone", "Europe/Kyiv")
    model = settings.get("gemini_model", "gemini-2.5-flash-lite")
    retry_attempts = max(1, int(settings.get("gemini_retry_attempts", 2)))
    max_docs_per_run = max(1, int(settings.get("max_docs_per_run", 4)))
    max_api_requests_per_run = max(1, int(settings.get("max_api_requests_per_run", 8)))
    sleep_after_each_request_seconds = float(settings.get("sleep_after_each_request_seconds", 7))
    stop_after_first_429 = bool(settings.get("stop_after_first_429", True))
    stop_after_consecutive_503 = int(settings.get("stop_after_consecutive_503", 2))

    interim_path = ROOT_DIR / "data" / "interim" / "vp_selected_for_analysis.csv"
    text_dir = ROOT_DIR / "data" / "processed" / "text"
    analysis_dir = ROOT_DIR / "data" / "processed" / "analysis"
    schema_path = ROOT_DIR / "config" / "gemini_schema.json"
    state_path = ROOT_DIR / "data" / "state" / "processed_doc_ids.json"
    last_daily_state_path = ROOT_DIR / "data" / "state" / "last_daily_analyzed_doc_ids.json"

    analysis_dir.mkdir(parents=True, exist_ok=True)
    last_daily_state_path.parent.mkdir(parents=True, exist_ok=True)

    schema = load_json(schema_path, default={})
    if not schema:
        raise RuntimeError("Не знайдено або порожній config/gemini_schema.json")

    state = load_json(state_path, default={"processed_doc_ids": []})
    processed_doc_ids = set(state.get("processed_doc_ids", []))

    if not interim_path.exists():
        raise FileNotFoundError(f"Не знайдено {interim_path}")

    client = genai.Client(
        api_key=api_key,
        http_options=types.HttpOptions(api_version="v1"),
    )

    rows = list(iter_tsv_rows(interim_path))
    pending_rows: list[dict] = []

    for row in rows:
        doc_id = str(row.get("doc_id", "")).strip()
        if not doc_id:
            continue
        if doc_id in processed_doc_ids:
            continue
        pending_rows.append(row)

    rows_to_process = pending_rows[:max_docs_per_run]

    logging.info("Усього записів у vp_selected_for_analysis.csv: %s", len(rows))
    logging.info("Нових записів для аналізу: %s", len(pending_rows))
    logging.info("Буде оброблено в цьому запуску: %s", len(rows_to_process))

    new_processed = 0
    failed_doc_ids: list[str] = []
    analyzed_this_run: list[str] = []
    api_requests_made = 0
    consecutive_503_count = 0
    stop_run = False
    stop_reason = ""

    analyzed_at = datetime.now(ZoneInfo(tz_name)).isoformat()

    for index, row in enumerate(rows_to_process, start=1):
        doc_id = str(row.get("doc_id", "")).strip()
        text_path = text_dir / f"{doc_id}.txt"

        if not text_path.exists():
            logging.warning("Не знайдено текст для doc_id=%s", doc_id)
            append_unique(failed_doc_ids, doc_id)
            continue

        raw_text = text_path.read_text(encoding="utf-8", errors="ignore").strip()
        if not raw_text:
            logging.warning("Порожній текст для doc_id=%s", doc_id)
            append_unique(failed_doc_ids, doc_id)
            continue

        prompt_text = build_prompt(raw_text)
        doc_processed = False

        for attempt in range(1, retry_attempts + 1):
            if api_requests_made >= max_api_requests_per_run:
                stop_reason = (
                    f"Досягнуто внутрішнього ліміту API-викликів за запуск: "
                    f"{api_requests_made}/{max_api_requests_per_run}"
                )
                logging.warning(stop_reason)
                stop_run = True
                break

            api_requests_made += 1
            logging.info(
                "API-виклик %s/%s для doc_id=%s, спроба %s/%s",
                api_requests_made,
                max_api_requests_per_run,
                doc_id,
                attempt,
                retry_attempts,
            )

            try:
                result = call_gemini_once(
                    client=client,
                    model=model,
                    prompt_text=prompt_text,
                )
                validate(instance=result, schema=schema)
                result = post_validate_result(result)

                enriched = {
                    "doc_id": doc_id,
                    "cause_num": row.get("cause_num", ""),
                    "adjudication_date": row.get("adjudication_date", ""),
                    "date_publ": row.get("date_publ", ""),
                    "doc_url": row.get("doc_url", ""),
                    "char_count": row.get("char_count", ""),
                    "selection_reason": row.get("selection_reason", ""),
                    "analyzed_at": analyzed_at,
                    **result,
                }

                out_path = analysis_dir / f"{doc_id}.json"
                save_json(out_path, enriched)

                processed_doc_ids.add(doc_id)
                analyzed_this_run.append(doc_id)
                new_processed += 1
                consecutive_503_count = 0
                doc_processed = True

                logging.info(
                    "Проаналізовано doc_id=%s (%s/%s)",
                    doc_id,
                    index,
                    len(rows_to_process),
                )
                break

            except RateLimitError as exc:
                append_unique(failed_doc_ids, doc_id)
                stop_reason = f"Отримано 429/Rate Limit від Gemini: {exc}"
                logging.error(stop_reason)
                if stop_after_first_429:
                    stop_run = True
                break

            except TemporaryUnavailableError as exc:
                consecutive_503_count += 1
                logging.warning(
                    "Тимчасова недоступність Gemini для doc_id=%s, спроба %s/%s. Помилка: %s",
                    doc_id,
                    attempt,
                    retry_attempts,
                    exc,
                )

                if attempt >= retry_attempts:
                    append_unique(failed_doc_ids, doc_id)

                if stop_after_consecutive_503 > 0 and consecutive_503_count >= stop_after_consecutive_503:
                    stop_reason = (
                        f"Зупинка після {consecutive_503_count} поспіль помилок 503/temporary unavailable"
                    )
                    logging.error(stop_reason)
                    stop_run = True
                    append_unique(failed_doc_ids, doc_id)
                    break

            except (IncompleteJsonError, ValidationError) as exc:
                consecutive_503_count = 0
                logging.warning(
                    "Невалідна або неповна JSON-відповідь для doc_id=%s, спроба %s/%s. Помилка: %s",
                    doc_id,
                    attempt,
                    retry_attempts,
                    exc,
                )
                if attempt >= retry_attempts:
                    append_unique(failed_doc_ids, doc_id)

            except Exception as exc:
                consecutive_503_count = 0
                logging.exception("Неочікувана помилка Gemini для doc_id=%s: %s", doc_id, exc)
                append_unique(failed_doc_ids, doc_id)
                break

            finally:
                if sleep_after_each_request_seconds > 0:
                    logging.info(
                        "Пауза %.1f сек після API-виклику",
                        sleep_after_each_request_seconds,
                    )
                    time.sleep(sleep_after_each_request_seconds)

        if stop_run:
            break

        if not doc_processed:
            logging.info("doc_id=%s не було проаналізовано в цьому запуску", doc_id)

    save_json(state_path, {"processed_doc_ids": sorted(processed_doc_ids)})
    save_json(
        last_daily_state_path,
        {
            "run_at": analyzed_at,
            "doc_ids": analyzed_this_run,
        },
    )

    logging.info("API-викликів за запуск: %s", api_requests_made)
    logging.info("Нових проаналізованих постанов: %s", new_processed)
    logging.info("Не вдалося проаналізувати постанов: %s", len(failed_doc_ids))

    if analyzed_this_run:
        logging.info("doc_id, проаналізовані в цьому запуску: %s", ", ".join(analyzed_this_run))

    if failed_doc_ids:
        logging.warning("Список doc_id з помилками: %s", ", ".join(failed_doc_ids))

    if stop_reason:
        logging.warning("Причина дострокової зупинки: %s", stop_reason)

    if rows_to_process and new_processed == 0:
        raise RuntimeError(
            "Не вдалося проаналізувати жодної постанови."
            + (f" {stop_reason}" if stop_reason else "")
        )


if __name__ == "__main__":
    main()
