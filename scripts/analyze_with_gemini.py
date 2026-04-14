from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from google import genai
from google.genai import types
from jsonschema import ValidationError, validate

from common import ROOT_DIR, iter_tsv_rows, load_json, load_settings, save_json, setup_logging


PROMPT_TEMPLATE = """Проаналізуй повний текст постанови Великої Палати Верховного Суду України.

Поверни лише валідний JSON без пояснень, markdown і зайвого тексту.
Не вигадуй фактів. Не виходь за межі тексту постанови.
Аналіз має бути завершеним і спиратися на повний текст постанови.
Пиши стисло, чітко, українською мовою.

Вимоги до полів:
- short_summary: 2-4 короткі речення простою мовою, до 400 символів
- key_position: одна головна правова позиція, до 300 символів
- practical_value: чим це важливо для правозастосовної практики, до 300 символів
- public_value: чи має це суспільне значення; якщо ні — прямо зазнач, до 220 символів
- topic_tags: від 2 до 5 коротких тегів
- telegram_line: короткий блок для тижневого Telegram-дайджесту, до 700 символів
- needs_review: true, якщо текст неповний, нечіткий або висновок важко встановити

Текст постанови:
"""


def build_prompt(text: str) -> str:
    return f"{PROMPT_TEMPLATE}\n\n{text}"


def is_retryable_error(exc: Exception) -> bool:
    message = str(exc).lower()
    retry_markers = [
        "503",
        "500",
        "502",
        "504",
        "429",
        "unavailable",
        "resource_exhausted",
        "rate limit",
        "internal",
        "deadline exceeded",
        "temporarily unavailable",
    ]
    return any(marker in message for marker in retry_markers)


def call_gemini(
    client: genai.Client,
    model: str,
    prompt_text: str,
    schema: dict,
) -> dict:
    response = client.models.generate_content(
        model=model,
        contents=prompt_text,
        config=types.GenerateContentConfig(
            temperature=0.1,
            top_p=0.8,
            top_k=20,
            max_output_tokens=2048,
            response_mime_type="application/json",
            response_json_schema=schema,
        ),
    )

    response_text = (response.text or "").strip()
    if not response_text:
        raise RuntimeError("Gemini повернув порожню відповідь")

    try:
        parsed = json.loads(response_text)
    except json.JSONDecodeError as exc:
        snippet = response_text[:1000]
        raise RuntimeError(f"Gemini повернув невалідний JSON: {snippet}") from exc

    return parsed


def call_gemini_with_retry(
    client: genai.Client,
    model: str,
    prompt_text: str,
    schema: dict,
    max_attempts: int = 5,
) -> dict:
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return call_gemini(
                client=client,
                model=model,
                prompt_text=prompt_text,
                schema=schema,
            )
        except Exception as exc:
            last_exc = exc

            if is_retryable_error(exc) and attempt < max_attempts:
                wait_seconds = 5 * (2 ** (attempt - 1))
                logging.warning(
                    "Gemini тимчасово недоступний, спроба %s/%s, очікування %s сек. Помилка: %s",
                    attempt,
                    max_attempts,
                    wait_seconds,
                    exc,
                )
                time.sleep(wait_seconds)
                continue

            raise

    if last_exc is not None:
        raise last_exc

    raise RuntimeError("Невідома помилка виклику Gemini")


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


def main() -> None:
    setup_logging()
    settings = load_settings()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError("Не задано GEMINI_API_KEY")

    model = settings.get("gemini_model", "gemini-3.1-flash-lite-preview")
    retry_attempts = int(settings.get("gemini_retry_attempts", 5))
    sleep_between_docs_seconds = float(settings.get("sleep_between_docs_seconds", 2))

    interim_path = ROOT_DIR / "data" / "interim" / "vp_last7.csv"
    text_dir = ROOT_DIR / "data" / "processed" / "text"
    analysis_dir = ROOT_DIR / "data" / "processed" / "analysis"
    schema_path = ROOT_DIR / "config" / "gemini_schema.json"
    state_path = ROOT_DIR / "data" / "state" / "processed_doc_ids.json"

    analysis_dir.mkdir(parents=True, exist_ok=True)

    schema = load_json(schema_path, default={})
    if not schema:
        raise RuntimeError("Не знайдено або порожній config/gemini_schema.json")

    state = load_json(state_path, default={"processed_doc_ids": []})
    processed_doc_ids = set(state.get("processed_doc_ids", []))

    if not interim_path.exists():
        raise FileNotFoundError(f"Не знайдено {interim_path}")

    client = genai.Client(api_key=api_key)

    rows = list(iter_tsv_rows(interim_path))
    pending_rows = []
    for row in rows:
        doc_id = str(row.get("doc_id", "")).strip()
        if not doc_id:
            continue
        if doc_id in processed_doc_ids:
            continue
        pending_rows.append(row)

    logging.info("Усього записів у vp_last7.csv: %s", len(rows))
    logging.info("Нових записів для аналізу: %s", len(pending_rows))

    new_processed = 0
    failed_doc_ids: list[str] = []

    for index, row in enumerate(pending_rows, start=1):
        doc_id = str(row.get("doc_id", "")).strip()
        text_path = text_dir / f"{doc_id}.txt"

        if not text_path.exists():
            logging.warning("Не знайдено текст для doc_id=%s", doc_id)
            failed_doc_ids.append(doc_id)
            continue

        raw_text = text_path.read_text(encoding="utf-8", errors="ignore").strip()
        if not raw_text:
            logging.warning("Порожній текст для doc_id=%s", doc_id)
            failed_doc_ids.append(doc_id)
            continue

        prompt_text = build_prompt(raw_text)

        try:
            result = call_gemini_with_retry(
                client=client,
                model=model,
                prompt_text=prompt_text,
                schema=schema,
                max_attempts=retry_attempts,
            )
            validate(instance=result, schema=schema)
            result = post_validate_result(result)
        except (ValidationError, RuntimeError) as exc:
            logging.exception("Помилка Gemini для doc_id=%s: %s", doc_id, exc)
            failed_doc_ids.append(doc_id)
            continue
        except Exception as exc:
            logging.exception("Неочікувана помилка Gemini для doc_id=%s: %s", doc_id, exc)
            failed_doc_ids.append(doc_id)
            continue

        enriched = {
            "doc_id": doc_id,
            "cause_num": row.get("cause_num", ""),
            "adjudication_date": row.get("adjudication_date", ""),
            "date_publ": row.get("date_publ", ""),
            "doc_url": row.get("doc_url", ""),
            **result,
        }

        out_path = analysis_dir / f"{doc_id}.json"
        save_json(out_path, enriched)

        processed_doc_ids.add(doc_id)
        new_processed += 1

        logging.info(
            "Проаналізовано doc_id=%s (%s/%s)",
            doc_id,
            index,
            len(pending_rows),
        )

        if sleep_between_docs_seconds > 0:
            time.sleep(sleep_between_docs_seconds)

    save_json(state_path, {"processed_doc_ids": sorted(processed_doc_ids)})

    logging.info("Нових проаналізованих постанов: %s", new_processed)
    logging.info("Не вдалося проаналізувати постанов: %s", len(failed_doc_ids))

    if pending_rows and new_processed == 0:
        raise RuntimeError("Gemini не зміг проаналізувати жодну постанову")

    if failed_doc_ids:
        logging.warning("Список doc_id з помилками: %s", ", ".join(failed_doc_ids))


if __name__ == "__main__":
    main()
