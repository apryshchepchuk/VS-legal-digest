from __future__ import annotations

import json
import logging
import os
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
        raise RuntimeError(f"Gemini повернув невалідний JSON: {response_text[:1000]}") from exc

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
    if not isinstance(tags, list) or len(tags) < 2:
        needs_review = True
        result["topic_tags"] = tags if isinstance(tags, list) else []

    result["needs_review"] = needs_review
    return result


def main() -> None:
    setup_logging()
    settings = load_settings()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError("Не задано GEMINI_API_KEY")

    model = settings.get("gemini_model", "gemini-3.1-flash-lite-preview")

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
    new_processed = 0

    for row in rows:
        doc_id = (row.get("doc_id") or "").strip()
        if not doc_id:
            continue
        if doc_id in processed_doc_ids:
            continue

        text_path = text_dir / f"{doc_id}.txt"
        if not text_path.exists():
            logging.warning("Не знайдено текст для doc_id=%s", doc_id)
            continue

        raw_text = text_path.read_text(encoding="utf-8", errors="ignore").strip()
        if not raw_text:
            logging.warning("Порожній текст для doc_id=%s", doc_id)
            continue

        prompt_text = build_prompt(raw_text)

        try:
            result = call_gemini(
                client=client,
                model=model,
                prompt_text=prompt_text,
                schema=schema,
            )
            validate(instance=result, schema=schema)
            result = post_validate_result(result)
        except (ValidationError, RuntimeError) as exc:
            logging.exception("Помилка Gemini для doc_id=%s: %s", doc_id, exc)
            continue
        except Exception as exc:
            logging.exception("Неочікувана помилка Gemini для doc_id=%s: %s", doc_id, exc)
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
        logging.info("Проаналізовано doc_id=%s", doc_id)

    save_json(state_path, {"processed_doc_ids": sorted(processed_doc_ids)})
    logging.info("Нових проаналізованих постанов: %s", new_processed)


if __name__ == "__main__":
    main()
