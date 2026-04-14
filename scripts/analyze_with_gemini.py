from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import requests
from jsonschema import validate, ValidationError

from common import ROOT_DIR, iter_tsv_rows, load_json, load_settings, save_json, setup_logging


PROMPT_TEMPLATE = """Проаналізуй текст постанови Великої Палати Верховного Суду України.
Поверни лише валідний JSON без пояснень, markdown і зайвого тексту.
Не вигадуй фактів. Якщо чогось немає в тексті, прямо зазнач це.
Пиши стисло, українською мовою.

Обмеження:
- short_summary: до 400 символів
- key_position: до 300 символів
- practical_value: до 300 символів
- public_value: до 220 символів
- topic_tags: 2-5 коротких тегів
- telegram_line: до 700 символів
- needs_review: true, якщо текст неповний, нечіткий або висновок важко встановити

Поля JSON:
short_summary
key_position
practical_value
public_value
topic_tags
telegram_line
needs_review

Текст постанови:
"""


def call_gemini(api_key: str, model: str, text: str, timeout: int) -> dict:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    payload = {
        "contents": [
            {
                "parts": [
                    {"text": PROMPT_TEMPLATE + "\n\n" + text}
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json"
        }
    }
    response = requests.post(url, json=payload, timeout=timeout)
    response.raise_for_status()
    data = response.json()

    try:
        text_payload = data["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"Неочікувана відповідь Gemini: {data}") from exc

    return json.loads(text_payload)


def main() -> None:
    setup_logging()
    settings = load_settings()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError("Не задано GEMINI_API_KEY")

    model = settings.get("gemini_model", "gemini-2.5-flash")
    timeout = int(settings.get("request_timeout_seconds", 60))

    interim_path = ROOT_DIR / "data" / "interim" / "vp_last7.csv"
    text_dir = ROOT_DIR / "data" / "processed" / "text"
    analysis_dir = ROOT_DIR / "data" / "processed" / "analysis"
    schema_path = ROOT_DIR / "config" / "gemini_schema.json"
    state_path = ROOT_DIR / "data" / "state" / "processed_doc_ids.json"

    analysis_dir.mkdir(parents=True, exist_ok=True)

    schema = load_json(schema_path, default={})
    state = load_json(state_path, default={"processed_doc_ids": []})
    processed_doc_ids = set(state.get("processed_doc_ids", []))

    rows = list(iter_tsv_rows(interim_path))
    new_processed = 0

    for row in rows:
        doc_id = row.get("doc_id")
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

        try:
            result = call_gemini(api_key=api_key, model=model, text=raw_text, timeout=timeout)
            validate(instance=result, schema=schema)
        except (requests.RequestException, json.JSONDecodeError, ValidationError, RuntimeError) as exc:
            logging.exception("Помилка Gemini для doc_id=%s: %s", doc_id, exc)
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

    save_json(state_path, {"processed_doc_ids": sorted(processed_doc_ids)})
    logging.info("Нових проаналізованих постанов: %s", new_processed)


if __name__ == "__main__":
    main()
