from __future__ import annotations

import logging
import os

import requests

from common import ROOT_DIR, load_settings, setup_logging
from build_daily_telegram_post import (
    load_publication_state, read_ids, render_batch, save_publication_state,
)


def release_batch(state: dict) -> None:
    """Лише коли достеменно відомо, що Telegram не прийняв повідомлення."""
    state["in_flight"] = []
    save_publication_state(state)


def main() -> None:
    setup_logging()
    settings = load_settings()
    state = load_publication_state()
    doc_ids = state.get("in_flight", [])
    if not doc_ids:
        logging.info("Немає підготовленої добірки для відправлення")
        return

    # До HTTP-запиту невдача є однозначною: нічого не надіслано.
    try:
        if not settings.get("telegram_enabled", False):
            raise RuntimeError("Telegram вимкнено")
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            raise RuntimeError("Не задано TELEGRAM_BOT_TOKEN")
        chat_id = str(settings.get("telegram_chat_id", "")).strip()
        if not chat_id:
            raise RuntimeError("У settings.json не задано telegram_chat_id")
        disable_notification = bool(settings.get("telegram_disable_notification", True))
        post_path = ROOT_DIR / "outputs" / "digest" / "daily_telegram_post.txt"
        text = post_path.read_text(encoding="utf-8").strip()
        if not text or text != render_batch(doc_ids, settings).strip():
            raise RuntimeError("Текст не відповідає підготовленим doc_id")
    except Exception:
        release_batch(state)
        raise

    base_url = f"https://api.telegram.org/bot{token}"
    # Не повторюємо HTTP-запит автоматично: timeout не доводить, що пост не надіслано.
    try:
        if len(text.encode("utf-16-le")) // 2 <= 4096:
            response = requests.post(
                f"{base_url}/sendMessage",
                json={"chat_id": chat_id, "text": text, "disable_notification": disable_notification},
                timeout=60, allow_redirects=False,
            )
        else:
            response = requests.post(
                f"{base_url}/sendDocument",
                data={"chat_id": chat_id, "caption": text.splitlines()[0], "disable_notification": str(disable_notification).lower()},
                files={"document": ("daily_telegram_post.txt", text.encode("utf-8"), "text/plain")},
                timeout=120, allow_redirects=False,
            )
    except requests.RequestException:
        # Не виводимо URL винятку: він може містити токен бота.
        raise RuntimeError("Telegram: мережева помилка, результат невідомий; in_flight збережено") from None

    try:
        data = response.json()
    except ValueError:
        raise RuntimeError(f"Telegram: невідома відповідь HTTP {response.status_code}; in_flight збережено") from None

    if isinstance(data, dict) and data.get("ok") is False and (200 <= response.status_code < 300 or 400 <= response.status_code < 500):
        # Telegram однозначно відхилив запит; наступний run може повторити публікацію.
        release_batch(state)
        raise RuntimeError(f"Telegram відхилив запит (HTTP {response.status_code}, error_code={data.get('error_code')})")

    result = data.get("result") if isinstance(data, dict) else None
    if not (
        isinstance(data, dict)
        and 200 <= response.status_code < 300
        and data.get("ok") is True
        and isinstance(result, dict)
        and isinstance(result.get("message_id"), int)
    ):
        raise RuntimeError(f"Telegram: результат невідомий (HTTP {response.status_code}); in_flight збережено")

    posted = read_ids(state["posted_doc_ids"], "posted_doc_ids")
    posted.update(doc_ids)
    state["posted_doc_ids"] = sorted(posted)
    state["in_flight"] = []
    save_publication_state(state)
    logging.info("Telegram підтвердив публікацію message_id=%s; doc_id=%s", result["message_id"], ", ".join(doc_ids))


if __name__ == "__main__":
    main()
