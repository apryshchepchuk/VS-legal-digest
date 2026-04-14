from __future__ import annotations

import logging
import os
from pathlib import Path

import requests

from common import ROOT_DIR, load_settings, setup_logging


def main() -> None:
    setup_logging()
    settings = load_settings()

    if not settings.get("telegram_enabled", False):
        logging.info("Telegram вимкнено в settings.json")
        return

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise EnvironmentError("Не задано TELEGRAM_BOT_TOKEN")

    chat_id = str(settings.get("telegram_chat_id", "")).strip()
    if not chat_id:
        raise RuntimeError("У settings.json не задано telegram_chat_id")

    disable_notification = bool(settings.get("telegram_disable_notification", True))

    post_path = ROOT_DIR / "outputs" / "digest" / "daily_telegram_post.txt"
    if not post_path.exists():
        logging.info("Файл daily_telegram_post.txt відсутній, надсилати нічого")
        return

    text = post_path.read_text(encoding="utf-8").strip()
    if not text:
        logging.info("Щоденний Telegram-пост порожній, надсилати нічого")
        return

    base_url = f"https://api.telegram.org/bot{token}"

    # Якщо текст вміщується в sendMessage
    if len(text) <= 4096:
        resp = requests.post(
            f"{base_url}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "disable_notification": disable_notification,
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("ok"):
            raise RuntimeError(f"Telegram sendMessage error: {data}")

        logging.info("Повідомлення успішно надіслано через sendMessage")
        return

    # Якщо текст задовгий — надсилаємо файлом
    with post_path.open("rb") as f:
        resp = requests.post(
            f"{base_url}/sendDocument",
            data={
                "chat_id": chat_id,
                "caption": "Щоденний дайджест постанов Великої Палати ВС",
                "disable_notification": str(disable_notification).lower(),
            },
            files={
                "document": ("daily_telegram_post.txt", f, "text/plain"),
            },
            timeout=120,
        )

    resp.raise_for_status()
    data = resp.json()

    if not data.get("ok"):
        raise RuntimeError(f"Telegram sendDocument error: {data}")

    logging.info("Повідомлення успішно надіслано через sendDocument")


if __name__ == "__main__":
    main()
