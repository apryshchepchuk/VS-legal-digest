# Дайджест постанов Великої Палати ВС

MVP-проєкт для автоматичного збору, аналізу та підготовки щотижневого дайджесту
постанов Великої Палати Верховного Суду України.

## Що робить MVP

1. Завантажує архів відкритих даних ЄДРСР.
2. Фільтрує лише:
   - `court_code = 9951` — Велика Палата Верховного Суду
   - `judgment_code = 2` — постанова
   - `status = 1` — активне рішення
   - `date_publ` у межах останніх 7 днів
3. Завантажує тексти рішень за `doc_url`.
4. Нормалізує RTF у plain text.
5. Відправляє текст у Gemini API.
6. Зберігає окремий JSON по кожній постанові.
7. Формує:
   - `outputs/digest/weekly_digest.md`
   - `outputs/digest/telegram_post.txt`

## Структура репозиторію

```text
vp-vs-digest/
  README.md
  requirements.txt
  .gitignore

  config/
    settings.json
    gemini_schema.json

  data/
    raw/
      archives/
      docs_rtf/
    interim/
    processed/
      text/
      analysis/
    state/

  outputs/
    digest/

  scripts/
    fetch_dataset.py
    extract_vp_postanovy.py
    fetch_decision_texts.py
    normalize_rtf.py
    analyze_with_gemini.py
    build_weekly_digest.py

  .github/
    workflows/
      weekly_digest.yml
      manual_run.yml
```

## Налаштування

1. Скопіюйте `config/settings.example.json` у `config/settings.json`.
2. Заповніть:
   - `dataset_zip_url`
   - `lookback_days`
   - `gemini_model`
   - `timezone`
3. У GitHub Secrets додайте:
   - `GEMINI_API_KEY`

## Локальний запуск

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python scripts/fetch_dataset.py
python scripts/extract_vp_postanovy.py
python scripts/fetch_decision_texts.py
python scripts/normalize_rtf.py
python scripts/analyze_with_gemini.py
python scripts/build_weekly_digest.py
```

## Вихідні файли

- `data/interim/vp_last7.csv` — добірка постанов ВП за період
- `data/processed/text/{doc_id}.txt` — нормалізовані тексти
- `data/processed/analysis/{doc_id}.json` — JSON-аналіз по кожній постанові
- `outputs/digest/weekly_digest.md` — тижневий markdown-дайджест
- `outputs/digest/telegram_post.txt` — один готовий Telegram-пост

## Примітки

- На старті автопостинг у Telegram не реалізований.
- State-файли зберігаються в `data/state/`.
- Скрипти написані так, щоб їх було легко доробити через Codex або вручну.
