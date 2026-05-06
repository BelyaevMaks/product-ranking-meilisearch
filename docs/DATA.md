# Данные и конфиденциальность

Пайплайн и скоринг целиком: [PIPELINE_AND_SCORING_REFERENCE.md](PIPELINE_AND_SCORING_REFERENCE.md).

## Где лежат сырые файлы

Каталог по умолчанию: **`Personalisation LAB 26/`** в корне репозитория (см. `PipelineConfig.from_project_root`).

## Что не попадает в Git

- Вся папка **`Personalisation LAB 26/`** (кроме `.gitkeep` и этого README в подпапке) — в `.gitignore`.
- **`artifacts/`** после прогона — в `.gitignore`.
- **`meilisearch_data/`** — том Docker.

Так репозиторий можно публиковать на GitHub без выгрузок магазина и логов клиентов.

## Как завести проект у себя

1. Склонируйте репозиторий.
2. Скопируйте выгрузки в `Personalisation LAB 26/` согласно [README в этой папке](../Personalisation%20LAB%2026/README.md).
3. При смене имени файла каталога обновите `shop_catalog_path` в [`pipeline/config.py`](../pipeline/config.py).

## Ключ Meilisearch

Не храните продакшен-ключ в коде. Задайте `MEILI_MASTER_KEY` / `MEILISEARCH_MASTER_KEY` через переменные окружения или `.env` (файл `.env` в `.gitignore`).
