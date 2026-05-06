# Kixbox: пайплайн ранжирования и Meilisearch

Прототип ETL и скоринга для витрин: выгрузка каталога (InSales) + события Mindbox → product-level каталог → метрики → индекс **Meilisearch** → **FastAPI**.

## Канонический справочник для команды

**Один файл — максимально подробно: весь пайплайн обработки данных и все формулы скоринга** (точка синхронизации при объединении решений):

→ **[docs/PIPELINE_AND_SCORING_REFERENCE.md](docs/PIPELINE_AND_SCORING_REFERENCE.md)**

Если текст и код расходятся, верьте коду в `pipeline/` и `src/` и обновите этот документ в том же изменении.

## Документация (краткие оглавления)

| Документ | Содержание |
|----------|------------|
| **[docs/PIPELINE_AND_SCORING_REFERENCE.md](docs/PIPELINE_AND_SCORING_REFERENCE.md)** | **Полный** пайплайн + скоринг. |
| [docs/PIPELINE.md](docs/PIPELINE.md) | Запуск, переменные окружения, артефакты. |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Поток данных, матрица модулей, Meilisearch, **таблица API и фильтров**. |
| [docs/SCORING.md](docs/SCORING.md) | Цепочка score, таблица стратегий, бусты, **окно vs half-life** (деталь — в справочнике). |
| [docs/DATA.md](docs/DATA.md) | Где лежат сырые данные и почему они не в Git. |

Правила для ассистентов при правках кода: [AGENTS.md](AGENTS.md).

## Быстрый старт

```powershell
pip install -r requirements.txt
docker compose up -d meilisearch
```

Положите выгрузки в каталог **`Personalisation LAB 26/`** (см. [Personalisation LAB 26/README.md](Personalisation%20LAB%2026/README.md)).

Полный прогон (снять лимиты на строки):

```powershell
$env:PIPELINE_SOURCE_ROW_LIMIT = '999999'
$env:PIPELINE_EVENT_ROW_LIMIT = '999999'
$env:PIPELINE_EVENT_FILE_LIMIT = '999'
python -u src/run_pipeline.py
```

Проверка без записи в Meilisearch:

```powershell
$env:PIPELINE_SKIP_INDEX = '1'
python -u src/check_pipeline.py
```

API (локально):

```powershell
uvicorn api_service:app --app-dir src --host 0.0.0.0 --port 8000
```

Индексация всех стратегий из `artifacts/scored_catalog_*.csv`:

```powershell
python -u src/index_all_strategy_indexes.py
```

## Структура репозитория

| Путь | Назначение |
|------|------------|
| `pipeline/` | Оркестратор, загрузка, cleaner, feature engine, индексатор. |
| `src/` | CLI, API, анализ стратегий. |
| `docs/` | Актуальная документация. |
| `artifacts/` | Выход пайплайна (в Git не коммитится, только `.gitkeep`). |
| `docker-compose.yml` | Meilisearch + опционально API. |