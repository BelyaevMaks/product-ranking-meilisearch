# Пайплайн: запуск, данные, окружение

## Назначение

ETL от выгрузки InSales/Mindbox до индекса Meilisearch и API ранжирования: аудит → очистка → агрегаты событий → скоринг → индексация.

## Структура кода

| Путь | Роль |
|------|------|
| `pipeline/orchestrator.py` | Полный прогон (этапы 0–4). |
| `pipeline/config.py` | Пути к сырым файлам и артефактам. |
| `pipeline/loader/` | Чтение TSV/JSON. |
| `pipeline/cleaner/` | Product-level каталог (`EntityResolver`). |
| `pipeline/feature_engine/engine.py` | Окно событий, формулы score. |
| `pipeline/search_indexer/` | Документы Meilisearch. |
| `src/run_pipeline.py` | Точка входа: вызывает `orchestrator.main`. |
| `src/check_pipeline.py` | Smoke-тест, опционально без индекса. |
| `src/api_service.py` | FastAPI + запросы к Meilisearch. |
| `src/index_all_strategy_indexes.py` | Индексация `scored_catalog_{strategy}.csv` по стратегиям. |
| `src/analyze_score_variants.py` | Сравнение стратегий, артефакты в `artifacts/`. |

Подробнее про поток данных — [ARCHITECTURE.md](ARCHITECTURE.md).

## Сырые данные

См. [DATA.md](DATA.md) и `Personalisation LAB 26/README.md`. Имена файлов по умолчанию заданы в `pipeline/config.py` (каталог с датой в имени).

## Запуск Meilisearch

```powershell
docker compose up -d meilisearch
```

Переменная `MEILI_MASTER_KEY` (и для клиента `MEILISEARCH_MASTER_KEY`) должна совпадать с ключом в контейнере.

## Полный прогон с полным объёмом

```powershell
$env:PIPELINE_SOURCE_ROW_LIMIT = '999999'
$env:PIPELINE_EVENT_ROW_LIMIT = '999999'
$env:PIPELINE_EVENT_FILE_LIMIT = '999'
python -u src/run_pipeline.py
```

## Без индексации (только CSV)

```powershell
$env:PIPELINE_SKIP_INDEX = '1'
python -u src/check_pipeline.py
```

## Основные переменные окружения

| Переменная | Назначение |
|------------|------------|
| `PIPELINE_BASE_PATH` | Корень проекта (по умолчанию — родитель `pipeline/`). |
| `PIPELINE_AS_OF_DATE` | Дата «среза» (ISO), верхняя граница событий и расчёта `age_days`. |
| `PIPELINE_SCORE_WINDOW_DAYS` | Окно агрегации событий: последние N дней до `as_of`. |
| `PIPELINE_SCORE_WINDOW_START` / `PIPELINE_SCORE_WINDOW_END` | Явный диапазон дат (приоритетнее `DAYS`). |
| `PIPELINE_SOURCE_ROW_LIMIT` | Лимит строк каталога (по умолчанию 1000 — только для dev). |
| `PIPELINE_EVENT_ROW_LIMIT`, `PIPELINE_EVENT_FILE_LIMIT` | Лимиты чтения JSON Mindbox. |
| `PIPELINE_SKIP_INDEX` | `1` — не писать в Meilisearch. |
| `PIPELINE_SCORE_STRATEGY` | `baseline` / `funnel` / `robust` / `commercial`. |
| `PIPELINE_ENABLE_SEASONALITY` | Сезонный множитель в бусте. |
| `MEILISEARCH_URL`, `MEILISEARCH_MASTER_KEY` | Клиент индексации и API. |
| `MEILISEARCH_INDEX_BASE`, `MEILISEARCH_INDEX_OVERRIDE` | Имена индексов (мультистратегии). |

Бусты и half-life популярности — в [SCORING.md](SCORING.md).

## Артефакты (`artifacts/`)

После прогона (локально, не в Git):

- `raw_audit_report.json` — аудит сырья.
- `cleaned_catalog.csv` — product-level каталог.
- `event_product_daily.csv` — дневные агрегаты по `product_id`.
- `scored_catalog.csv` — скоры для стратегии текущего прогона.
- `pipeline_run_metadata.json` — параметры последнего запуска.

Для нескольких стратегий — `scored_catalog_{strategy}.csv`, сводки сравнения — `score_strategy_comparison.csv` и др. (после `analyze_score_variants`).

## Принцип «без утечки из будущего»

При расчёте на дату T используются только события с `event_date ≤ T` и окно, заданное переменными выше. См. `FeatureEngine.resolve_event_window` и агрегацию в `aggregate_events`.
