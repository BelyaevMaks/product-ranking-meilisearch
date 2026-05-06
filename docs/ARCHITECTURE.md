# Архитектура

## Поток данных

```text
shop_data (варианты, TSV)
  -> EntityResolver -> product-level каталог (cleaned)

customers-actions-new/*.json
  -> EventStreamBuilder -> дневные ряды по product_id

каталог + дневные события (окно по датам)
  -> FeatureEngine -> popularity, novelty, boost, final_score

scored DataFrame / CSV
  -> SearchIndexer -> Meilisearch

Meilisearch
  -> api_service (FastAPI) -> витрины и поиск
```

## Зачем два уровня (вариант → товар)

- Каталог InSales — **строки вариантов** (размер, свой штрих-код).
- Mindbox в событиях отдаёт **insalesId** варианта, а не внутренний `ID товара` напрямую.

Поэтому строится отображение **штрих-код / insalesId → product_id**, затем все метрики считаются на уровне **одной карточки товара**.

## Ключевые модули

- **`pipeline/cleaner/entity_resolver.py`** — нормализация полей TSV, группировка по `ID товара`, поля `barcode`, `variant_barcodes`, `category_name` из «Размещение на сайте».
- **`pipeline/loader/event_stream.py`** — разбор JSON, классификация интентов, `aggregate_daily_products`.
- **`pipeline/feature_engine/engine.py`** — окно дат, смешивание сигналов по стратегии, `age_days`, итоговый score.
- **`pipeline/search_indexer/indexer.py`** — плоский документ для Meilisearch (без массивов размеров/картинок в текущей версии).

## Meilisearch

- Один документ = один `product_id`.
- Сортировка по умолчанию в правилах ранжирования включает `final_score:desc`.
- Несколько индексов: `{MEILISEARCH_INDEX_BASE}_{strategy}` (см. `pipeline/meilisearch_strategy_indexes.py`).

## API и «страницы»

Фильтры витрин задаются в `src/api_service.py` (`_page_filter_clauses`): новинки (`is_new`), распродажа (`is_sale`), мужское (`gender = male`), куртки (список `category_name`). К индексу добавляются общие условия `in_stock` и `current_on_site` для `get_page`.

## Справочник categories.csv

Файл читается в бандле загрузки, но **`EntityResolver` не джойнит его** к товару: `category_name` берётся из путей размещения на сайте. Отдельная агрегация событий по категориям из справочника в основном пайплайне не используется.
