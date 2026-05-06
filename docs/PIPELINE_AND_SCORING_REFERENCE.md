# Полный справочник: пайплайн данных и скоринг

**Этот файл — единая точка синхронизации команды.**  
Здесь собрано максимально подробное описание этапов обработки данных и всех формул ранжирования в том виде, как они реализованы в коде на момент последнего обновления документа. При расхождениях с кодом приоритет у **`pipeline/`** и **`src/`**; после правок кода этот документ нужно обновлять.

**Исходники правды в коде:**

- Пайплайн: `pipeline/orchestrator.py`, `pipeline/config.py`, `pipeline/cleaner/`, `pipeline/loader/`, `pipeline/search_indexer/`
- Скоринг: `pipeline/feature_engine/engine.py`
- API / фильтры витрин: `src/api_service.py`

---

## Часть A. Входные данные и конфигурация

### A.1. Пути по умолчанию (`pipeline/config.py`)

Корень проекта + подпапка **`Personalisation LAB 26/`** (`raw_data_dir`):

| Файл | Назначение |
|------|------------|
| `shop_data-10.04.2026 2.csv` | Каталог InSales на уровне **вариантов** (размеры и т.д.). Имя файла зашито в `shop_catalog_path`; при новой выгрузке обновите свойство или конфиг. |
| `categories.csv` | Справочник категорий: **загружается**, но **не используется** для вычисления `category_name` товара в `EntityResolver` (категория берётся из размещения на сайте). |
| `customers-actions-new/*.json` | События Mindbox (просмотры, корзина, покупки). |

Артефакты пишутся в **`artifacts/`** (в Git не коммитятся по `.gitignore`).

### A.2. Связь событий с товаром (entity resolution)

- В JSON событий товар часто идёт как **`insalesId`** варианта.
- В каталоге есть **`Штрих-код`** и **`ID товара`** на уровне варианта.
- В оркестраторе строится отображение **штрих-код → `product_id`** (и при агрегации событий — **insalesId → product_id** через тот же словарь в `EventStreamBuilder`).

Если штрих-код пустой или не совпал, событие не попадёт в агрегат по товару.

### A.3. Принцип «без утечки из будущего» (zero future leakage)

- Задаётся **`as_of_date`** (`PIPELINE_AS_OF_DATE` или дата из конфига / сегодня).
- Окно событий всегда обрезается: **`event_date ≤ min(window_end, as_of_date)`** (см. `FeatureEngine.resolve_event_window` и фильтр в `aggregate_events`).
- События строго после среза в расчёт не попадают.

---

## Часть B. Этапы пайплайна (orchestrator)

### Этап 0. Аудит сырья

- Модуль: `pipeline/audit.py`
- Результат: `artifacts/raw_audit_report.json`
- Цель: проверить наличие файлов, оценить объёмы, вывести производные даты.

### Этап 1. Загрузка

- Модуль: `pipeline/loader/raw_data_loader.py`
- Читает каталог и (опционально) категории; путь к JSON — `customer_actions_json_dir`.
- **Лимиты dev:** `PIPELINE_SOURCE_ROW_LIMIT`, `PIPELINE_EVENT_ROW_LIMIT`, `PIPELINE_EVENT_FILE_LIMIT` — при маленьких значениях результат **не сопоставим** с полным прогоном.

### Этап 2. Очистка и product-level каталог

- Модуль: `pipeline/cleaner/entity_resolver.py`, `pipeline/cleaner/cleaner.py`
- Вариантные строки **группируются по `ID товара`** (`product_id`).
- Ключевые поля карточки: название, бренд, артикул, URL, цена/старая цена, суммарный остаток, видимость на витрине, сезон, типы, **категория из путей «Размещение на сайте»**, флаги `is_new` (из параметра новинки), штрих-коды: **`barcode`** (первый уникальный) и **`variant_barcodes`** (все уникальные через ` | `).
- **`filter_catalog_candidates`:** если доля строк с `current_on_site` или `stock_total > 0` достаточна (≥ max(10, 1% каталога)), остаются только такие товары; иначе фильтр не применяется (чтобы не обнулить каталог).
- Результат: `artifacts/cleaned_catalog.csv`

### Этап 3. События → дневные агрегаты

- Модуль: `pipeline/loader/event_stream.py` (`aggregate_daily_products`)
- Одна строка: **`event_date`, `product_id`**, суммы `views`, `cart_adds`, `cart_removes`, `purchases`, `units_purchased`, `revenue`, уникальные клиенты по ролям.
- Результат: `artifacts/event_product_daily.csv`
- В скоринг в `FeatureEngine` передаётся этот датафрейм (или пустой → см. fallback ниже).

### Этап 4. Скоринг

- Модуль: `pipeline/feature_engine/engine.py`
- На входе: product-level каталог + таблица дневных агрегатов.
- На выходе: те же строки + `views`, `purchases`, …, `age_days`, `popularity`, `novelty`, `boost`, `final_score`; опционально пересчёт `is_new` от `age_days` (порог **365** дней).
- Результат: `artifacts/scored_catalog.csv` (имя может совпадать со стратегией в других сценариях).

### Этап 5. Индексация Meilisearch

- Модуль: `pipeline/search_indexer/indexer.py`
- Документ = одна строка каталога после скоринга; поля задаются в `prepare_documents` (см. часть F).
- Имя индекса: `MEILISEARCH_INDEX_NAME` / `{base}_{strategy}` и т.д. (`pipeline/meilisearch_strategy_indexes.py`).

---

## Часть C. Агрегация событий внутри FeatureEngine

### C.1. Окно дат

Приоритет в `resolve_event_window`:

1. Если заданы **`window_start` и/или `window_end`** — используется явный диапазон (конец не выше `as_of_date`).
2. Иначе если задан **`window_days`** — последние N календарных дней до `end_date` (включительно): `start = end - (N - 1) дней`.
3. Иначе — **вся история** до `end_date` (`start = None`).

Переменные окружения оркестратора: `PIPELINE_SCORE_WINDOW_DAYS`, `PIPELINE_SCORE_WINDOW_START`, `PIPELINE_SCORE_WINDOW_END`.

### C.2. Фильтрация дневных строк

Для таблицы с колонками `event_date`, `product_id`, метриками:

- Строки с пустой датой отбрасываются.
- Применяется **`event_date <= window_end`** и **`event_date >= window_start`** (если границы заданы).

### C.3. Суммирование по товару

По каждому **`product_id`** суммируются столбцы:

`views`, `cart_adds`, `cart_removes`, `purchases`, `units_purchased`, `revenue`, `unique_customers`, `unique_viewers`, `unique_cart_customers`, `unique_buyers`.

Далее эти ряды **мапятся** в строки каталога; отсутствующий `product_id` → нули.

### C.4. Fallback без событий

Если `events` пустой или не передан:

- `views` / `purchases` считаются **эвристикой из каталога** (наличие, скидка и т.д.) — см. `aggregate_events`; остальные счётчики нули.
- Это режим для отладки, **не** целевой продакшен-режим при наличии Mindbox.

---

## Часть D. Возраст карточки `age_days`

Порядок в `calculate_age_days`:

1. Если есть **`created_at`** и хотя бы одна дата парсится:  
   **`age_days = max(0, as_of_date - created_at)`** в днях; пропуски → **180**.
2. Иначе если есть **`season_code`** формата `(SS|AW|HO|FW)-(\d{2})`: дата **1-го числа** месяца сезона (SS→3, AW→8, FW→9, HO→10); возраст до `as_of_date`; неподходящий код → **180** для строки.
3. Иначе если есть **`shopify_handle`**: эвристика разбора года/месяца из slug; ошибка → **180**.
4. Иначе если в каталоге уже есть **`age_days`** — берётся из колонки.
5. Иначе константа **180** для всех.

**`created_at` в каталоге** обычно выводится из сезона в `EntityResolver` (`_season_to_created_at`), а не из даты создания в Insales.

### D.1. Флаг новинки после скоринга

`update_new_flag`: **`is_new = (age_days < 365)`** (если колонка `age_days` есть).

---

## Часть E. Формулы скоринга

Обозначения в строке товара после мержа событий:

- `views`, `purchases`, `cart_adds`, `cart_removes`, `units_purchased`, `revenue`, `unique_*` — за **выбранное окно**.
- `age_days` — см. часть D.
- **`total_purchases`** (для новизны): **сумма `purchases` по всем товарам** в агрегированной таблице событий после окна (целое; если ряд пуст при расчёте, в коде подставляется **1** для ветки суммирования — см. `compute_scores`; при нулевой сумме новизна обрабатывается в `calculate_novelty`).

### E.1. Промежуточные сигналы

- **`views_signal`** = `max(views, 0)` если стратегия без log-просмотров; иначе **`ln(1 + max(views,0))`** (`np.log1p`).
- **`net_cart`** = `max(cart_adds - cart_removes, 0)`.

### E.2. Базовый балл `base_score` (до затухания)

Затем **`popularity = base_score * exp(-λ * age_days)`**, где:

**`λ = ln(2) / POPULARITY_HALF_LIFE`**, переменная окружения **`POPULARITY_HALF_LIFE`**, по умолчанию **30** дней.

| Стратегия (`PIPELINE_SCORE_STRATEGY`) | Формула `base_score` |
|---------------------------------------|----------------------|
| **baseline** | `views_signal * 0.3 + net_cart * 0 + purchases * 0.7` — то есть **`0.3 * views + 0.7 * purchases`** (линейные просмотры). |
| **funnel** | `0.2 * views_signal + 0.3 * net_cart + 0.5 * purchases` с `views_signal = log1p(views)`. |
| **robust** | `0.15 * unique_viewers + 0.30 * unique_cart_customers + 0.35 * unique_buyers + 0.20 * purchases` (лог-просмотры в base не входят). |
| **commercial** | `commercial_signal = 0.4*purchases + 0.2*units_purchased + 0.4*log1p(max(revenue,0))`; **`base_score = 0.15 * views_signal + commercial_signal`** с log-просмотрами. |

Веса заданы в **`SCORE_STRATEGIES`** в `engine.py`.

### E.3. Новизна `novelty`

- Если **`total_purchases <= 0`**: **`novelty = 1.0`**.
- Иначе: **`novelty = -log2((purchases + 1) / (total_purchases + 1))`** (логарифм по основанию 2).

Новизна **не зависит** от кода стратегии — только от распределения покупок в окне.

### E.4. Буст `boost` (все множители перемножаются)

Старт: **1.0**.

1. **Наличие:** если `in_stock` истина → **`× BOOST_IN_STOCK`** (env, по умолчанию **1.5**); иначе → **`× BOOST_OUT_OF_STOCK`** (по умолчанию **0.05**).  
   Булево поле: `true` / `1` / `yes` / `y` (регистр не важен); по умолчанию для отсутствующего значения в ряду — **true** (осторожно при данных).

2. **Featured:** если `is_featured` или `featured` истина → **`× BOOST_FEATURED`** (по умолчанию **1.3**). В типичном каталоге колонок может не быть → множитель не применяется.

3. **Акция:** если `is_sale` истина **или** `compare_at_price`/`old_price` > `price` → **`× BOOST_SALE`** (по умолчанию **1.2**).

4. **Категория:** если задан **`BOOST_CATEGORY_MAP`** (JSON-объект), ключ = **`category_name` в нижнем регистре**; при совпадении → **`×`** соответствующее число. Иначе **×1**.

5. **Сезонность:** если **`PIPELINE_ENABLE_SEASONALITY`** включена и `season_code` валиден: «сезон товара совпадает с календарной группой месяца `as_of_date`» → **×1.15**, иначе **×0.90**; при выключенной сезонности **×1**.

### E.5. Итоговый балл `final_score`

```text
final_score = log1p(popularity) * (novelty * NOVELTY_WEIGHT / 14) * boost
```

- **`NOVELTY_WEIGHT`** — env, по умолчанию **1.0**.
- Делитель **14** — константа **`novelty_divisor`** в коде.
- Если **`popularity <= 0`** и **`novelty == 0`** → **`final_score = 0`**.

`log1p` — натуральный логарифм.

---

## Часть F. Поля документа Meilisearch (индекс)

Формируются в `SearchIndexer.prepare_documents`:

`id`, `product_id`, `title`, `brand`, `vendor`, `article`, `barcode`, `product_url`, `category_id`, `category_name`, `product_types`, `gender`, `price`, `discount`, `in_stock`, `current_on_site`, `is_new`, `is_sale`, `created_at`, `age_days`, `views`, `purchases`, `popularity`, `novelty`, `boost`, `final_score`.

**Не индексируются** в текущей версии: массивы размеров/цветов/картинок, `variant_barcodes`, отдельные уровни категорий — только то, что перечислено выше.

---

## Часть G. Витрины в API (после индексации)

В `src/api_service.py` для **`get_page`** к фильтру страницы добавляются **`in_stock = true`** и **`current_on_site = true`**.

Дополнительно по **`page_tag`**:

| `page_tag` | Условие |
|------------|---------|
| `new_arrivals` | `is_new = true` |
| `sale` | `is_sale = true` |
| `men` | `gender = 'male'` |
| `jackets` | `category_name` ∈ {`Куртки`, `Куртка`, `Пуховики`, `Жилеты`} (OR) |

Эндпоинты **`/products/new`** и **`/products/sale`** задают фильтры явно в коде (см. `get_new_arrivals`, `get_sale_items`).

---

## Часть H. Синхронизация команды

1. Зафиксировать **коммит** и **полный список env** (можно скопировать вывод `Get-ChildItem Env:PIPELINE*`, `Env:BOOST*`, `Env:MEILISEARCH*`).
2. Для сравнения результатов использовать **один и тот же** `scored_catalog_*.csv` или один staging-индекс.
3. Любое изменение формул — правка **`engine.py`** + обновление **этого файла** в том же PR.

---

*Версия документа: синхронизируйте дату коммита при крупных изменениях скоринга.*
