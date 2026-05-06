# Скоринг и стратегии

**Все формулы, стратегии, бусты и порядок расчёта `age_days` подробно:** [PIPELINE_AND_SCORING_REFERENCE.md](PIPELINE_AND_SCORING_REFERENCE.md).

## Итоговая формула

```text
final_score = log1p(popularity) × (novelty × NOVELTY_WEIGHT / 14) × boost
```

- При `popularity ≤ 0` и `novelty == 0` → `final_score = 0`.
- Делитель **14** — константа в `FeatureEngine` (`novelty_divisor`).

## Популярность

```text
popularity = base_score × exp(−λ × age_days)
```

- **λ** задаётся через **half-life**: `λ = ln(2) / POPULARITY_HALF_LIFE`, по умолчанию **30** дней (`POPULARITY_HALF_LIFE`).
- **`age_days`** — см. расчёт в `FeatureEngine.calculate_age_days` (приоритет: `created_at` → `season_code` → эвристики → дефолт 180).

### Стратегии (`PIPELINE_SCORE_STRATEGY`)

| Код | Смысл `base_score` (упрощённо) |
|-----|--------------------------------|
| `baseline` | `0.3 × views + 0.7 × purchases` (линейные просмотры). |
| `funnel` | `0.2 × log1p(views) + 0.3 × net_cart + 0.5 × purchases`. |
| `robust` | Веса на уникальных зрителях / корзине / покупателях + доля покупок. |
| `commercial` | Блок из покупок, штук, log1p(revenue) + небольшой вес log-просмотров. |

Детальная шпаргалка по веткам — в коде `calculate_popularity_from_signals`.

## Новизна

```text
novelty = −log2((purchases + 1) / (total_purchases + 1))
```

`total_purchases` — сумма покупок по каталогу **в том же окне событий**. Если `total_purchases ≤ 0` → `novelty = 1`.

Новизна **не зависит от кода стратегии** (одинаковая для всех стратегий при тех же событиях).

## Буст (мультипликативно)

Значения по умолчанию задаются через env (см. `FeatureEngine.__init__):

| Множитель | Env | Дефолт |
|-----------|-----|--------|
| В наличии | `BOOST_IN_STOCK` | 1.5 |
| Нет в наличии | `BOOST_OUT_OF_STOCK` | 0.05 |
| Ручное «featured» | `BOOST_FEATURED` | 1.3 (нужны колонки `is_featured` / `featured`) |
| Акция | `BOOST_SALE` | 1.2 (`is_sale` или старая цена > текущей) |
| Категория | `BOOST_CATEGORY_MAP` | JSON `{"куртки": 1.1, ...}` по нижнему регистру `category_name`; если не задан — **×1** |

Дополнительно: при `PIPELINE_ENABLE_SEASONALITY` — множитель по `season_code` и календарному сезону.

## Окно событий

Не путать с **half-life** популярности:

- Окно — **какие дни событий суммируются** (`PIPELINE_SCORE_WINDOW_*`).
- Half-life — **как быстро стареет карточка** в формуле `exp(−λ × age_days)`.

## Страницы витрин (фильтры API)

После индексации отбор «Новинки / Распродажа / …» — это **фильтры Meilisearch** по полям `is_new`, `is_sale`, `gender`, `category_name`, а не отдельные индексы.

## Воспроизводимость между разработчиками

Одинаковые сырые файлы недостаточны: совпадают ещё **коммит**, **env**, **лимиты** и **какой CSV** ушёл в индекс. Для сравнения лучше зафиксировать общий `scored_catalog_*.csv` или общий staging-индекс. См. [PIPELINE.md](PIPELINE.md).
