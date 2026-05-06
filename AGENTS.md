# Инструкция по реализации проекта: Ranking Pipeline & Meilisearch Integration

## Роль Агента
Ты — Senior Data Engineer / ML Engineer. Твоя задача: построить прототип системы ранжирования товаров на основе сырых выгрузок (Shopify/Website + Mindbox).

## Основные принципы
1. **Zero Future Leakage**: При расчете признаков на дату T запрещено использовать любые данные из "будущего" (события > T).
2. **Entity Resolution**: Главная сложность — корректно связать ID товаров сайта с ID Mindbox.
3. **Modular Code**: Весь пайплайн должен быть разбит на логические блоки: Loader -> Cleaner -> FeatureEngine -> SearchIndexer.

## Этапы реализации
1. **Data Audit**: Анализ пропусков, дублей и конфликтов в ID.
2. **Schema & Logic**: Построение схемы связей и логики временного окна.
3. **Scoring Engine**: Реализация формул популярности, новизны и бустинга.
4. **Indexing**: Подготовка JSON и конфигурация Meilisearch.
5. **Validation**: Выдача для страниц "Новинки", "Распродажа", "Мужское", "Куртки".

## Формулы ранжирования
Единый подробный справочник: [docs/PIPELINE_AND_SCORING_REFERENCE.md](docs/PIPELINE_AND_SCORING_REFERENCE.md). Кратко (baseline):
- **Popularity**: `(views * 0.3 + purchases * 0.7) * exp(-λ * age_days)` (λ от half-life)
- **Novelty**: `-log2((purchases + 1) / (total_purchases + 1))`
- **Final Score**: `log1p(popularity) * (novelty * NOVELTY_WEIGHT / 14) * boost`