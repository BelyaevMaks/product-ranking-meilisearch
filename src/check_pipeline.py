"""
Lightweight pipeline smoke check for the current shop-data-first flow.
"""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.audit import PipelineAudit
from pipeline.cleaner.cleaner import PipelineCleaner
from pipeline.config import PipelineConfig
from pipeline.feature_engine.engine import FeatureEngine
from pipeline.loader.event_stream import EventStreamBuilder
from pipeline.loader.raw_data_loader import RawDataLoader


def _env_int(name: str, default: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def _env_optional_int(name: str):
    raw_value = os.environ.get(name)
    if raw_value is None or not raw_value.strip():
        return None
    try:
        value = int(raw_value)
    except ValueError:
        return None
    return value if value > 0 else None


def _env_optional_date(name: str):
    raw_value = os.environ.get(name)
    if raw_value is None or not raw_value.strip():
        return None
    try:
        return date.fromisoformat(raw_value.strip())
    except ValueError:
        return None


def _build_variant_to_product_lookup(shop_df):
    mapping = {}
    for _, row in shop_df.iterrows():
        barcode = str(row.get("Штрих-код", "")).strip()
        product_id = row.get("ID товара")
        if not barcode or not str(product_id).strip():
            continue
        try:
            mapping[barcode] = int(float(str(product_id).replace(",", ".")))
        except ValueError:
            continue
    return mapping


def main() -> int:
    shop_rows = _env_int("CHECK_SHOP_ROWS", 1000)
    audit_rows = _env_int("CHECK_AUDIT_ROWS", 1000)
    score_window_days = _env_optional_int("PIPELINE_SCORE_WINDOW_DAYS")
    score_window_start = _env_optional_date("PIPELINE_SCORE_WINDOW_START")
    score_window_end = _env_optional_date("PIPELINE_SCORE_WINDOW_END")
    score_strategy = (os.environ.get("PIPELINE_SCORE_STRATEGY") or "baseline").strip().lower() or "baseline"
    enable_seasonality = (os.environ.get("PIPELINE_ENABLE_SEASONALITY") or "").strip().lower() in {"1", "true", "yes", "y"}

    config = PipelineConfig.from_project_root(PROJECT_ROOT)
    loader = RawDataLoader(config)
    loader.validate_sources()

    print("[Check] Running lightweight raw audit...")
    audit = PipelineAudit(config)
    report = audit.run(
        source_row_limit=audit_rows,
        max_event_files=1,
        max_event_rows_per_file=1000,
        intent_sample_files=1,
        intent_sample_rows_per_file=1000,
    )
    print(f"  Derived as_of_date: {report.get('derived_as_of_date')}")
    print(f"  Event rows scanned: {report.get('events', {}).get('rows_scanned', 0)}")
    print(f"  Event intents: {report.get('events', {}).get('intent_sample', {}).get('intent_counts', {})}")

    print("[Check] Loading bounded shop sample...")
    shop_df = loader.load_shop_products(minimal=True, nrows=shop_rows)
    categories_df = loader.load_categories()
    print(f"  Shop rows: {len(shop_df)}")
    print(f"  Categories rows: {len(categories_df)}")

    print("[Check] Resolving and cleaning sample catalog...")
    cleaner = PipelineCleaner.from_raw_sources(
        shop_products_df=shop_df,
        categories_df=categories_df,
    )
    cleaned_df = cleaner.clean()
    print(f"  Cleaned products: {len(cleaned_df)}")
    print(f"  In stock: {int(cleaned_df['in_stock'].sum())}")
    print(f"  On sale: {int(cleaned_df['is_sale'].sum())}")

    print("[Check] Aggregating sample events...")
    event_builder = EventStreamBuilder(loader)
    variant_lookup = _build_variant_to_product_lookup(shop_df)
    daily_events = event_builder.aggregate_daily_products(
        variant_lookup,
        max_files=1,
        max_rows_per_file=1000,
        as_of_date=config.as_of_date.isoformat() if config.as_of_date else None,
    )
    print(f"  Product daily rows: {len(daily_events)}")

    print("[Check] Scoring sample catalog...")
    engine = FeatureEngine(
        catalog_df=cleaned_df,
        events_df=daily_events,
        as_of_date=config.as_of_date,
        window_days=score_window_days,
        window_start=score_window_start,
        window_end=score_window_end,
        strategy=score_strategy,
        enable_seasonality=enable_seasonality,
    )
    scored_df = engine.compute_scores()
    required_columns = {
        "product_id",
        "title",
        "category_id",
        "category_name",
        "price",
        "discount",
        "in_stock",
        "is_new",
        "is_sale",
        "gender",
        "popularity",
        "novelty",
        "final_score",
    }
    missing_columns = sorted(required_columns - set(scored_df.columns))
    if missing_columns:
        print(f"[Check] Missing required columns: {missing_columns}")
        return 1

    print("[Check] Smoke check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
