"""
Reproducible audit over the current catalog snapshot and JSON event logs.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from .config import PipelineConfig
from .loader.event_intents import summarize_intents_from_templates
from .loader.event_stream import EventStreamBuilder
from .loader.raw_data_loader import RawDataLoader


class PipelineAudit:
    """Builds a structured audit report from raw inputs."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.loader = RawDataLoader(config)

    @staticmethod
    def _safe_int(value: Any) -> int:
        return int(value) if pd.notna(value) else 0

    @staticmethod
    def _to_numeric(series: pd.Series) -> pd.Series:
        normalized = (
            series.fillna("")
            .astype(str)
            .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)
            .replace("", pd.NA)
        )
        return pd.to_numeric(normalized, errors="coerce")

    def _shop_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        product_id_col = "ID товара"
        variant_id_col = "ID варианта"
        barcode_col = "Штрих-код"
        article_col = "Артикул"
        visibility_col = "Видимость на витрине"
        price_col = "Цена продажи"
        old_price_col = "Старая цена"
        brand_col = "Параметр: Бренд"
        season_col = "Параметр: Сезон"

        price_series = self._to_numeric(df.get(price_col, pd.Series(dtype=object)))
        old_price_series = self._to_numeric(df.get(old_price_col, pd.Series(dtype=object)))
        product_counts = df.groupby(product_id_col).size() if product_id_col in df.columns else pd.Series(dtype=int)

        return {
            "rows": int(len(df)),
            "unique_products": self._safe_int(df[product_id_col].nunique()) if product_id_col in df.columns else 0,
            "unique_variants": self._safe_int(df[variant_id_col].nunique()) if variant_id_col in df.columns else 0,
            "unique_barcodes": self._safe_int(df[barcode_col].nunique()) if barcode_col in df.columns else 0,
            "unique_articles": self._safe_int(df[article_col].dropna().nunique()) if article_col in df.columns else 0,
            "missing_price_rows": self._safe_int(price_series.isna().sum()),
            "zero_price_rows": self._safe_int((price_series.fillna(0) <= 0).sum()),
            "sale_rows": self._safe_int((old_price_series > price_series.fillna(0)).sum()),
            "visible_rows": self._safe_int(
                df.get(visibility_col, pd.Series(dtype=object))
                .fillna("")
                .astype(str)
                .str.lower()
                .eq("выставлен")
                .sum()
            ),
            "products_with_multiple_variants": self._safe_int((product_counts > 1).sum()),
            "max_variants_per_product": self._safe_int(product_counts.max()) if not product_counts.empty else 0,
            "top_brands": (
                {str(k): int(v) for k, v in df.get(brand_col, pd.Series(dtype=object)).value_counts(dropna=False).head(15).items()}
                if brand_col in df.columns
                else {}
            ),
            "top_seasons": (
                {str(k): int(v) for k, v in df.get(season_col, pd.Series(dtype=object)).value_counts(dropna=False).head(15).items()}
                if season_col in df.columns
                else {}
            ),
        }

    def _categories_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        category_name_col = "CategoryName"
        category_id_col = "CategoryIdsInsalesId"

        return {
            "rows": int(len(df)),
            "unique_category_names": self._safe_int(df[category_name_col].dropna().nunique()) if category_name_col in df.columns else 0,
            "unique_insales_category_ids": self._safe_int(df[category_id_col].dropna().nunique()) if category_id_col in df.columns else 0,
        }

    def run(
        self,
        *,
        source_row_limit: Optional[int] = 100_000,
        max_event_files: Optional[int] = None,
        max_event_rows_per_file: Optional[int] = None,
        intent_sample_files: int = 2,
        intent_sample_rows_per_file: int = 2000,
    ) -> Dict[str, Any]:
        bundle = self.loader.load_bundle_selective(
            include_categories=True,
            row_limit=source_row_limit,
        )
        event_scan = self.loader.scan_event_archive(
            max_files=max_event_files,
            max_rows_per_file=max_event_rows_per_file,
        )

        report = {
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "audit_mode": {
                "source_row_limit": source_row_limit,
            },
            "paths": {
                "raw_data_dir": str(self.config.raw_data_dir),
                "shop_catalog_path": str(self.config.shop_catalog_path),
                "categories_path": str(self.config.categories_path),
                "customer_actions_json_dir": str(self.config.customer_actions_json_dir),
            },
            "shop_catalog": self._shop_summary(bundle.shop_products),
            "categories": self._categories_summary(bundle.categories),
            "events": asdict(event_scan),
        }

        template_counts = report["events"].get("action_template_counts_top20", {})
        report["events"]["intent_summary_top_templates"] = summarize_intents_from_templates(template_counts)
        report["events"]["has_product_link_fields"] = bool(report["events"].get("product_related_paths"))

        stream_builder = EventStreamBuilder(self.loader)
        intent_frame = stream_builder.to_frame(
            max_files=intent_sample_files,
            max_rows_per_file=intent_sample_rows_per_file,
        )
        if intent_frame.empty:
            report["events"]["intent_sample"] = {
                "rows": 0,
                "intent_counts": {},
                "date_range": None,
            }
        else:
            valid_times = intent_frame["event_time_utc"].dropna()
            report["events"]["intent_sample"] = {
                "rows": int(len(intent_frame)),
                "intent_counts": {
                    str(k): int(v)
                    for k, v in intent_frame["intent"].value_counts(dropna=False).items()
                },
                "date_range": {
                    "min": valid_times.min().isoformat() if not valid_times.empty else None,
                    "max": valid_times.max().isoformat() if not valid_times.empty else None,
                },
            }

        last_event_str = event_scan.last_event_timestamp
        if last_event_str:
            last_event_dt = datetime.fromisoformat(last_event_str)
            report["derived_as_of_date"] = self.config.resolve_as_of_date(last_event_dt).isoformat()
        else:
            report["derived_as_of_date"] = None

        return report

    def save_report(self, report: Dict[str, Any], path: Optional[str | Path] = None) -> Path:
        output_path = Path(path) if path else self.config.audit_report_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return output_path
