"""
Pipeline cleaner for the compact product-level catalog.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from .entity_resolver import EntityResolver


class PipelineCleaner:
    """Handles product-level normalization for ranking and indexing."""

    FEMALE_KEYWORDS = ["women", "female", "girl", "women's", "жен"]
    MALE_KEYWORDS = ["men", "male", "boy", "mens", "man's", "men's", "муж"]
    UNISEX_KEYWORDS = ["unisex", "kids", "child", "youth", "унисекс"]

    def __init__(self, catalog_df: pd.DataFrame):
        self.catalog = catalog_df.copy()

    @classmethod
    def from_raw_sources(
        cls,
        shop_products_df: pd.DataFrame,
        categories_df: Optional[pd.DataFrame] = None,
    ) -> "PipelineCleaner":
        resolver = EntityResolver(
            shop_products=shop_products_df,
            categories=categories_df,
        )
        return cls(resolver.build_mapping())

    def clean_gender(self) -> "PipelineCleaner":
        def _detect_gender(row: pd.Series) -> str:
            raw_gender = str(row.get("gender", "")).strip().lower()
            if raw_gender in {"male", "female", "unisex"}:
                return raw_gender
            if raw_gender in {"муж", "мужской"}:
                return "male"
            if raw_gender in {"жен", "женский"}:
                return "female"
            if raw_gender in {"унисекс"}:
                return "unisex"

            text = f"{row.get('title', '')} {row.get('product_types', '')} {row.get('category_name', '')}".lower()
            if any(keyword in text for keyword in self.UNISEX_KEYWORDS):
                return "unisex"
            if any(keyword in text for keyword in self.FEMALE_KEYWORDS):
                return "female"
            if any(keyword in text for keyword in self.MALE_KEYWORDS):
                return "male"
            return "unknown"

        self.catalog["gender"] = self.catalog.apply(_detect_gender, axis=1)
        return self

    def extract_category(self) -> "PipelineCleaner":
        self.catalog["category_name"] = (
            self.catalog.get("category_name", pd.Series("", index=self.catalog.index))
            .fillna("")
            .astype(str)
            .str.strip()
            .replace("", "Uncategorized")
        )
        self.catalog["category_id"] = (
            self.catalog["category_name"]
            .str.lower()
            .str.replace(r"[^a-z0-9а-я]+", "_", regex=True)
            .str.strip("_")
        )
        return self

    def calculate_sale_flag(self) -> "PipelineCleaner":
        current_price = pd.to_numeric(self.catalog.get("price"), errors="coerce").fillna(0.0)
        old_price = pd.to_numeric(self.catalog.get("compare_at_price"), errors="coerce").fillna(0.0)

        self.catalog["is_sale"] = (old_price > current_price) & current_price.gt(0)
        self.catalog["discount"] = (
            ((old_price - current_price) / old_price)
            .where(old_price.gt(0) & current_price.gt(0) & old_price.gt(current_price), 0.0)
            .round(6)
        )
        return self

    def calculate_stock_status(self) -> "PipelineCleaner":
        stock_total = pd.to_numeric(self.catalog.get("stock_total"), errors="coerce").fillna(0.0)
        current_on_site = (
            self.catalog.get("current_on_site", pd.Series(False, index=self.catalog.index))
            .fillna(False)
            .astype(bool)
        )
        existing_in_stock = (
            self.catalog.get("in_stock", pd.Series(False, index=self.catalog.index))
            .fillna(False)
            .astype(bool)
        )
        self.catalog["in_stock"] = existing_in_stock | stock_total.gt(0) | current_on_site
        return self

    def initialize_temporal_fields(self) -> "PipelineCleaner":
        if "created_at" not in self.catalog.columns:
            self.catalog["created_at"] = ""
        else:
            created = pd.to_datetime(self.catalog["created_at"], errors="coerce")
            self.catalog["created_at"] = created.dt.strftime("%Y-%m-%d").fillna("")
        if "is_new" not in self.catalog.columns:
            self.catalog["is_new"] = False
        return self

    def filter_catalog_candidates(self) -> "PipelineCleaner":
        if "current_on_site" not in self.catalog.columns and "stock_total" not in self.catalog.columns:
            return self

        visibility_mask = pd.Series(False, index=self.catalog.index)
        if "current_on_site" in self.catalog.columns:
            visibility_mask = visibility_mask | self.catalog["current_on_site"].fillna(False).astype(bool)
        if "stock_total" in self.catalog.columns:
            visibility_mask = visibility_mask | pd.to_numeric(
                self.catalog["stock_total"], errors="coerce"
            ).fillna(0).gt(0)

        retained = int(visibility_mask.sum())
        threshold = max(10, int(len(self.catalog) * 0.01))
        if retained >= threshold:
            self.catalog = self.catalog.loc[visibility_mask].reset_index(drop=True)
        return self

    def sanitize_dtypes(self) -> "PipelineCleaner":
        numeric_cols = [
            "product_id",
            "stock_total",
            "price",
            "compare_at_price",
            "discount",
            "popularity",
            "novelty",
            "final_score",
            "age_days",
            "views",
            "purchases",
            "boost",
        ]
        for col in numeric_cols:
            if col in self.catalog.columns:
                self.catalog[col] = pd.to_numeric(self.catalog[col], errors="coerce")

        if "product_id" in self.catalog.columns:
            self.catalog["product_id"] = self.catalog["product_id"].fillna(0).astype(int)

        bool_cols = ["is_new", "is_sale", "in_stock", "current_on_site"]
        for col in bool_cols:
            if col in self.catalog.columns:
                self.catalog[col] = self.catalog[col].fillna(False).astype(bool)

        string_cols = [
            "title",
            "vendor",
            "barcode",
            "variant_barcodes",
            "article",
            "gender",
            "product_types",
            "category_id",
            "category_name",
            "product_url",
            "created_at",
            "season_code",
        ]
        for col in string_cols:
            if col in self.catalog.columns:
                self.catalog[col] = self.catalog[col].fillna("").astype(str)

        return self

    def persist(self, path: str | Path) -> Path:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self.catalog.to_csv(output_path, index=False, encoding="utf-8-sig")
        return output_path

    def clean(self) -> pd.DataFrame:
        print("[Cleaner] Starting data cleaning pipeline...")

        self.clean_gender()
        print(f"  Gender normalized ({self.catalog['gender'].value_counts(dropna=False).to_dict()})")

        self.extract_category()
        print(f"  Categories extracted ({self.catalog['category_name'].nunique()} unique)")

        self.calculate_sale_flag()
        print(f"  Sale flag calculated ({int(self.catalog['is_sale'].sum())} products on sale)")

        self.calculate_stock_status()
        print(f"  Stock status calculated ({int(self.catalog['in_stock'].sum())} in stock)")

        self.initialize_temporal_fields()
        before_filter = len(self.catalog)
        self.filter_catalog_candidates()
        if len(self.catalog) != before_filter:
            print(f"  Active candidate filter kept {len(self.catalog)} of {before_filter} products")
        self.sanitize_dtypes()
        print("  Data types sanitized")

        print(f"[Cleaner] Cleaning complete: {len(self.catalog)} products processed\n")
        return self.catalog
