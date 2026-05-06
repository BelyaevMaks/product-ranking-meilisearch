"""
Current catalog resolver built from `shop_data`.

The resolver converts variant rows into a compact product-level catalog.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional

import pandas as pd


def _safe_str(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "<na>"} else text


def _safe_float(value: object) -> float:
    text = _safe_str(value).replace(" ", "").replace(",", ".")
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _truthy(value: object) -> bool:
    text = _safe_str(value).lower()
    return text in {"true", "1", "yes", "да", "выставлен"}


def _unique_preserve(values: Iterable[object]) -> List[str]:
    result: List[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = _safe_str(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def _extract_article_from_description(value: object) -> str:
    text = _safe_str(value)
    if not text:
        return ""
    match = re.search(r"Артикул:\s*([^<\n\r]+)", text, flags=re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip().replace("&nbsp;", "").upper()


def _extract_paths(value: object) -> List[str]:
    raw = _safe_str(value)
    if not raw:
        return []
    return [part.strip() for part in raw.split("##") if part.strip()]


def _extract_leaf_categories(paths: Iterable[str]) -> List[str]:
    leafs: List[str] = []
    for path in paths:
        segments = [segment.strip() for segment in str(path).split("/") if segment.strip()]
        if segments:
            leafs.append(segments[-1])
    return _unique_preserve(leafs)


def _choose_primary_category(row: pd.Series, leaf_categories: List[str]) -> str:
    skip = {
        "Каталог",
        "Бренды",
        "Популярные",
        "Мужское",
        "Женское",
        "Новинки",
        "СКИДКИ",
        "Under 3000",
        "Under 5000",
        "Under 10000",
        "Under 20000",
        "Under 50000",
    }
    brand = _safe_str(row.get("Параметр: Бренд"))
    for category in leaf_categories:
        if category not in skip and category != brand:
            return category
    for key in ("Параметр: Тип2", "Параметр: Тип", "Параметр: Тип3"):
        value = _safe_str(row.get(key))
        if value:
            return value
    return leaf_categories[0] if leaf_categories else "Uncategorized"


def _season_to_created_at(season_code: str) -> str:
    match = re.fullmatch(r"(SS|AW|HO|FW|SU)-(\d{2})", _safe_str(season_code).upper())
    if not match:
        return ""
    season, year_suffix = match.groups()
    year = 2000 + int(year_suffix)
    month_map = {"SS": 3, "SU": 6, "AW": 8, "FW": 9, "HO": 10}
    month = month_map.get(season, 6)
    return f"{year:04d}-{month:02d}-01"


class EntityResolver:
    """Build a product-level catalog from the current `shop_data` snapshot."""

    def __init__(
        self,
        shop_products: pd.DataFrame,
        categories: Optional[pd.DataFrame] = None,
    ):
        self.shop_products = shop_products.copy()
        self.categories = categories.copy() if categories is not None else pd.DataFrame()

    def prepare_shop_products(self) -> pd.DataFrame:
        if self.shop_products.empty:
            return self.shop_products

        df = self.shop_products.copy()
        df["product_id"] = pd.to_numeric(df["ID товара"], errors="coerce").astype("Int64")
        df["variant_id"] = pd.to_numeric(df["ID варианта"], errors="coerce").astype("Int64")
        df["price_value"] = df["Цена продажи"].map(_safe_float)
        df["old_price_value"] = df["Старая цена"].map(_safe_float)
        df["stock_total_row"] = df["Остаток"].map(_safe_float)

        stock_cols = [col for col in df.columns if col.startswith("Остаток:")]
        for col in stock_cols:
            df[col] = df[col].map(_safe_float)
            df["stock_total_row"] = df["stock_total_row"] + df[col]

        df["article_raw"] = df["Артикул"].map(_safe_str)
        missing_article_mask = df["article_raw"].eq("")
        df.loc[missing_article_mask, "article_raw"] = df.loc[missing_article_mask, "Описание"].map(
            _extract_article_from_description
        )
        df["article"] = df["article_raw"].str.upper()
        df["barcode"] = df["Штрих-код"].map(_safe_str)
        df["product_url"] = df["URL"].map(_safe_str)
        df["title"] = df["Название товара или услуги"].map(_safe_str)
        df["vendor"] = df["Параметр: Бренд"].map(_safe_str)
        df["gender"] = df["Параметр: Пол"].map(_safe_str)
        df["product_type"] = df["Параметр: Тип"].map(_safe_str)
        df["product_type2"] = df["Параметр: Тип2"].map(_safe_str)
        df["product_type3"] = df["Параметр: Тип3"].map(_safe_str)
        df["season_code"] = df["Параметр: Сезон"].map(_safe_str).str.upper()
        df["created_at"] = df["season_code"].map(_season_to_created_at)
        df["current_on_site"] = df["Видимость на витрине"].map(_truthy)
        df["is_new_flag"] = df["Параметр: новинка"].map(_truthy)
        df["site_paths"] = df["Размещение на сайте"].map(_extract_paths)
        df["site_leaf_categories"] = df["site_paths"].map(_extract_leaf_categories)
        return df

    def build_mapping(self) -> pd.DataFrame:
        df = self.prepare_shop_products()
        if df.empty:
            return pd.DataFrame()

        grouped_rows = []
        for product_id, product_rows in df.groupby("product_id", dropna=True):
            first = product_rows.iloc[0]
            leaf_categories = _unique_preserve(
                category for categories in product_rows["site_leaf_categories"] for category in categories
            )
            primary_category = _choose_primary_category(first, leaf_categories)
            product_types = _unique_preserve(
                [first.get("product_type"), first.get("product_type2"), first.get("product_type3")]
            )
            barcodes = _unique_preserve(product_rows["barcode"].tolist())
            visible_on_site = bool(product_rows["current_on_site"].fillna(False).astype(bool).any())
            stock_total = float(product_rows["stock_total_row"].fillna(0).sum())
            in_stock = stock_total > 0
            price_candidates = product_rows["price_value"][product_rows["price_value"] > 0]
            old_price_candidates = product_rows["old_price_value"][product_rows["old_price_value"] > 0]
            price = float(price_candidates.min()) if not price_candidates.empty else 0.0
            compare_at_price = float(old_price_candidates.max()) if not old_price_candidates.empty else 0.0

            grouped_rows.append(
                {
                    "product_id": int(product_id),
                    "title": _safe_str(first.get("title")),
                    "product_url": _safe_str(first.get("product_url")),
                    "vendor": _safe_str(first.get("vendor")),
                    "gender": _safe_str(first.get("gender")),
                    "article": _safe_str(first.get("article")),
                    "barcode": barcodes[0] if barcodes else "",
                    "variant_barcodes": " | ".join(barcodes),
                    "price": price,
                    "compare_at_price": compare_at_price,
                    "stock_total": stock_total,
                    "in_stock": in_stock,
                    "current_on_site": visible_on_site,
                    "is_new": bool(product_rows["is_new_flag"].fillna(False).astype(bool).any()),
                    "created_at": _safe_str(first.get("created_at")),
                    "season_code": _safe_str(first.get("season_code")),
                    "product_types": "; ".join(product_types),
                    "category_name": primary_category or "Uncategorized",
                    "category_id": re.sub(r"[^0-9a-zа-я]+", "_", (primary_category or "Uncategorized").lower()).strip("_"),
                }
            )

        return pd.DataFrame(grouped_rows)
