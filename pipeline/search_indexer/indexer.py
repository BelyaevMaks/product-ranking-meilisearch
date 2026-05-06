"""
Pipeline Search Indexer - Meilisearch indexing for product catalog.
"""

from __future__ import annotations

from typing import Dict, List, Optional

import meilisearch
import pandas as pd


class SearchIndexer:
    """Handles all Meilisearch indexing operations."""

    def __init__(
        self,
        client: Optional[meilisearch.Client],
        index_name: str = "kixbox_products",
        primary_key: str = "id",
    ):
        self.client = client
        self.index_name = index_name
        self.primary_key = primary_key

    def prepare_documents(self, catalog_df: pd.DataFrame) -> List[Dict]:
        print(f"[SearchIndexer] Preparing {len(catalog_df)} documents for indexing...")

        def _str(row: pd.Series, key: str, default: str = "") -> str:
            value = row.get(key, default)
            if pd.isna(value):
                return default
            return str(value)

        def _float(row: pd.Series, key: str, default: float = 0.0) -> float:
            value = pd.to_numeric(row.get(key, default), errors="coerce")
            if pd.isna(value):
                return default
            return float(value)

        def _bool(row: pd.Series, key: str, default: bool = False) -> bool:
            value = row.get(key, default)
            if pd.isna(value):
                return default
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in {"true", "1", "yes"}

        records = []
        for _, row in catalog_df.iterrows():
            product_id = int(row["product_id"])
            brand = _str(row, "vendor")

            records.append(
                {
                    "id": product_id,
                    "product_id": product_id,
                    "title": _str(row, "title"),
                    "brand": brand,
                    "vendor": brand,
                    "article": _str(row, "article"),
                    "barcode": _str(row, "barcode"),
                    "product_url": _str(row, "product_url"),
                    "category_id": _str(row, "category_id"),
                    "category_name": _str(row, "category_name", "Uncategorized") or "Uncategorized",
                    "product_types": _str(row, "product_types"),
                    "gender": _str(row, "gender", "unknown") or "unknown",
                    "price": _float(row, "price"),
                    "discount": _float(row, "discount"),
                    "in_stock": _bool(row, "in_stock"),
                    "current_on_site": _bool(row, "current_on_site"),
                    "is_new": _bool(row, "is_new"),
                    "is_sale": _bool(row, "is_sale"),
                    "created_at": _str(row, "created_at"),
                    "age_days": _float(row, "age_days"),
                    "views": int(_float(row, "views")),
                    "purchases": int(_float(row, "purchases")),
                    "popularity": _float(row, "popularity"),
                    "novelty": _float(row, "novelty"),
                    "boost": _float(row, "boost", 1.0),
                    "final_score": _float(row, "final_score"),
                }
            )

        print(f"[SearchIndexer] Prepared {len(records)} documents")
        return records

    def create_or_update_index(self, settings: Optional[Dict] = None):
        print(f"[SearchIndexer] Configuring Meilisearch index '{self.index_name}'...")

        try:
            indexes_response = self.client.get_indexes()
            if hasattr(indexes_response, "results"):
                existing_uids = [idx.uid for idx in indexes_response.results]
            elif isinstance(indexes_response, dict):
                raw_results = indexes_response.get("results", [])
                existing_uids = []
                for idx in raw_results:
                    if hasattr(idx, "uid"):
                        existing_uids.append(idx.uid)
                    elif isinstance(idx, dict) and "uid" in idx:
                        existing_uids.append(idx["uid"])
            else:
                existing_uids = []

            if self.index_name in existing_uids:
                print(f"[SearchIndexer] Deleting existing index '{self.index_name}'...")
                task = self.client.index(self.index_name).delete()
                self.client.wait_for_task(task.task_uid)
                print("[SearchIndexer] Index deleted successfully")

        except Exception as exc:
            print(f"[SearchIndexer] Note: Could not check existing indexes: {exc}")

        task = self.client.create_index(
            uid=self.index_name,
            options={"primaryKey": self.primary_key},
        )
        print(f"[SearchIndexer] Creating index '{self.index_name}' (primary key: '{self.primary_key}')...")
        self.client.wait_for_task(task.task_uid)

        if settings is None:
            settings = {
                "filterableAttributes": [
                    "product_id",
                    "category_id",
                    "category_name",
                    "brand",
                    "gender",
                    "price",
                    "is_new",
                    "is_sale",
                    "in_stock",
                    "current_on_site",
                ],
                "sortableAttributes": [
                    "final_score",
                    "popularity",
                    "novelty",
                    "price",
                    "discount",
                    "age_days",
                ],
                "searchableAttributes": [
                    "title",
                    "brand",
                    "article",
                    "category_name",
                    "product_types",
                ],
                "displayedAttributes": ["*"],
                "rankingRules": [
                    "words",
                    "typo",
                    "proximity",
                    "attribute",
                    "sort",
                    "exactness",
                    "final_score:desc",
                ],
            }

        index = self.client.index(self.index_name)
        task = index.update_settings(settings)
        self.client.wait_for_task(task.task_uid)

        print("[SearchIndexer] Index settings applied:")
        print(f"  - Filterable: {settings['filterableAttributes']}")
        print(f"  - Sortable: {settings['sortableAttributes']}")
        print(f"  - Searchable: {settings['searchableAttributes']}\n")

    def import_documents(self, documents: List[Dict]) -> Dict:
        print(f"[SearchIndexer] Importing {len(documents)} documents...")

        index = self.client.index(self.index_name)
        task = index.add_documents(documents)
        result_task = self.client.wait_for_task(task.task_uid)

        print("[SearchIndexer] Import complete:")
        print(f"  - Task UID: {result_task.uid}")
        print(f"  - Status: {result_task.status}")
        print(f"  - Documents indexed: {len(documents)}\n")

        return {"uid": result_task.uid, "status": result_task.status}

    def get_index_stats(self) -> Dict:
        try:
            index = self.client.index(self.index_name)
            stats = index.get_stats()

            if hasattr(stats, "number_of_documents"):
                stats_payload = {
                    "numberOfDocuments": stats.number_of_documents,
                    "isIndexing": getattr(stats, "is_indexing", None),
                    "fieldDistribution": getattr(stats, "field_distribution", None),
                }
            else:
                stats_payload = stats

            print(f"[SearchIndexer] Index '{self.index_name}' statistics:")
            print(f"  - Number of documents: {stats_payload['numberOfDocuments']}")
            print(f"  - Is indexing: {stats_payload['isIndexing']}")
            print(f"  - Primary key: {self.primary_key}")

            return stats_payload

        except Exception as exc:
            print(f"[SearchIndexer] Error getting index stats: {exc}")
            return {}

    def full_index_pipeline(self, catalog_df: pd.DataFrame) -> bool:
        print("=" * 60)
        print("SEARCH INDEXER PIPELINE")
        print("=" * 60)

        try:
            documents = self.prepare_documents(catalog_df)
            self.create_or_update_index()
            self.import_documents(documents)
            self.get_index_stats()

            print("=" * 60)
            print("[SUCCESS] Indexing pipeline completed successfully")
            print("=" * 60)
            return True

        except Exception as exc:
            print("=" * 60)
            print(f"[FAILED] Indexing pipeline failed: {exc}")
            print("=" * 60)
            return False
