"""
Pipeline orchestrator - main entry point for the ranking pipeline.
"""

from __future__ import annotations

import sys
import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

import meilisearch
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.audit import PipelineAudit
from pipeline.cleaner.cleaner import PipelineCleaner
from pipeline.config import PipelineConfig
from pipeline.feature_engine.engine import FeatureEngine
from pipeline.loader.event_stream import EventStreamBuilder
from pipeline.loader.raw_data_loader import RawDataLoader
from pipeline.search_indexer.indexer import SearchIndexer


class PipelineOrchestrator:
    """Coordinates the ranking pipeline from raw data to Meilisearch index."""

    def __init__(
        self,
        base_path: Optional[str] = None,
        scored_csv_path: Optional[str] = None,
        as_of_date: Optional[date] = None,
        score_window_days: Optional[int] = None,
        score_window_start: Optional[date] = None,
        score_window_end: Optional[date] = None,
        score_strategy: str = "baseline",
        enable_seasonality: bool = False,
        meilisearch_url: str = "http://localhost:7700",
        meilisearch_api_key: str = "Yu4HfgPAV9gWM5nrutsfJi0b0RsXSFK8tiPw8Zx__Co",
        index_name: str = "kixbox_products",
        skip_index: bool = False,
        source_row_limit: int = 1000,
        event_row_limit: int = 1000,
        event_file_limit: int = 1,
    ):
        self.base_path = Path(base_path).resolve() if base_path else Path(__file__).parent.parent.resolve()
        self.project_root = self.base_path.resolve()
        self.config = PipelineConfig.from_project_root(
            self.project_root,
            as_of_date=as_of_date,
            meilisearch_url=meilisearch_url,
            meilisearch_api_key=meilisearch_api_key,
            meilisearch_index_name=index_name,
        )

        self.scored_csv_path = scored_csv_path or str(self.config.scored_catalog_path)
        self.score_window_days = score_window_days if score_window_days and score_window_days > 0 else None
        self.score_window_start = score_window_start
        self.score_window_end = score_window_end
        self.score_strategy = score_strategy
        self.enable_seasonality = enable_seasonality
        self.meilisearch_url = meilisearch_url
        self.meilisearch_api_key = meilisearch_api_key
        self.index_name = index_name
        self.skip_index = skip_index
        self.source_row_limit = source_row_limit
        self.event_row_limit = event_row_limit
        self.event_file_limit = event_file_limit

        self.raw_loader: Optional[RawDataLoader] = None
        self.raw_bundle = None
        self.audit: Optional[PipelineAudit] = None
        self.cleaner: Optional[PipelineCleaner] = None
        self.feature_engine: Optional[FeatureEngine] = None
        self.search_indexer: Optional[SearchIndexer] = None
        self.results: Dict[str, Any] = {}

    def _create_meilisearch_client(self) -> meilisearch.Client:
        print(f"[Orchestrator] Connecting to Meilisearch at {self.meilisearch_url}...")
        client = meilisearch.Client(self.meilisearch_url, self.meilisearch_api_key)

        try:
            import requests as http_requests

            response = http_requests.get(f"{self.meilisearch_url}/health", timeout=5)
            if response.status_code == 200:
                print("[Orchestrator] Connected to Meilisearch successfully\n")
            else:
                raise ConnectionError(f"Meilisearch returned status {response.status_code}")
        except Exception as exc:
            try:
                client.get_indexes()
                print("[Orchestrator] Connected to Meilisearch successfully\n")
            except Exception:
                raise ConnectionError(f"Failed to connect to Meilisearch: {exc}") from exc

        return client

    def run(self) -> Dict[str, Any]:
        print("=" * 70)
        print(" KIXBOX RANKING PIPELINE - FULL EXECUTION")
        print("=" * 70)
        print()

        try:
            self._stage_audit_raw_sources()
            self._stage_load_data()
            self._stage_clean_data()
            self._stage_calculate_scores()
            if self.skip_index:
                self._stage_skip_meilisearch()
            else:
                self._stage_index_to_meilisearch()

            print("\n" + "=" * 70)
            print(" PIPELINE EXECUTION COMPLETE")
            print("=" * 70)
            self.results["success"] = True
            self._print_summary()
            return self.results

        except Exception as exc:
            print(f"\n[ERROR] Pipeline failed: {exc}")
            import traceback

            traceback.print_exc()
            self.results["success"] = False
            self.results["error"] = str(exc)
            raise

    def _stage_audit_raw_sources(self) -> None:
        print("-" * 70)
        print("STAGE 0: RAW SOURCE AUDIT")
        print("-" * 70)

        self.raw_loader = RawDataLoader(self.config)
        self.audit = PipelineAudit(self.config)

        report = self.audit.run(
            source_row_limit=self.source_row_limit,
            max_event_files=self.event_file_limit,
            max_event_rows_per_file=self.event_row_limit,
            intent_sample_files=1,
            intent_sample_rows_per_file=self.event_row_limit,
        )
        audit_path = self.audit.save_report(report)

        print("[Stage 0] Raw audit complete")
        print(f"  Raw data dir: {self.config.raw_data_dir}")
        print(f"  Event files scanned: {report['events']['json_files']}")
        print(f"  Derived as_of_date: {report.get('derived_as_of_date')}")
        print(f"  Audit report: {audit_path}\n")

        self.results["audit"] = {
            "raw_data_dir": str(self.config.raw_data_dir),
            "audit_report_path": str(audit_path),
            "derived_as_of_date": report.get("derived_as_of_date"),
            "shop_rows": report.get("shop_catalog", {}).get("rows", 0),
            "event_files": report.get("events", {}).get("json_files", 0),
            "event_rows_scanned": report.get("events", {}).get("rows_scanned", 0),
            "source_row_limit": self.source_row_limit,
        }

    def _stage_load_data(self) -> None:
        print("-" * 70)
        print("STAGE 1: LOAD DATA")
        print("-" * 70)

        if self.raw_loader is None:
            self.raw_loader = RawDataLoader(self.config)

        self.raw_bundle = self.raw_loader.load_bundle_selective(
            include_categories=True,
            row_limit=self.source_row_limit,
        )

        print("[Stage 1] Loaded raw source tables")
        print(f"  Shop rows: {len(self.raw_bundle.shop_products)}")
        print(f"  Categories rows: {len(self.raw_bundle.categories)}")
        print(f"  Event JSON dir: {self.config.customer_actions_json_dir}\n")

        self.results["load"] = {
            "mode": "raw_sources",
            "shop_rows": len(self.raw_bundle.shop_products),
            "categories_rows": len(self.raw_bundle.categories),
            "raw_data_dir": str(self.config.raw_data_dir),
            "source_row_limit": self.source_row_limit,
        }

    def _stage_clean_data(self) -> None:
        print("-" * 70)
        print("STAGE 2: CLEAN & NORMALIZE")
        print("-" * 70)

        if self.raw_bundle is None:
            raise RuntimeError("Raw bundle is required for the current pipeline flow")

        self.cleaner = PipelineCleaner.from_raw_sources(
            shop_products_df=self.raw_bundle.shop_products,
            categories_df=self.raw_bundle.categories,
        )

        cleaned_df = self.cleaner.clean()
        cleaned_path = self.cleaner.persist(self.config.cleaned_catalog_path)

        print("[Stage 2] Data cleaning complete")
        print(f"  Products: {len(cleaned_df)}")
        print(f"  Saved cleaned catalog: {cleaned_path}")
        print(f"  Gender distribution: {cleaned_df['gender'].value_counts(dropna=False).to_dict()}\n")

        self.results["clean"] = {
            "products_count": len(cleaned_df),
            "gender_distribution": cleaned_df["gender"].value_counts(dropna=False).to_dict(),
            "categories": int(cleaned_df["category_name"].nunique()),
            "cleaned_catalog_path": str(cleaned_path),
        }

    def _stage_calculate_scores(self) -> None:
        print("-" * 70)
        print("STAGE 3: CALCULATE SCORES")
        print("-" * 70)

        if self.cleaner is None or self.raw_bundle is None or self.raw_loader is None:
            raise RuntimeError("Cleaner, raw bundle and loader must be ready before scoring")

        event_builder = EventStreamBuilder(self.raw_loader)
        variant_lookup = self._build_variant_to_product_lookup(self.raw_bundle.shop_products)
        daily_product_events = event_builder.aggregate_daily_products(
            variant_lookup,
            max_files=self.event_file_limit,
            max_rows_per_file=self.event_row_limit,
            as_of_date=self.config.as_of_date.isoformat() if self.config.as_of_date else None,
        )
        daily_product_events.to_csv(self.config.product_daily_events_path, index=False, encoding="utf-8-sig")

        self.feature_engine = FeatureEngine(
            catalog_df=self.cleaner.catalog,
            events_df=daily_product_events,
            as_of_date=self.config.as_of_date,
            window_days=self.score_window_days,
            window_start=self.score_window_start,
            window_end=self.score_window_end,
            strategy=self.score_strategy,
            enable_seasonality=self.enable_seasonality,
        )
        scored_df = self.feature_engine.compute_scores()
        scored_df.to_csv(self.config.scored_catalog_path, index=False, encoding="utf-8-sig")

        effective_window_start, effective_window_end = self.feature_engine.resolve_event_window()

        print("[Stage 3] Scoring complete")
        print(f"  Products scored: {len(scored_df)}")
        print(f"  Product daily events: {self.config.product_daily_events_path}")
        if effective_window_start or effective_window_end:
            start_repr = effective_window_start.isoformat() if effective_window_start else "(-inf)"
            end_repr = effective_window_end.isoformat() if effective_window_end else "(+inf)"
            print(f"  Score event window: {start_repr} .. {end_repr}")
        print(f"  Saved scored catalog: {self.config.scored_catalog_path}\n")

        self.results["scoring"] = {
            "products_scored": len(scored_df),
            "event_daily_path": str(self.config.product_daily_events_path),
            "scored_catalog_path": str(self.config.scored_catalog_path),
            "window": {
                "days": self.score_window_days,
                "start": effective_window_start.isoformat() if effective_window_start else None,
                "end": effective_window_end.isoformat() if effective_window_end else None,
            },
            "strategy": self.score_strategy,
            "seasonality_enabled": self.enable_seasonality,
            "score_stats": {
                "popularity": {
                    "min": float(scored_df["popularity"].min()),
                    "max": float(scored_df["popularity"].max()),
                    "mean": float(scored_df["popularity"].mean()),
                },
                "novelty": {
                    "min": float(scored_df["novelty"].min()),
                    "max": float(scored_df["novelty"].max()),
                    "mean": float(scored_df["novelty"].mean()),
                },
                "final_score": {
                    "min": float(scored_df["final_score"].min()),
                    "max": float(scored_df["final_score"].max()),
                    "mean": float(scored_df["final_score"].mean()),
                },
            },
        }
        self._persist_run_metadata()

    @staticmethod
    def _build_variant_to_product_lookup(shop_products: pd.DataFrame) -> Dict[str, int]:
        if shop_products.empty:
            return {}

        mapping_frame = pd.DataFrame(
            {
                "product_id": pd.to_numeric(shop_products["ID товара"], errors="coerce"),
                "barcode": shop_products["Штрих-код"].fillna("").astype(str).str.strip(),
            }
        )
        mapping_frame = mapping_frame.loc[mapping_frame["product_id"].notna() & mapping_frame["barcode"].ne("")]
        return {
            str(row["barcode"]).strip(): int(row["product_id"])
            for _, row in mapping_frame.drop_duplicates(subset=["barcode"]).iterrows()
        }

    def _stage_index_to_meilisearch(self) -> None:
        print("-" * 70)
        print("STAGE 4: INDEX TO MEILISEARCH")
        print("-" * 70)

        if self.feature_engine is None:
            raise RuntimeError("Feature engine is not initialized")

        df = self.feature_engine.catalog
        meili_client = self._create_meilisearch_client()
        self.search_indexer = SearchIndexer(client=meili_client, index_name=self.index_name)

        success = self.search_indexer.full_index_pipeline(df)
        if not success:
            raise RuntimeError("Meilisearch indexing failed")

        print("[Stage 4] Indexed to Meilisearch successfully\n")
        self.results["indexing"] = {
            "success": True,
            "index_name": self.index_name,
            "products_indexed": len(df),
        }

    def _stage_skip_meilisearch(self) -> None:
        print("-" * 70)
        print("STAGE 4: INDEX TO MEILISEARCH")
        print("-" * 70)
        print("[Stage 4] Skipped by configuration (PIPELINE_SKIP_INDEX=1)\n")
        self.results["indexing"] = {
            "success": None,
            "skipped": True,
            "reason": "skip_index enabled",
            "index_name": self.index_name,
            "products_indexed": 0,
        }

    def _print_summary(self) -> None:
        print("\nPipeline Summary:")
        if "audit" in self.results:
            print(f"  Raw audit report: {self.results['audit'].get('audit_report_path')}")
            print(f"  Derived as_of_date: {self.results['audit'].get('derived_as_of_date')}")
        if "load" in self.results:
            print(f"  Load mode: {self.results['load'].get('mode')}")
        window_info = self.results.get("scoring", {}).get("window", {})
        if window_info:
            print(
                "  Score window: "
                f"days={window_info.get('days')}, "
                f"start={window_info.get('start')}, "
                f"end={window_info.get('end')}"
            )
        scoring_info = self.results.get("scoring", {})
        if scoring_info:
            print(f"  Score strategy: {scoring_info.get('strategy')}")
            print(f"  Seasonality enabled: {scoring_info.get('seasonality_enabled')}")
        print(f"  Products cleaned: {self.results.get('clean', {}).get('products_count', 0)}")
        print(f"  Scores calculated: {self.results.get('scoring', {}).get('products_scored', 0)}")
        indexing = self.results.get("indexing", {})
        if indexing.get("skipped"):
            print("  Products indexed: skipped")
        else:
            print(f"  Products indexed: {indexing.get('products_indexed', 0)}")

        if "score_stats" in self.results.get("scoring", {}):
            stats = self.results["scoring"]["score_stats"]
            print("\nScore Statistics:")
            print(
                f"  Popularity: min={stats['popularity']['min']:.4f}, "
                f"max={stats['popularity']['max']:.4f}, "
                f"mean={stats['popularity']['mean']:.4f}"
            )
            print(
                f"  Novelty: min={stats['novelty']['min']:.4f}, "
                f"max={stats['novelty']['max']:.4f}, "
                f"mean={stats['novelty']['mean']:.4f}"
            )
            print(
                f"  Final Score: min={stats['final_score']['min']:.6f}, "
                f"max={stats['final_score']['max']:.6f}, "
                f"mean={stats['final_score']['mean']:.6f}"
            )

    def _persist_run_metadata(self) -> None:
        metadata = {
            "as_of_date": self.config.as_of_date.isoformat() if self.config.as_of_date else None,
            "source_row_limit": self.source_row_limit,
            "event_row_limit": self.event_row_limit,
            "event_file_limit": self.event_file_limit,
            "score_window": self.results.get("scoring", {}).get("window", {}),
            "score_strategy": self.results.get("scoring", {}).get("strategy"),
            "seasonality_enabled": self.results.get("scoring", {}).get("seasonality_enabled"),
        }
        self.config.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.config.pipeline_run_metadata_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def main() -> None:
    import os

    def _env_int(name: str, default: int) -> int:
        raw_value = os.environ.get(name)
        if raw_value is None:
            return default
        try:
            return int(raw_value)
        except ValueError:
            return default

    def _env_optional_int(name: str) -> Optional[int]:
        raw_value = os.environ.get(name)
        if raw_value is None or not raw_value.strip():
            return None
        try:
            value = int(raw_value)
        except ValueError:
            return None
        return value if value > 0 else None

    def _env_optional_date(name: str) -> Optional[date]:
        raw_value = os.environ.get(name)
        if raw_value is None or not raw_value.strip():
            return None
        try:
            return date.fromisoformat(raw_value.strip())
        except ValueError:
            return None

    base_path = os.environ.get("PIPELINE_BASE_PATH", str(Path(__file__).parent.parent))
    meilisearch_url = os.environ.get("MEILISEARCH_URL", "http://localhost:7700")
    meilisearch_key = os.environ.get(
        "MEILISEARCH_MASTER_KEY",
        "Yu4HfgPAV9gWM5nrutsfJi0b0RsXSFK8tiPw8Zx__Co",
    )
    as_of_date_raw = os.environ.get("PIPELINE_AS_OF_DATE")
    as_of_date = date.fromisoformat(as_of_date_raw) if as_of_date_raw else None
    score_window_days = _env_optional_int("PIPELINE_SCORE_WINDOW_DAYS")
    score_window_start = _env_optional_date("PIPELINE_SCORE_WINDOW_START")
    score_window_end = _env_optional_date("PIPELINE_SCORE_WINDOW_END")
    score_strategy = os.environ.get("PIPELINE_SCORE_STRATEGY", "baseline").strip().lower() or "baseline"
    enable_seasonality = os.environ.get("PIPELINE_ENABLE_SEASONALITY", "").strip().lower() in {"1", "true", "yes", "y"}
    index_base = os.environ.get("MEILISEARCH_INDEX_BASE", "").strip()
    index_override = os.environ.get("MEILISEARCH_INDEX_OVERRIDE", "").strip()
    if index_override:
        index_name = index_override
    elif index_base:
        index_name = f"{index_base}_{score_strategy}"
    else:
        index_name = os.environ.get("MEILISEARCH_INDEX_NAME", "kixbox_products")
    source_row_limit = _env_int("PIPELINE_SOURCE_ROW_LIMIT", 1000)
    event_row_limit = _env_int("PIPELINE_EVENT_ROW_LIMIT", 1000)
    event_file_limit = _env_int("PIPELINE_EVENT_FILE_LIMIT", 1)
    skip_index = os.environ.get("PIPELINE_SKIP_INDEX", "").strip().lower() in {"1", "true", "yes", "y"}

    orchestrator = PipelineOrchestrator(
        base_path=base_path,
        as_of_date=as_of_date,
        score_window_days=score_window_days,
        score_window_start=score_window_start,
        score_window_end=score_window_end,
        score_strategy=score_strategy,
        enable_seasonality=enable_seasonality,
        meilisearch_url=meilisearch_url,
        meilisearch_api_key=meilisearch_key,
        index_name=index_name,
        skip_index=skip_index,
        source_row_limit=source_row_limit,
        event_row_limit=event_row_limit,
        event_file_limit=event_file_limit,
    )

    try:
        results = orchestrator.run()
        sys.exit(0 if results.get("success", True) else 1)
    except Exception as exc:
        print(f"\nPipeline execution failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
