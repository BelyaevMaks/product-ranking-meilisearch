"""
Central configuration for the ranking pipeline.

The pipeline now relies on:
- current catalog snapshot from `shop_data-10.04.2026 2.csv`
- category dictionary from `categories.csv`
- customer events from `customers-actions-new/*.json`
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
import re


@dataclass
class PipelineConfig:
    """Shared configuration for raw-data and indexing stages."""

    project_root: Path
    raw_data_dir: Path
    artifacts_dir: Path
    as_of_date: Optional[date] = None
    meilisearch_url: str = "http://localhost:7700"
    meilisearch_api_key: str = "Yu4HfgPAV9gWM5nrutsfJi0b0RsXSFK8tiPw8Zx__Co"
    meilisearch_index_name: str = "kixbox_products"

    @classmethod
    def from_project_root(
        cls,
        project_root: str | Path,
        *,
        raw_data_subdir: str = "Personalisation LAB 26",
        artifacts_subdir: str = "artifacts",
        as_of_date: Optional[date] = None,
        meilisearch_url: str = "http://localhost:7700",
        meilisearch_api_key: str = "Yu4HfgPAV9gWM5nrutsfJi0b0RsXSFK8tiPw8Zx__Co",
        meilisearch_index_name: str = "kixbox_products",
    ) -> "PipelineConfig":
        root = Path(project_root).resolve()
        return cls(
            project_root=root,
            raw_data_dir=root / raw_data_subdir,
            artifacts_dir=root / artifacts_subdir,
            as_of_date=as_of_date,
            meilisearch_url=meilisearch_url,
            meilisearch_api_key=meilisearch_api_key,
            meilisearch_index_name=meilisearch_index_name,
        )

    @property
    def shop_catalog_path(self) -> Path:
        return self.raw_data_dir / "shop_data-10.04.2026 2.csv"

    @property
    def categories_path(self) -> Path:
        return self.raw_data_dir / "categories.csv"

    @property
    def customer_actions_json_dir(self) -> Path:
        return self.raw_data_dir / "customers-actions-new"

    @property
    def shop_snapshot_date(self) -> Optional[date]:
        match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", self.shop_catalog_path.name)
        if not match:
            return None
        day, month, year = match.groups()
        try:
            return date(int(year), int(month), int(day))
        except ValueError:
            return None

    @property
    def cleaned_catalog_path(self) -> Path:
        return self.artifacts_dir / "cleaned_catalog.csv"

    @property
    def scored_catalog_path(self) -> Path:
        return self.artifacts_dir / "scored_catalog.csv"

    @property
    def audit_report_path(self) -> Path:
        return self.artifacts_dir / "raw_audit_report.json"

    @property
    def product_daily_events_path(self) -> Path:
        return self.artifacts_dir / "event_product_daily.csv"

    @property
    def pipeline_run_metadata_path(self) -> Path:
        return self.artifacts_dir / "pipeline_run_metadata.json"

    def ensure_required_paths_exist(self) -> None:
        required_paths = [
            self.shop_catalog_path,
            self.categories_path,
            self.customer_actions_json_dir,
        ]
        missing = [str(path) for path in required_paths if not path.exists()]
        if missing:
            raise FileNotFoundError(
                "Missing required raw sources:\n" + "\n".join(missing)
            )

    def resolve_as_of_date(self, last_event_timestamp: Optional[datetime]) -> date:
        """
        Return the observation date.

        If the config already has an explicit date, keep it.
        Otherwise derive it as the day after the latest event timestamp.
        """
        if self.as_of_date is not None:
            return self.as_of_date

        if last_event_timestamp is None:
            raise ValueError(
                "Cannot derive as_of_date without the latest event timestamp."
            )

        derived_date = last_event_timestamp.date() + timedelta(days=1)
        snapshot_date = self.shop_snapshot_date
        if snapshot_date is not None and derived_date > snapshot_date:
            derived_date = snapshot_date

        self.as_of_date = derived_date
        return self.as_of_date
