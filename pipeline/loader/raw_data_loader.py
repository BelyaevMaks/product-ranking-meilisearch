"""
Raw source loader for the current catalog snapshot and JSON customer actions.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd

from ..config import PipelineConfig


@dataclass
class EventArchiveScanResult:
    """Summary of the JSON customer action archive."""

    json_files: int
    rows_scanned: int
    top_level_keys: List[str]
    product_related_paths: List[str]
    action_template_counts_top20: Dict[str, int]
    first_event_timestamp: Optional[str]
    last_event_timestamp: Optional[str]


@dataclass
class RawDataBundle:
    """In-memory raw tables used by downstream audit or cleaning stages."""

    shop_products: pd.DataFrame
    categories: pd.DataFrame


class RawDataLoader:
    """Loads raw source dumps and scans JSON event files."""

    SHOP_MINIMAL_COLUMNS = [
        "ID товара",
        "Название товара или услуги",
        "URL",
        "Описание",
        "Видимость на витрине",
        "Размещение на сайте",
        "ID варианта",
        "Артикул",
        "Штрих-код",
        "Цена продажи",
        "Старая цена",
        "Остаток",
        "Остаток: 212",
        "Остаток: 106",
        "Остаток: 102",
        "Остаток: 220",
        "Остаток: 218",
        "Остаток: 204",
        "Остаток: 217",
        "Остаток: 107",
        "Остаток: Технический",
        "Остаток: 202",
        "Параметр: Бренд",
        "Параметр: Пол",
        "Параметр: Тип",
        "Параметр: Сезон",
        "Параметр: Тип3",
        "Параметр: Тип2",
        "Параметр: новинка",
    ]

    def __init__(self, config: PipelineConfig):
        self.config = config

    def validate_sources(self) -> None:
        self.config.ensure_required_paths_exist()

    @staticmethod
    def _load_csv(
        path: Path,
        *,
        sep: str,
        encoding: str = "utf-8-sig",
        low_memory: bool = False,
        usecols: Optional[List[str]] = None,
        nrows: Optional[int] = None,
    ) -> pd.DataFrame:
        return pd.read_csv(
            path,
            sep=sep,
            encoding=encoding,
            low_memory=low_memory,
            usecols=usecols,
            nrows=nrows,
            dtype=str,
        )

    def load_shop_products(
        self,
        *,
        minimal: bool = False,
        nrows: Optional[int] = None,
    ) -> pd.DataFrame:
        usecols = self.SHOP_MINIMAL_COLUMNS if minimal else None
        return self._load_csv(
            self.config.shop_catalog_path,
            sep="\t",
            encoding="utf-16",
            low_memory=False,
            usecols=usecols,
            nrows=nrows,
        )

    def load_categories(self, *, nrows: Optional[int] = None) -> pd.DataFrame:
        return self._load_csv(
            self.config.categories_path,
            sep=";",
            low_memory=False,
            usecols=["CategoryIdsInsalesId", "CategoryName"],
            nrows=nrows,
        )

    def load_bundle(self) -> RawDataBundle:
        return self.load_bundle_selective(include_categories=True)

    def load_bundle_selective(
        self,
        *,
        include_categories: bool = True,
        row_limit: Optional[int] = None,
    ) -> RawDataBundle:
        self.validate_sources()
        return RawDataBundle(
            shop_products=self.load_shop_products(minimal=True, nrows=row_limit),
            categories=self.load_categories() if include_categories else pd.DataFrame(),
        )

    def list_event_json_files(self) -> List[Path]:
        base_dir = self.config.customer_actions_json_dir
        return sorted(path for path in base_dir.glob("*.json") if "__MACOSX" not in str(path))

    @staticmethod
    def _load_json_file(path: Path) -> Dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8-sig"))

    def iter_event_objects(
        self,
        *,
        max_files: Optional[int] = None,
        max_rows_per_file: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        json_files = self.list_event_json_files()
        if max_files is not None:
            json_files = json_files[:max_files]

        for json_path in json_files:
            payload = self._load_json_file(json_path)
            actions = payload.get("customerActions", [])
            if not isinstance(actions, list):
                continue

            row_count = 0
            for action in actions:
                if not isinstance(action, dict):
                    continue
                yield action
                row_count += 1
                if max_rows_per_file is not None and row_count >= max_rows_per_file:
                    break

    @staticmethod
    def _extract_template_name(action: Dict[str, Any]) -> str:
        template = action.get("actionTemplate", {}) or {}
        system_name = (((template.get("ids") or {}).get("systemName")) or "").strip()
        name = (template.get("name") or "").strip()
        return system_name or name

    @staticmethod
    def _parse_iso_datetime(value: str) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def scan_event_archive(
        self,
        *,
        max_files: Optional[int] = None,
        max_rows_per_file: Optional[int] = None,
    ) -> EventArchiveScanResult:
        json_files = self.list_event_json_files()
        if max_files is not None:
            json_files = json_files[:max_files]

        action_templates: Counter[str] = Counter()
        top_level_keys = set()
        product_related_paths = {
            "products[].ids.insalesId",
            "productCategories[].ids.insalesId",
            "order.lines[].product.ids.insalesId",
        }
        first_event_timestamp: Optional[datetime] = None
        last_event_timestamp: Optional[datetime] = None
        rows_scanned = 0

        for json_path in json_files:
            payload = self._load_json_file(json_path)
            actions = payload.get("customerActions", [])
            if not isinstance(actions, list):
                continue

            file_rows = 0
            for action in actions:
                if not isinstance(action, dict):
                    continue

                rows_scanned += 1
                file_rows += 1
                top_level_keys.update(action.keys())

                template_name = self._extract_template_name(action)
                if template_name:
                    action_templates[template_name] += 1

                timestamp = self._parse_iso_datetime(str(action.get("dateTimeUtc") or ""))
                if timestamp:
                    if first_event_timestamp is None or timestamp < first_event_timestamp:
                        first_event_timestamp = timestamp
                    if last_event_timestamp is None or timestamp > last_event_timestamp:
                        last_event_timestamp = timestamp

                if max_rows_per_file is not None and file_rows >= max_rows_per_file:
                    break

        return EventArchiveScanResult(
            json_files=len(json_files),
            rows_scanned=rows_scanned,
            top_level_keys=sorted(top_level_keys),
            product_related_paths=sorted(product_related_paths),
            action_template_counts_top20=dict(action_templates.most_common(20)),
            first_event_timestamp=first_event_timestamp.isoformat() if first_event_timestamp else None,
            last_event_timestamp=last_event_timestamp.isoformat() if last_event_timestamp else None,
        )

    @staticmethod
    def scan_result_to_dict(scan_result: EventArchiveScanResult) -> Dict[str, object]:
        return asdict(scan_result)
