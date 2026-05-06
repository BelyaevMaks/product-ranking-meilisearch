"""
Index all strategy-specific scored catalogs into separate Meilisearch indexes.

Expects files: artifacts/scored_catalog_{baseline|funnel|robust|commercial}.csv
Index UIDs: {MEILISEARCH_INDEX_BASE}_{strategy} (default base: kixbox_products).

Usage:
  python src/index_all_strategy_indexes.py

Env:
  MEILISEARCH_URL, MEILISEARCH_MASTER_KEY
  MEILISEARCH_INDEX_BASE (default kixbox_products)
  MEILISEARCH_INDEX_OVERRIDE — if set, all strategies go to this single UID (not recommended)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import meilisearch
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.feature_engine.engine import SCORE_STRATEGIES
from pipeline.meilisearch_strategy_indexes import get_index_base, get_index_override, resolve_index_uid
from pipeline.search_indexer.indexer import SearchIndexer


def main() -> int:
    override = get_index_override()
    base = get_index_base()
    url = os.environ.get("MEILISEARCH_URL", "http://localhost:7700")
    api_key = os.environ.get(
        "MEILISEARCH_MASTER_KEY",
        "Yu4HfgPAV9gWM5nrutsfJi0b0RsXSFK8tiPw8Zx__Co",
    )
    artifacts = PROJECT_ROOT / "artifacts"
    client = meilisearch.Client(url, api_key)

    if override:
        print(f"[index-all] MEILISEARCH_INDEX_OVERRIDE set -> single index {override!r} (last write wins)")

    indexed = 0
    for code in SCORE_STRATEGIES:
        csv_path = artifacts / f"scored_catalog_{code}.csv"
        if not csv_path.exists():
            print(f"[index-all] Skip {code}: missing {csv_path.name}")
            continue

        uid = override or f"{base}_{code}"
        print(f"[index-all] {code}: {csv_path.name} -> index {uid!r}")
        df = pd.read_csv(csv_path, low_memory=False)
        indexer = SearchIndexer(client=client, index_name=uid)
        if not indexer.full_index_pipeline(df):
            print(f"[index-all] FAILED: {uid}")
            return 1
        indexed += 1

    if indexed == 0:
        print("[index-all] No scored_catalog_*.csv files found. Run analyze_score_variants.py or pipeline per strategy.")
        return 1

    print(f"[index-all] Done. Indexed {indexed} Meilisearch index(es).")
    print(f"[index-all] Example UID: {resolve_index_uid('baseline')!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
