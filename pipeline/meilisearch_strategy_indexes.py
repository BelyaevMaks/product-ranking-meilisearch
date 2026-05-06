"""
Meilisearch multi-index naming: one physical index per score strategy.

Index UID pattern: {MEILISEARCH_INDEX_BASE}_{strategy}
Example: kixbox_products_baseline, kixbox_products_funnel

Optional escape hatch: MEILISEARCH_INDEX_OVERRIDE forces a single UID for all requests.
"""

from __future__ import annotations

import os
from typing import Optional

from pipeline.feature_engine.engine import SCORE_STRATEGIES

ALLOWED_STRATEGY_CODES = frozenset(SCORE_STRATEGIES.keys())


def get_index_base() -> str:
    return (os.environ.get("MEILISEARCH_INDEX_BASE") or "kixbox_products").strip()


def get_index_override() -> str:
    return (os.environ.get("MEILISEARCH_INDEX_OVERRIDE") or "").strip()


def get_default_strategy() -> str:
    raw = (os.environ.get("MEILISEARCH_DEFAULT_STRATEGY") or "baseline").strip().lower()
    return raw if raw in ALLOWED_STRATEGY_CODES else "baseline"


def normalize_strategy(strategy: Optional[str], *, default: Optional[str] = None) -> str:
    """Return a validated strategy code or raise ValueError."""
    resolved = (strategy if strategy is not None else default or get_default_strategy()).strip().lower()
    if resolved not in ALLOWED_STRATEGY_CODES:
        allowed = ", ".join(sorted(ALLOWED_STRATEGY_CODES))
        raise ValueError(f"Unknown strategy '{strategy}'. Allowed: {allowed}")
    return resolved


def resolve_index_uid(strategy: Optional[str] = None) -> str:
    """Resolve Meilisearch index UID for API / indexer."""
    override = get_index_override()
    if override:
        return override
    base = get_index_base()
    code = normalize_strategy(strategy)
    return f"{base}_{code}"


def strategy_labels() -> dict[str, str]:
    return {code: strat.label for code, strat in SCORE_STRATEGIES.items()}
