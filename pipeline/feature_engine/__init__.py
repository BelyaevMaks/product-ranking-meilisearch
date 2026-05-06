"""
Pipeline Feature Engine Module - Ranking score calculation.

Handles:
- Popularity scoring with time decay
- Novelty scoring for cold-start products
- Boost multiplier calculation
- Final ranking score aggregation
"""

from .engine import FeatureEngine

__all__ = ['FeatureEngine']
