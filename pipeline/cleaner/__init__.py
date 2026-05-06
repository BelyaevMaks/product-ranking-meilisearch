"""
Pipeline Cleaner Module - Data cleaning and normalization.

Handles:
- Gender heuristic detection
- Category extraction
- Sale/in-stock flag calculation
- Data type conversions
"""

from .cleaner import PipelineCleaner
from .entity_resolver import EntityResolver

__all__ = ['PipelineCleaner', 'EntityResolver']
