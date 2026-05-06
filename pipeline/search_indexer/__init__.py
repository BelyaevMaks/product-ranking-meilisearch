"""
Pipeline Search Indexer Module - Meilisearch indexing.

Handles:
- Document preparation for Meilisearch
- Index creation and configuration
- Bulk document import
"""

from .indexer import SearchIndexer

__all__ = ['SearchIndexer']
