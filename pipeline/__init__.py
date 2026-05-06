"""
Kixbox Ranking Pipeline - Modular Data Processing System

Architecture: Loader → Cleaner → FeatureEngine → SearchIndexer

Usage:
    from pipeline import PipelineOrchestrator
    
    orchestrator = PipelineOrchestrator()
    result = orchestrator.run()
"""

import platform
import sys


def _patch_windows_processor_probe() -> None:
    """
    Avoid a Windows/Python 3.13 WMI hang during pandas/numpy imports.

    The pipeline does not use CPU model metadata, so returning an empty
    processor string is safer than letting imports block indefinitely.
    """
    if sys.platform.startswith("win"):
        platform.system = lambda: "Windows"
        platform.machine = lambda: "AMD64"
        platform.processor = lambda: ""


_patch_windows_processor_probe()

from .config import PipelineConfig
from .audit import PipelineAudit
from .loader.raw_data_loader import RawDataLoader
from .cleaner.cleaner import PipelineCleaner
from .feature_engine.engine import FeatureEngine
from .search_indexer.indexer import SearchIndexer
from .orchestrator import PipelineOrchestrator

__all__ = [
    'PipelineConfig',
    'PipelineAudit',
    'RawDataLoader',
    'PipelineCleaner', 
    'FeatureEngine',
    'SearchIndexer',
    'PipelineOrchestrator'
]
