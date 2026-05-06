"""
CLI wrapper for running the ranking pipeline from the project root.
"""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.orchestrator import main


if __name__ == "__main__":
    main()
