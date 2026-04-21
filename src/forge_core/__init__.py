"""
Forge Core — Open-source JSON→relational decomposition engine.

Automatically decompose nested JSON in your data warehouse into
normalized dbt models, rollup views, and browseable documentation.
"""

__version__ = "0.1.0"

from forge_core.core import build_core, CoreBuildResult

__all__ = ["build_core", "CoreBuildResult", "__version__"]
