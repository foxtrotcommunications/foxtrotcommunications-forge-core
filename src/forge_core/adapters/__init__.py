"""
Forge Core — Adapter Factory

Maps source_type strings to warehouse adapter classes.
"""

from forge_core.adapters.base import WarehouseAdapter


def get_adapter(adapter_type: str = "bigquery", **kwargs) -> WarehouseAdapter:
    """
    Create and return the appropriate warehouse adapter.

    Args:
        adapter_type: One of 'bigquery', 'snowflake', 'databricks', 'redshift'

    Returns:
        WarehouseAdapter instance
    """
    adapter_type = adapter_type.lower()

    if adapter_type == "bigquery":
        from forge_core.adapters.bigquery import BigQueryAdapter
        return BigQueryAdapter(**kwargs)
    elif adapter_type == "snowflake":
        from forge_core.adapters.snowflake import SnowflakeAdapter
        return SnowflakeAdapter(**kwargs)
    elif adapter_type == "databricks":
        from forge_core.adapters.databricks import DatabricksAdapter
        return DatabricksAdapter(**kwargs)
    elif adapter_type == "redshift":
        from forge_core.adapters.redshift import RedshiftAdapter
        return RedshiftAdapter(**kwargs)
    else:
        raise ValueError(
            f"Unknown adapter type: {adapter_type}. "
            f"Supported: bigquery, snowflake, databricks, redshift"
        )
