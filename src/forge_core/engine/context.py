"""
Forge Core — Context Module

Handles shared state for the warehouse adapter singleton.
"""

from forge_core.adapters import get_adapter

# Global adapter instance
ADAPTER = None


def get_warehouse_adapter():
    """
    Retrieves the global warehouse adapter instance.
    Initializes it to the 'bigquery' adapter if it hasn't been created yet.

    Returns:
        WarehouseAdapter instance
    """
    global ADAPTER
    if ADAPTER is None:
        ADAPTER = get_adapter("bigquery")
    return ADAPTER
