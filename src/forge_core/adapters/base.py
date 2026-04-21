from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Any, Optional


class WarehouseAdapter(ABC):
    """
    Abstract base class for warehouse adapters.
    Defines the interface for interacting with different data warehouses.
    """

    @abstractmethod
    def execute_query(self, sql: str) -> pd.DataFrame:
        """Executes a SQL query and returns the result as a pandas DataFrame."""
        pass

    @abstractmethod
    def get_keys(
        self, table_name: str, field_name: str, is_array: bool
    ) -> pd.DataFrame:
        """Retrieves the keys from the source table."""
        pass

    @abstractmethod
    def get_types_sql(
        self, table_name: str, field_name: str, key: str, is_array: bool
    ) -> str:
        """Generates the SQL to discover types for a specific key."""
        pass

    @abstractmethod
    def build_select_expression(
        self, field_name: str, safe_field: str, clean_field_name: str, field_type: str
    ) -> str:
        """Generates the SELECT expression for a specific field (e.g., JSON extraction)."""
        pass

    @abstractmethod
    def get_create_table_sql(
        self,
        table_name: str,
        field_name: str,
        selects_sql: str,
        is_array: bool,
        table_path: str,
    ) -> str:
        """Generates the SQL to create a new table/model."""
        pass

    @abstractmethod
    def validate_source(self, table_name: str, field_name: str) -> bool:
        """Validates that the source table and field exist and are accessible."""
        pass

    @abstractmethod
    def get_root_table_sql(
        self,
        table_name: str,
        field_name: str,
        is_string: bool,
        limit: Optional[int] = None,
    ) -> str:
        """Generates the SQL for the root table."""
        pass

    @abstractmethod
    def get_rows_processed_sql(
        self, project: str, dataset: str, table: str, timestamp: str
    ) -> str:
        """Generates the SQL to count processed rows."""
        pass

    @abstractmethod
    def generate_rollup_sql(
        self,
        metadata_list: List[Dict[str, Any]],
        target_dataset: str,
        model_prefix: str = "",
    ) -> str:
        """Generates the SQL for the final rollup view."""
        pass

    @abstractmethod
    def clean_dataset(self, dataset: str) -> bool:
        """
        Drops all tables and views in the target dataset.
        Returns True if successful, False otherwise.
        """
        pass

    def apply_column_descriptions(
        self,
        dataset: str,
        model_descriptions: Dict[str, Dict[str, str]],
        logger=None,
    ) -> int:
        """
        Apply column descriptions to tables in the warehouse.
        Default implementation returns 0 (no-op for unsupported adapters).

        Args:
            dataset: Target dataset/schema
            model_descriptions: Dict of model_name -> {column_name: description}
            logger: Optional logger for output

        Returns:
            Number of tables updated
        """
        if logger:
            logger.info("Column descriptions not supported for this adapter")
        return 0
