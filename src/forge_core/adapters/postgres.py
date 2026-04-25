"""
Forge Core — PostgreSQL Adapter

PostgreSQL implementation of the WarehouseAdapter.
Uses json (text-based) functions — NOT native jsonb operators.
All JSON/JSONB source columns are cast to text at the root model stage.

No rollup support — PostgreSQL cannot handle the CTE-heavy rollup SQL.
"""

import pandas as pd
import os
import sys
import logging
from typing import List, Dict, Any, Optional

from .base import WarehouseAdapter

logger = logging.getLogger(__name__)


class PostgresAdapter(WarehouseAdapter):
    """
    PostgreSQL implementation of the WarehouseAdapter.

    Connects via psycopg2. All JSON processing uses the text-based `json`
    type functions (json_object_keys, json_extract_path_text, json_typeof,
    json_array_elements) — never native jsonb operators.
    """

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None,
        schema: str = None,
    ):
        """
        Initialize PostgreSQL adapter.

        Args can be provided explicitly or via environment variables:
        - POSTGRES_HOST
        - POSTGRES_PORT (default: 5432)
        - POSTGRES_DATABASE
        - POSTGRES_USER
        - POSTGRES_PASSWORD
        - POSTGRES_SCHEMA (default: public)
        """
        self.host = host or os.getenv("POSTGRES_HOST")
        self.port = port or int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = database or os.getenv("POSTGRES_DATABASE")
        self.user = user or os.getenv("POSTGRES_USER")
        self.password = password or os.getenv("POSTGRES_PASSWORD")
        self.schema = schema or os.getenv("POSTGRES_SCHEMA", "public")
        self.connection = None
        # Lazy init — do not connect here

    def _initialize_connection(self):
        """Initialize PostgreSQL connection using psycopg2."""
        try:
            import psycopg2
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            # Enable autocommit so SELECT queries don't need explicit commits
            self.connection.autocommit = True
            logger.info(
                f"✓ Connected to PostgreSQL: {self.host}:{self.port}/{self.database}"
            )
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")

    def _ensure_client(self):
        """Lazy initialization of the connection."""
        if not self.connection:
            self._initialize_connection()

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        if not sql or sql.isspace():
            return pd.DataFrame()

        self._ensure_client()

        if not self.connection:
            logger.error("PostgreSQL connection not initialized (check credentials).")
            return pd.DataFrame()

        try:
            return pd.read_sql_query(sql, self.connection)
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            logger.debug(f"SQL: {sql}")
            raise

    def _read_template(self, template_name: str) -> str:
        """Read SQL template from templates/postgres directory."""
        try:
            base_path = os.path.dirname(__file__)
            template_path = os.path.join(
                base_path, "templates", "postgres", template_name
            )
            with open(template_path, "r") as f:
                return f.read()
        except FileNotFoundError:
            logger.error(f"Template not found: {template_name}")
            return ""

    # =========================================================================
    # Schema Discovery
    # =========================================================================

    def get_keys(
        self, table_name: str, field_name: str, is_array: bool
    ) -> pd.DataFrame:
        """
        Get JSON keys from a field.

        Uses json_array_elements + json_object_keys to discover all unique
        keys across all rows. Works on text columns containing JSON strings.
        """
        # Everything in Forge is wrapped as an array (root wraps in [...]),
        # so always use the array template
        sql = self._read_template("get_keys_array.sql")
        sql = sql.replace("~JSON_FIELD~", field_name)
        sql = sql.replace("~TABLE_NAME~", table_name)
        return self.execute_query(sql)

    def get_types_sql(
        self, table_name: str, field_name: str, key: str, is_array: bool
    ) -> str:
        """
        Generate SQL to detect the type of a JSON key.

        Uses json_typeof() with type promotion:
        object (0) > array (1) > scalar (2).
        """
        sql = self._read_template("get_types_array.sql")
        sql = sql.replace("~JSON_FIELD~", field_name)
        sql = sql.replace("~KEY~", key)
        sql = sql.replace("~TABLE_NAME~", table_name)
        return sql

    # =========================================================================
    # SQL Generation
    # =========================================================================

    def build_select_expression(
        self, field_name: str, safe_field: str, clean_field_name: str, field_type: str
    ) -> str:
        """
        Generate PostgreSQL SQL for JSON field extraction.

        Uses json_extract_path_text() for scalars, json_extract_path()::text
        for objects/arrays. Objects are wrapped in [...] for next-level processing.
        """
        if field_type == "array":
            # Array: pass through as-is (already a JSON array string)
            return (
                f"CASE "
                f"WHEN json_typeof(json_extract_path(\"{field_name}\"::json, '{safe_field}')) = 'null' "
                f"THEN NULL "
                f"ELSE json_extract_path(\"{field_name}\"::json, '{safe_field}')::text "
                f"END AS \"{clean_field_name}\""
            )
        elif field_type == "object":
            # Object: wrap in [...] so downstream treats it as an array
            return (
                f"CASE "
                f"WHEN json_typeof(json_extract_path(\"{field_name}\"::json, '{safe_field}')) = 'null' "
                f"THEN NULL "
                f"ELSE '[' || json_extract_path(\"{field_name}\"::json, '{safe_field}')::text || ']' "
                f"END AS \"{clean_field_name}\""
            )
        else:
            # Scalar: extract as text
            return (
                f"json_extract_path_text(\"{field_name}\"::json, '{safe_field}') "
                f"AS \"{clean_field_name}\""
            )

    def get_create_table_sql(
        self,
        table_name: str,
        field_name: str,
        selects_sql: str,
        is_array: bool,
        table_path: str,
    ) -> str:
        """Generate dbt model SQL for a child table."""
        sql = self._read_template("create_table_array.sql")
        sql = sql.replace("~TABLE_NAME~", table_name)
        sql = sql.replace("~JSON_FIELD~", field_name)
        sql = sql.replace("~DBT_SELECT~", selects_sql)
        sql = sql.replace("~TABLE_PATH~", table_path)
        return sql

    def get_root_table_sql(
        self,
        table_name: str,
        limit: Optional[int] = None,
    ) -> str:
        """Generate root model SQL."""
        sql = self._read_template("create_root_aggregate.sql")
        sql = sql.replace("~SQL_SELECTS~", self.get_json_column_mapping(table_name))

        if limit is not None:
            sql = sql.replace("~LIMITER~", f"LIMIT {limit}")
        else:
            sql = sql.replace("~LIMITER~", "")

        return sql

    def get_rows_processed_sql(
        self, project: str, dataset: str, table: str, timestamp: str
    ) -> str:
        """Generate SQL to count rows processed."""
        sql = self._read_template("get_rows_processed.sql")
        sql = sql.replace("~SCHEMA~", dataset)
        sql = sql.replace("~TABLE_NAME~", table)
        sql = sql.replace("~BEGINNING_TS~", timestamp)
        return sql

    # =========================================================================
    # Rollup — NOT SUPPORTED for PostgreSQL
    # =========================================================================

    def generate_rollup_sql(
        self,
        metadata_list: List[Dict[str, Any]],
        target_dataset: str,
        model_prefix: str = "",
    ) -> str:
        """
        Rollup is not supported for PostgreSQL.

        PostgreSQL cannot handle the CTE-heavy rollup SQL that Forge generates
        for the other warehouses. Returns empty string.
        """
        return ""

    # =========================================================================
    # Source Validation & Management
    # =========================================================================

    def validate_source(self, table_name: str) -> bool:
        """Validate connection to source table."""
        try:
            self._ensure_client()
            sql = f"SELECT 1 FROM {table_name} LIMIT 1"
            self.execute_query(sql)
            logger.info("✓ Successfully connected to PostgreSQL source")
            return True
        except Exception as e:
            logger.error(f"Source validation failed: {e}")
            return False

    def clean_dataset(self, dataset: str) -> bool:
        """Drop all tables and views in target schema."""
        try:
            self._ensure_client()
            if not self.connection:
                return False

            get_object_sql = f"""
                SELECT
                    'DROP ' || CASE WHEN table_type = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END
                    || ' IF EXISTS "' || table_schema || '"."' || table_name || '" CASCADE;' AS _drop
                FROM information_schema.tables
                WHERE table_schema = '{dataset}'
                AND table_type IN ('BASE TABLE', 'VIEW')
            """

            object_deletes_df = self.execute_query(get_object_sql)

            if object_deletes_df.empty:
                return True

            cursor = self.connection.cursor()
            for i in range(len(object_deletes_df)):
                drop_sql = object_deletes_df.iloc[i, 0]
                try:
                    cursor.execute(drop_sql)
                except Exception as e:
                    logger.error(f"Error dropping object: {e}")
                    cursor.close()
                    return False
            cursor.close()

            return True
        except Exception as e:
            logger.error(f"Error cleaning dataset {dataset}: {e}")
            return False

    # =========================================================================
    # JSON Column Detection (non-ABC, used internally)
    # =========================================================================

    def check_column_is_json(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column contains JSON data.

        Attempts to cast the first non-null value to json and checks that
        it starts with '{' or '[' (object or array, not bare scalars).
        """
        query = f"""
            SELECT
                ({column_name}::text)::json IS NOT NULL
                AND left(ltrim({column_name}::text), 1) IN ('{{', '[')
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            LIMIT 1
        """
        try:
            result_df = self.execute_query(query)
            if not result_df.empty and bool(result_df.iloc[0, 0]):
                return True
        except Exception:
            return False
        return False

    def get_table_columns(self, table_name: str) -> List[str]:
        """Get all column names from a table."""
        query = f"SELECT * FROM {table_name} LIMIT 1"
        df = self.execute_query(query)
        return df.columns.tolist()

    def get_json_column_mapping(self, table_name: str) -> str:
        """
        Generate SQL that formats table rows as a single JSON object.

        For single-column JSON tables: uses the column directly as root.
        For multi-column tables: wraps all columns into a json_build_object.

        JSONB columns are cast to text first (spec constraint #3).
        """
        columns = self.get_table_columns(table_name)

        # Single JSON column case
        if len(columns) == 1 and self.check_column_is_json(table_name, columns[0]):
            return (
                f'SELECT {columns[0]}::text::json AS "root" '
                f"FROM {table_name} "
                f"~LIMITER~"
            )

        # Multi-column case: wrap in json_build_object
        build_parts = []
        for col in columns:
            if self.check_column_is_json(table_name, col):
                # Cast jsonb/json to text then back to json
                build_parts.append(f"'{col}', {col}::text::json")
            else:
                build_parts.append(f"'{col}', {col}")

        sql = (
            f"SELECT json_build_object({', '.join(build_parts)}) AS \"root\" "
            f"FROM {table_name} "
            f"~LIMITER~"
        )
        return sql
