import pandas as pd
import psycopg2
from .base import WarehouseAdapter
from typing import List, Dict, Any, Optional
import os
import sys


class RedshiftAdapter(WarehouseAdapter):
    """
    Amazon Redshift implementation of the WarehouseAdapter.
    Redshift is based on PostgreSQL 8.0.2 with AWS-specific extensions.
    """

    def __init__(
        self,
        host: str = None,
        port: int = 5439,
        database: str = None,
        user: str = None,
        password: str = None,
        schema: str = "public",
    ):
        """
        Initialize Redshift connection.

        Args can be provided explicitly or via environment variables:
        - REDSHIFT_HOST
        - REDSHIFT_PORT (default: 5439)
        - REDSHIFT_DATABASE
        - REDSHIFT_USER
        - REDSHIFT_PASSWORD
        - REDSHIFT_SCHEMA (default: public)
        """
        self.host = host or os.getenv("REDSHIFT_HOST")
        self.port = port or int(os.getenv("REDSHIFT_PORT", "5439"))
        self.database = database or os.getenv("REDSHIFT_DATABASE")
        self.user = user or os.getenv("REDSHIFT_USER")
        self.password = password or os.getenv("REDSHIFT_PASSWORD")
        self.schema = schema or os.getenv("REDSHIFT_SCHEMA", "public")

        self.connection = None
        self._initialize_connection()

    def _initialize_connection(self):
        """Initialize Redshift connection using psycopg2."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            print(
                f"✓ Connected to Redshift: {self.database}.{self.schema}",
                file=sys.stderr,
            )
        except Exception as e:
            print(f"Error connecting to Redshift: {e}", file=sys.stderr)

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        if not sql or sql.isspace():
            return pd.DataFrame()

        if not self.connection:
            self._initialize_connection()

        try:
            return pd.read_sql_query(sql, self.connection)
        except Exception as e:
            print(f"Query execution error: {e}", file=sys.stderr)
            print(f"SQL: {sql}", file=sys.stderr)
            print(f"SQL: {sql}", file=sys.stderr)
            raise

    def _read_template(self, template_name: str) -> str:
        """Read SQL template from templates/redshift directory."""
        try:
            base_path = os.path.dirname(__file__)
            template_path = os.path.join(
                base_path, "templates", "redshift", template_name
            )
            with open(template_path, "r") as f:
                return f.read()
        except FileNotFoundError:
            print(
                f"Error: Template {template_name} not found at {template_path}",
                file=sys.stderr,
            )
            return ""

    def build_select_expression(
        self, field_name: str, safe_field: str, clean_field_name: str, field_type: str
    ) -> str:
        """
        Generate Redshift SQL for JSON field extraction.

        Redshift uses JSON functions similar to PostgreSQL:
        - JSON_EXTRACT_PATH_TEXT() for scalar values
        - JSON_EXTRACT_PATH() for objects/arrays
        """
        # Redshift JSON path uses dot notation without $
        # BigQuery: $.field.subfield → Redshift: field,subfield (as arguments)

        if field_type == "array":
            return f"""
            COALESCE(
                JSON_EXTRACT_PATH_TEXT(
                    CASE 
                        WHEN JSON_SERIALIZE(
                            JSON_PARSE(REGEXP_REPLACE(REGEXP_REPLACE(JSON_SERIALIZE(JSON_EXTRACT_PATH("{field_name}", '{safe_field}')), '^\[\\[+', '['), '\\]\\]+$', ']'))
                        ) = 'null'
                        THEN NULL
                        ELSE REGEXP_REPLACE(REGEXP_REPLACE(JSON_EXTRACT_PATH(JSON_EXTRACT_PATH("{field_name}", '{safe_field}'), '$'), '^\[\\[+', '['), '\\]\\]+$', ']')
                    END
                ),
                CASE 
                    WHEN JSON_SERIALIZE(
                        JSON_PARSE(REGEXP_REPLACE(REGEXP_REPLACE(JSON_SERIALIZE(JSON_EXTRACT_PATH("{field_name}", '{safe_field}')), '^\[\\[+', '['), '\\]\\]+$', ']'))
                    ) = 'null'
                    THEN NULL
                    ELSE REGEXP_REPLACE(REGEXP_REPLACE(JSON_EXTRACT_PATH(JSON_EXTRACT_PATH("{field_name}", '{safe_field}'), '$'), '^\[\\[+', '['), '\\]\\]+$', ']')
                END
            ) AS "{clean_field_name}"
            """
        elif field_type == "object":
            return f"""
            REGEXP_REPLACE(REGEXP_REPLACE('[' ||
            COALESCE(
                JSON_EXTRACT_PATH_TEXT(
                    CASE 
                        WHEN JSON_SERIALIZE(JSON_EXTRACT_PATH("{field_name}", '{safe_field}')) = 'null'
                        THEN NULL
                        ELSE JSON_EXTRACT_PATH(JSON_EXTRACT_PATH("{field_name}", '{safe_field}'), '$')::VARCHAR
                    END
                ),
                CASE 
                    WHEN JSON_SERIALIZE(JSON_EXTRACT_PATH("{field_name}", '{safe_field}')) = 'null'
                    THEN NULL
                    ELSE JSON_EXTRACT_PATH(JSON_EXTRACT_PATH("{field_name}", '{safe_field}'), '$')::VARCHAR
                END
            ) || ']', '^\[\\[+', '['), '\\]\\]+$', ']') AS "{clean_field_name}"
            """
        else:
            # Scalar field
            return f"""JSON_EXTRACT_PATH_TEXT("{field_name}", '{safe_field}') AS "{clean_field_name}" """

    def get_keys(
        self, table_name: str, field_name: str, is_array: bool
    ) -> pd.DataFrame:
        """Get JSON keys from a field."""
        # Redshift doesn't have JSON_KEYS, need to use custom logic
        sql = f"""
            SELECT JSON_EXTRACT_PATH_TEXT({field_name}, '$') as keys
            FROM "{table_name}"
            LIMIT 1
        """
        df = self.execute_query(sql)
        return df

    def get_types_sql(
        self, table_name: str, field_name: str, key: str, is_array: bool
    ) -> str:
        """Generate SQL to get field types."""
        # Simplified for Redshift - type detection similar to PostgreSQL
        sql = f"""
            SELECT
                '{key}' as field,
                CASE 
                    WHEN JSON_TYPEOF(JSON_EXTRACT_PATH("{field_name}", '{key}')) = 'object' THEN 'object'
                    WHEN JSON_TYPEOF(JSON_EXTRACT_PATH("{field_name}", '{key}')) = 'array' THEN 'array'
                    ELSE 'scalar'
                END as type
            FROM "{table_name}"
            LIMIT 1
        """
        return sql

    def get_create_table_sql(
        self,
        table_name: str,
        field_name: str,
        selects_sql: str,
        is_array: bool,
        table_path: str,
    ) -> str:
        """Generate CREATE TABLE SQL for Redshift."""

        # Add array unnesting if this is an array field
        if is_array and field_name:
            # Redshift: Use SUPER type array iteration with index
            # Generate rows for each array element using a numbers table
            from_clause = f"""FROM {table_name},
    (SELECT ROW_NUMBER() OVER () - 1 AS arr_idx FROM STL_SCAN LIMIT 1000) AS array_indices
WHERE JSON_ARRAY_LENGTH("{field_name}") > array_indices.arr_idx
  AND JSON_EXTRACT_ARRAY_ELEMENT_TEXT("{field_name}", array_indices.arr_idx) IS NOT NULL"""
        else:
            from_clause = f"FROM {table_name}"

        # Use dbt ref syntax
        sql = f"""
        {{{{
            config(
                materialized='view'
            )
        }}}}
        
        SELECT
            ingestion_hash,
            ingestion_timestamp,
            idx,
            {selects_sql}
        {from_clause}
        """
        return sql

    def validate_source(self, table_name: str) -> bool:
        """Validate connection to source table."""
        try:
            sql = f'SELECT 1 FROM "{table_name}" LIMIT 1'
            self.execute_query(sql)
            print("Successfully Connected To Source DB", file=sys.stderr)
            return True
        except Exception as e:
            print(f"ERROR - {e}", file=sys.stderr)
            return False

    def check_column_is_json(
        self,
        table_name: str,
        column_name: str,
    ) -> bool:
        """Check if a column contains JSON data."""
        query = f"""
            SELECT JSON_PARSE({column_name}) IS NOT NULL
            FROM "{table_name}"
            WHERE {column_name} IS NOT NULL
            LIMIT 1
        """
        try:
            result_df = self.execute_query(query)
            if not result_df.empty and result_df.iloc[0, 0] == True:
                return True
        except Exception:
            return False
        return False

    def get_table_columns(self, table_name: str) -> List[str]:
        """Get all column names from a table."""
        query = f'SELECT * FROM "{table_name}" LIMIT 1'
        df = self.execute_query(query)
        return df.columns.tolist()

    def get_json_column_mapping(self, table_name: str) -> str:
        """Generate SQL that formats table row as single JSON object."""
        columns = self.get_table_columns(table_name)

        # Single JSON column case
        if len(columns) == 1 and self.check_column_is_json(table_name, columns[0]):
            return f'SELECT JSON_PARSE({columns[0]}) AS "root" FROM "{table_name}" LIMIT 10'

        # Multi-column case: wrap in JSON object
        construct_parts = []
        for col in columns:
            if self.check_column_is_json(table_name, col):
                construct_parts.append(f"'{col}', JSON_PARSE({col})")
            else:
                construct_parts.append(f"'{col}', {col}")

        # Redshift uses JSON_SERIALIZE to build objects
        sql = f'SELECT JSON_SERIALIZE(OBJECT({", ".join(construct_parts)})) AS "root" FROM "{table_name}" LIMIT 10'
        return sql

    def get_root_table_sql(
        self,
        table_name: str,
        limit: Optional[int] = None,
    ) -> str:
        """Generate root table SQL."""
        sql = self.get_json_column_mapping(table_name)
        if limit:
            sql = sql.replace("LIMIT 10", f"LIMIT {limit}")
        return sql

    def get_rows_processed_sql(
        self, project: str, dataset: str, table: str, timestamp: str
    ) -> str:
        """Generate SQL to count rows processed."""
        sql = f"""
            SELECT COUNT(*) as rows_processed
            FROM "{project}"."{dataset}"."{table}"
            WHERE ingestion_timestamp >= '{timestamp}'
        """
        return sql

    def generate_rollup_sql(
        self,
        metadata_list: List[Dict[str, Any]],
        target_dataset: str,
        model_prefix: str = "",
    ) -> str:
        """
        Generate dbt rollup SQL for Redshift.

        Key translations from BigQuery:
        - STRUCT → JSON object literals
        - ARRAY_AGG → ARRAY_AGG (same!)
        - SPLIT(...)[OFFSET(n)] → SPLIT_PART(..., delimiter, n+1)
        - Backticks → Double quotes
        """
        # Organize metadata
        models = {m["model_name"]: m for m in metadata_list}

        # Group by depth
        depth_groups = {}
        max_depth = 0
        for m in metadata_list:
            d = m["depth"]
            if d not in depth_groups:
                depth_groups[d] = []
            depth_groups[d].append(m)
            if d > max_depth:
                max_depth = d

        # Generate CTEs
        ctes = []

        for depth in range(max_depth, 0, -1):
            if depth not in depth_groups:
                continue

            for model in depth_groups[depth]:
                model_name = model["model_name"]

                # Build joins
                joins = []
                for child in model["children"]:
                    child_model_name = f"{model_name}__{child['model_suffix']}"
                    if child_model_name not in models:
                        continue

                    child_cte_name = f"{child_model_name}_agg"
                    join_conditions = []
                    for i in range(depth + 1):
                        # Redshift: SPLIT_PART is 1-indexed
                        join_conditions.append(
                            f"SPLIT_PART(t.idx, '_', {i+1}) = SPLIT_PART({child_cte_name}.idx, '_', {i+1})"
                        )

                    joins.append(
                        f"LEFT JOIN {child_cte_name} ON t.ingestion_hash = {child_cte_name}.ingestion_hash AND {' AND '.join(join_conditions)}"
                    )

                # Build JSON object fields
                json_fields = []

                # Helper to extract field name from either string or dict format
                def get_field_name(field):
                    if isinstance(field, dict):
                        return field.get("name", str(field))
                    return str(field)

                for field in model["scalar_fields"]:
                    field_name = get_field_name(field)
                    json_fields.append(f"'{field_name}', t.\"{field_name}\"")

                for child in model["children"]:
                    child_model_name = f"{model_name}__{child['model_suffix']}"
                    if child_model_name in models:
                        child_cte_name = f"{child_model_name}_agg"
                        if child["type"] == "ARRAY":
                            json_fields.append(
                                f"'{child['field_name']}', ARRAY_AGG({child_cte_name}.\"{child['field_name']}_struct\")"
                            )
                        else:
                            json_fields.append(
                                f"'{child['field_name']}', MAX({child_cte_name}.\"{child['field_name']}_struct\")"
                            )

                # Extract field names for GROUP BY clause
                scalar_field_names = [get_field_name(f) for f in model["scalar_fields"]]
                group_by_suffix = (
                    ", " + ", ".join([f't."{f}"' for f in scalar_field_names])
                    if scalar_field_names
                    else ""
                )

                cte_sql = f"""{model_name}_agg AS (
    SELECT 
        t.ingestion_hash,
        t.idx,
        JSON_SERIALIZE(OBJECT({', '.join(json_fields)})) as "{model['field_name']}_struct"
    FROM {{{{ ref('{model_prefix}{model_name}') }}}} t
    {' '.join(joins)}
    GROUP BY t.ingestion_hash, t.idx{group_by_suffix}
)"""
                ctes.append(cte_sql)

        # Final root SELECT
        root_model = [m for m in metadata_list if m["depth"] == 0]
        if not root_model:
            return ""

        root = root_model[0]
        root_joins = []
        root_json_fields = []

        for child in root["children"]:
            child_model_name = f"{root['model_name']}__{child['model_suffix']}"
            if child_model_name not in models:
                continue

            child_cte_name = f"{child_model_name}_agg"
            join_conditions = [
                f"SPLIT_PART(r.idx, '_', 1) = SPLIT_PART({child_cte_name}.idx, '_', 1)"
            ]

            root_joins.append(
                f"LEFT JOIN {child_cte_name} ON r.ingestion_hash = {child_cte_name}.ingestion_hash AND {' AND '.join(join_conditions)}"
            )

            if child["type"] == "ARRAY":
                root_json_fields.append(
                    f"'{child['field_name']}', ARRAY_AGG({child_cte_name}.\"{child['field_name']}_struct\")"
                )
            else:
                root_json_fields.append(
                    f"'{child['field_name']}', MAX({child_cte_name}.\"{child['field_name']}_struct\")"
                )

        config_block = """
{{
    config(
        materialized='view'
    )
}}
"""

        final_sql = f"""{config_block}

WITH 
{', '.join(ctes)}

SELECT
    r.ingestion_hash,
    r.ingestion_timestamp,
    r.idx,
    JSON_SERIALIZE(OBJECT({', '.join(root_json_fields)})) as "{root['model_name']}"
FROM {{{{ ref('{model_prefix}{root['model_name']}') }}}} r
{' '.join(root_joins)}
GROUP BY r.ingestion_hash, r.ingestion_timestamp, r.idx
"""
        return final_sql

    def clean_dataset(self, dataset: str) -> bool:
        """Drop all tables and views in target schema."""
        try:
            # Get list of objects
            get_object_sql = f"""
                SELECT 
                    'DROP ' || CASE WHEN table_type = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END || ' "' || table_schema || '"."' || table_name || '";' as _drop 
                FROM information_schema.tables 
                WHERE table_schema = '{dataset}' 
                AND table_type IN ('BASE TABLE', 'VIEW')
            """

            object_deletes_df = self.execute_query(get_object_sql)

            if object_deletes_df.empty:
                return True

            # Execute drops
            cursor = self.connection.cursor()
            for i in range(len(object_deletes_df)):
                drop_sql = object_deletes_df.iloc[i, 0]
                try:
                    cursor.execute(drop_sql)
                except Exception as e:
                    print(f"Error dropping: {e}", file=sys.stderr)
                    return False
            cursor.close()
            self.connection.commit()

            return True
        except Exception as e:
            print(f"Error cleaning dataset {dataset}: {e}", file=sys.stderr)
            return False
