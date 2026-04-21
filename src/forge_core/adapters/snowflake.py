import pandas as pd
from snowflake.connector import connect
from .base import WarehouseAdapter
from typing import List, Dict, Any, Optional
import os
import sys

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class SnowflakeAdapter(WarehouseAdapter):
    """
    Snowflake implementation of the WarehouseAdapter.
    Translates BigQuery SQL patterns to Snowflake dialect.
    """

    def __init__(
        self,
        account: str = None,
        user: str = None,
        warehouse: str = None,
        database: str = None,
        schema: str = None,
        role: str = None,
        private_key_path: str = None,
    ):
        """
        Initialize Snowflake connection.

        Args can be provided explicitly or via environment variables:
        - SNOWFLAKE_ACCOUNT
        - SNOWFLAKE_USER
        - SNOWFLAKE_WAREHOUSE
        - SNOWFLAKE_DATABASE
        - SNOWFLAKE_SCHEMA
        - SNOWFLAKE_ROLE
        - SNOWFLAKE_PRIVATE_KEY_PATH (path to PEM private key file)
        """
        self.account = account or os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = user or os.getenv("SNOWFLAKE_USER")
        self.warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
        self.database = database or os.getenv("SNOWFLAKE_DATABASE")
        self.schema = schema or os.getenv("SNOWFLAKE_SCHEMA")
        self.role = role or os.getenv("SNOWFLAKE_ROLE")
        self.private_key_path = private_key_path or os.getenv(
            "SNOWFLAKE_PRIVATE_KEY_PATH"
        )

        self.connection = None
        self._initialize_connection()

    def _initialize_connection(self):
        """Initialize Snowflake connection."""
        try:
            connect_args = {
                "account": self.account,
                "user": self.user,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema,
                "role": self.role,
            }

            # Handle Private Key Auth
            if self.private_key_path and os.path.exists(self.private_key_path):
                try:
                    with open(self.private_key_path, "rb") as key:
                        p_key = serialization.load_pem_private_key(
                            key.read(), password=None, backend=default_backend()
                        )

                    pkb = p_key.private_bytes(
                        encoding=serialization.Encoding.DER,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption(),
                    )
                    connect_args["private_key"] = pkb
                    print("✓ Loaded Snowflake Private Key", file=sys.stderr)
                except Exception as e:
                    print(
                        f"Error: Failed to load private key from {self.private_key_path}: {e}",
                        file=sys.stderr,
                    )
                    # No fallback to password allowed
                    raise e
            else:
                print(
                    f"Warning: Private key not found at {self.private_key_path}",
                    file=sys.stderr,
                )

            self.connection = connect(**connect_args)

            print(
                f"✓ Connected to Snowflake: {self.database}.{self.schema}",
                file=sys.stderr,
            )
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}", file=sys.stderr)

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        if not sql or sql.isspace():
            return pd.DataFrame()

        if not self.connection:
            self._initialize_connection()

        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)

            # Fetch results
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            cursor.close()
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            print(f"Query execution error: {e}", file=sys.stderr)
            print(f"SQL: {sql}", file=sys.stderr)
            raise

    def _read_template(self, template_name: str) -> str:
        """Read SQL template from templates/snowflake directory."""
        try:
            base_path = os.path.dirname(__file__)
            template_path = os.path.join(
                base_path, "templates", "snowflake", template_name
            )
            with open(template_path, "r") as f:
                return f.read()
        except FileNotFoundError:
            print(
                f"Error: Template {template_name} not found at {template_path}",
                file=sys.stderr,
            )
            return ""

    def get_keys(
        self, table_name: str, field_name: str, is_array: bool
    ) -> pd.DataFrame:
        """Get JSON keys from a field."""
        import json

        template = "get_keys_array.sql" if is_array else "get_keys_array.sql"
        sql = self._read_template(template)
        sql = sql.replace("~JSON_FIELD~", field_name)
        sql = sql.replace("~TABLE_NAME~", table_name)
        df = self.execute_query(sql)

        # Ensure the output is a list, not a string representation
        if not df.empty and isinstance(df.iloc[0, 0], str):
            try:
                df.iloc[0, 0] = json.loads(df.iloc[0, 0])
            except Exception:
                pass
        return df

    def get_types_sql(
        self, table_name: str, field_name: str, key: str, is_array: bool
    ) -> str:
        """Generate SQL to get field types."""
        template = "get_types_array.sql" if is_array else "get_types_array.sql"
        sql = self._read_template(template)
        sql = sql.replace("~JSON_FIELD~", field_name)
        sql = sql.replace("~KEY~", key)
        sql = sql.replace("~TABLE_NAME~", table_name)
        return sql

    def build_select_expression(
        self, field_name: str, safe_field: str, clean_field_name: str, field_type: str
    ) -> str:
        """
        Generate Snowflake SQL for JSON field extraction.

        Translates BigQuery patterns:
        - JSON_QUERY → GET_PATH or :notation
        - JSON_EXTRACT_SCALAR → GET with cast
        - SAFE.PARSE_JSON → TRY_PARSE_JSON
        """
        # Use Snowflake's colon notation for cleaner syntax
        # field_name:path.to.field is equivalent to GET_PATH(field_name, 'path.to.field')

        if field_type == "array":
            return f"""
            COALESCE(
                IFF(
                    TO_JSON(
                        TRY_PARSE_JSON(REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON(GET_PATH("{field_name}".VALUE, '{safe_field}')), '^\\\\[\\\\[+', '['), '\\\\]\\\\]+$', ']'))
                    ) = 'null',
                    null,
                    REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON(GET_PATH("{field_name}".VALUE, '{safe_field}')), '^\\\\[\\\\[+', '['), '\\\\]\\\\]+$', ']')
                ),
                IFF(
                    TO_JSON(
                        TRY_PARSE_JSON(REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON(GET_PATH("{field_name}".VALUE, '{safe_field}')), '^\\\\[\\\\[+', '['), '\\\\]\\\\]+$', ']'))
                    ) = 'null',
                    null,
                    REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON(GET_PATH("{field_name}".VALUE, '{safe_field}')), '^\\\\[\\\\[+', '['), '\\\\]\\\\]+$', ']')
                )
            ) AS "{clean_field_name}"
            """
        elif field_type == "object":
            return f"""
            REGEXP_REPLACE(REGEXP_REPLACE('[' ||
            COALESCE(
                IFF(
                    TO_JSON(
                        GET_PATH("{field_name}".VALUE, '{safe_field}')
                    ) = 'null',
                    null,
                    TO_JSON(GET_PATH("{field_name}".VALUE, '{safe_field}'))
                ),
                IFF(
                    TO_JSON(
                        GET_PATH("{field_name}".VALUE, '{safe_field}')
                    ) = 'null',
                    null,
                    TO_JSON(GET_PATH("{field_name}".VALUE, '{safe_field}'))
                )
            ) || ']', '^\\\\[\\\\[+', '['), '\\\\]\\\\]+$', ']') AS "{clean_field_name}"
            """
        else:
            # Scalar field
            return f"""GET_PATH("{field_name}".VALUE, '{safe_field}')::VARCHAR AS "{clean_field_name}" """

    def get_create_table_sql(
        self,
        table_name: str,
        field_name: str,
        selects_sql: str,
        is_array: bool,
        table_path: str,
    ) -> str:
        """Generate CREATE TABLE SQL for Snowflake."""
        template = (
            "create_table_array.sql"
            if table_name == "frg__root"
            else "create_table_array.sql"
        )
        sql = self._read_template(template)
        sql = sql.replace("~TABLE_NAME~", table_name)
        sql = sql.replace("~JSON_FIELD~", field_name)
        sql = sql.replace("~DBT_SELECT~", selects_sql)
        sql = sql.replace("~TABLE_PATH~", table_path)
        return sql

    def validate_source(self, table_name: str) -> bool:
        """Validate connection to source table."""
        try:
            sql = f"SELECT 1 FROM {table_name} LIMIT 1"
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
        """
        Check if a column contains JSON data.
        In Snowflake, check if column is VARIANT type or can be parsed as JSON.
        """
        query = f"""
            SELECT TRY_PARSE_JSON({column_name}::VARCHAR) IS NOT NULL
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            LIMIT 1
        """
        try:
            result_df = self.execute_query(query)
            if not result_df.empty and result_df.iloc[0, 0] == True:
                return True
        except Exception as e:
            return False
        return False

    def get_table_columns(self, table_name: str) -> List[str]:
        """Get all column names from a table."""
        query = f"SELECT * FROM {table_name} LIMIT 1"
        df = self.execute_query(query)
        return df.columns.tolist()

    def get_json_column_mapping(self, table_name: str) -> str:
        """
        Generate SQL that formats table row as single JSON object.
        Translates BigQuery's JSON_OBJECT to Snowflake's OBJECT_CONSTRUCT.
        """
        columns = self.get_table_columns(table_name)

        # Single JSON column case
        if len(columns) == 1 and self.check_column_is_json(table_name, columns[0]):
            # Check for 'root' key
            keys_query = f"""
                SELECT OBJECT_KEYS(TRY_PARSE_JSON({columns[0]}::VARCHAR))
                FROM {table_name}
                WHERE {columns[0]} IS NOT NULL
                LIMIT 1
            """
            try:
                keys_df = self.execute_query(keys_query)
                if (
                    not keys_df.empty
                    and len(keys_df.iloc[0, 0]) == 1
                    and keys_df.iloc[0, 0][0] == "root"
                ):
                    # Unnest root key
                    return f"SELECT GET_PATH(TRY_PARSE_JSON({columns[0]}::VARCHAR), 'root') AS \"root\" \n\t\tFROM {table_name}\n\t ~LIMITER~"
            except Exception:
                pass

            # Default: use whole object
            return f'SELECT TRY_PARSE_JSON({columns[0]}::VARCHAR) AS "root" \n\t\tFROM {table_name}\n\t ~LIMITER~'

        # Multi-column case: wrap in OBJECT_CONSTRUCT
        mapping = {}
        for col in columns:
            mapping[col] = self.check_column_is_json(table_name, col)

        construct_parts = []
        for column_name, is_json in mapping.items():
            if is_json:
                construct_parts.append(
                    f"'{column_name}', TRY_PARSE_JSON({column_name}::VARCHAR)"
                )
            else:
                construct_parts.append(f"'{column_name}', {column_name}")

        sql = f'SELECT OBJECT_CONSTRUCT( {", ".join(construct_parts)} ) AS "root" \n\t\tFROM {table_name}\n\t ~LIMITER~'

        return sql

    def get_root_table_sql(
        self,
        table_name: str,
        limit: Optional[int] = None,
    ) -> str:
        """Generate root table SQL."""
        template = "create_root_aggregate.sql"
        sql = self._read_template(template)
        sql = sql.replace("~SQL_SELECTS~", self.get_json_column_mapping(table_name))

        if limit is not None:
            sql = sql.replace("~LIMITER~", f"\tLIMIT {limit}")
        else:
            sql = sql.replace("~LIMITER~", "")

        return sql

    def get_rows_processed_sql(
        self, project: str, dataset: str, table: str, timestamp: str
    ) -> str:
        """
        Generate SQL to count rows processed.
        Note: Snowflake doesn't have exact BigQuery equivalent of __TABLES__ metadata.
        This may need adjustment based on Snowflake monitoring setup.
        """
        # Placeholder - Snowflake uses INFORMATION_SCHEMA differently
        sql = f"""
            SELECT COUNT(*) as rows_processed
            FROM "{project}"."{dataset}"."{table.upper()}"
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
        Generate dbt rollup SQL for Snowflake.

        Key translations:
        - STRUCT → OBJECT_CONSTRUCT
        - SPLIT(...)[OFFSET(n)] → SPLIT(...)[n]
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

                # Build joins with children
                joins = []
                for child in model["children"]:
                    child_model_name = f"{model_name}__{child['model_suffix']}"

                    if child_model_name not in models:
                        continue

                    child_cte_name = f"{child_model_name}_agg"

                    join_conditions = []
                    for i in range(depth + 1):
                        join_conditions.append(
                            f"SPLIT(t.idx, '_')[{i}] = SPLIT({child_cte_name}.idx, '_')[{i}]"
                        )

                    joins.append(
                        f"LEFT JOIN {child_cte_name} ON t.ingestion_hash = {child_cte_name}.ingestion_hash AND {' AND '.join(join_conditions)}"
                    )

                # Build OBJECT_CONSTRUCT fields
                construct_parts = []

                # Helper to extract field name from either string or dict format
                def get_field_name(field):
                    if isinstance(field, dict):
                        return field.get("name", str(field))
                    return str(field)

                for field in model["scalar_fields"]:
                    field_name = get_field_name(field)
                    construct_parts.append(f"'{field_name}', t.\"{field_name}\"")

                for child in model["children"]:
                    child_model_name = f"{model_name}__{child['model_suffix']}"
                    if child_model_name in models:
                        child_cte_name = f"{child_model_name}_agg"
                        if child["type"] == "ARRAY":
                            construct_parts.append(
                                f"'{child['field_name']}', ARRAY_AGG({child_cte_name}.\"{child['field_name']}_struct\")"
                            )
                        else:
                            construct_parts.append(
                                f"'{child['field_name']}', ANY_VALUE({child_cte_name}.\"{child['field_name']}_struct\")"
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
        OBJECT_CONSTRUCT(
            {', '.join(construct_parts)}
        ) as "{model['field_name']}_struct"
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
        root_construct_parts = []

        for child in root["children"]:
            child_model_name = f"{root['model_name']}__{child['model_suffix']}"

            if child_model_name not in models:
                continue

            child_cte_name = f"{child_model_name}_agg"

            join_conditions = [
                f"SPLIT(r.idx, '_')[0] = SPLIT({child_cte_name}.idx, '_')[0]"
            ]

            root_joins.append(
                f"LEFT JOIN {child_cte_name} ON r.ingestion_hash = {child_cte_name}.ingestion_hash AND {' AND '.join(join_conditions)}"
            )

            if child["type"] == "ARRAY":
                root_construct_parts.append(
                    f"'{child['field_name']}', ARRAY_AGG({child_cte_name}.\"{child['field_name']}_struct\")"
                )
            else:
                root_construct_parts.append(
                    f"'{child['field_name']}', ANY_VALUE({child_cte_name}.\"{child['field_name']}_struct\")"
                )

        config_block = """
{{
    config(
        materialized='view',
        unique_key=['ingestion_hash','idx']
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
    OBJECT_CONSTRUCT(
        {', '.join(root_construct_parts)}
    ) as "{root['model_name']}"
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
                SELECT CONCAT('DROP ', CASE WHEN TABLE_TYPE = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END, ' "', TABLE_SCHEMA, '"."', TABLE_NAME, '";') as _drop 
                FROM "{self.database}".INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{dataset}' 
                AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            """

            object_deletes_df = self.execute_query(get_object_sql)

            if object_deletes_df.empty:
                return True

            # Execute drops
            for i in range(len(object_deletes_df)):
                drop_sql = object_deletes_df.iloc[i, 0]
                try:
                    self.execute_query(drop_sql)
                except Exception as e:
                    print(f"Error dropping: {e}", file=sys.stderr)
                    return False

            return True
        except Exception as e:
            print(f"Error cleaning dataset {dataset}: {e}", file=sys.stderr)
            return False

    def apply_descriptions_to_table(
        self,
        table_ref: str,
        col_descriptions: Dict[str, str],
        logger=None,
    ) -> bool:
        """
        Apply column descriptions to a single Snowflake table.

        Args:
            table_ref: Fully qualified table reference ("DB"."SCHEMA"."TABLE")
            col_descriptions: Flat dict of field_name -> description
            logger: Optional logger

        Returns:
            True if any descriptions were applied
        """
        if not self.connection or not col_descriptions:
            return False

        try:
            # Try to apply descriptions directly, catching errors for non-existent columns
            updated_any = False
            for col_name, description in col_descriptions.items():
                safe_desc = description.replace("'", "''")
                alter_sql = f"""
                    ALTER TABLE {table_ref}
                    ALTER COLUMN "{col_name.upper()}" COMMENT '{safe_desc}'
                """
                try:
                    self.execute_query(alter_sql)
                    updated_any = True
                except Exception:
                    pass

            if updated_any and logger:
                logger.info(f"✓ Applied descriptions to {table_ref}")
            return updated_any
        except Exception as e:
            if logger:
                logger.warning(f"Failed to apply descriptions to {table_ref}: {e}")
            return False

    def apply_column_descriptions(
        self,
        dataset: str,
        model_descriptions: Dict[str, Dict[str, str]],
        logger=None,
    ) -> int:
        """
        Apply column descriptions to Snowflake tables using ALTER TABLE syntax.

        Args:
            dataset: Target Snowflake schema
            model_descriptions: Dict of model_name -> {column_name: description}
            logger: Optional logger for output

        Returns:
            Number of tables updated
        """
        if not self.connection:
            if logger:
                logger.warning("Snowflake connection not initialized")
            return 0

        tables_updated = 0

        for model_name, col_descriptions in model_descriptions.items():
            try:
                # Check if table exists
                check_sql = f"""
                    SELECT COUNT(*) 
                    FROM "{self.database}".INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = '{dataset}' 
                    AND TABLE_NAME = '{model_name.upper()}'
                """
                result = self.execute_query(check_sql)
                if result.empty or result.iloc[0, 0] == 0:
                    continue

                # Get existing columns
                col_sql = f"""
                    SELECT COLUMN_NAME 
                    FROM "{self.database}".INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = '{dataset}' 
                    AND TABLE_NAME = '{model_name.upper()}'
                """
                cols_df = self.execute_query(col_sql)
                existing_cols = set(cols_df["COLUMN_NAME"].str.lower().tolist())

                # Apply descriptions to matching columns
                updated_any = False
                for col_name, description in col_descriptions.items():
                    if col_name.lower() in existing_cols:
                        # Escape single quotes in description
                        safe_desc = description.replace("'", "''")
                        # Snowflake: ALTER TABLE ... ALTER COLUMN ... COMMENT
                        alter_sql = f"""
                            ALTER TABLE "{self.database}"."{dataset}"."{model_name.upper()}"
                            ALTER COLUMN "{col_name.upper()}" COMMENT '{safe_desc}'
                        """
                        try:
                            self.execute_query(alter_sql)
                            updated_any = True
                        except Exception:
                            pass

                if updated_any:
                    tables_updated += 1
                    if logger:
                        logger.info(f"✓ Applied descriptions to: {model_name}")

            except Exception as e:
                if logger:
                    logger.warning(f"Failed to apply descriptions to {model_name}: {e}")

        return tables_updated
