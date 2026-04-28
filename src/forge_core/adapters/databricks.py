import pandas as pd
from databricks import sql
from .base import WarehouseAdapter
from typing import List, Dict, Any, Optional
import os
import sys


class DatabricksAdapter(WarehouseAdapter):
    """
    Databricks implementation of the WarehouseAdapter.
    Uses Spark SQL and Delta Lake format.
    """

    def __init__(
        self,
        server_hostname: str = None,
        http_path: str = None,
        access_token: str = None,
        client_id: str = None,
        client_secret: str = None,
        catalog: str = None,
        schema: str = None,
    ):
        """
        Initialize Databricks connection.

        Args can be provided explicitly or via environment variables:
        - DATABRICKS_SERVER_HOSTNAME
        - DATABRICKS_HTTP_PATH
        - DATABRICKS_ACCESS_TOKEN (for PAT auth)
        - DATABRICKS_CLIENT_ID (for M2M OAuth)
        - DATABRICKS_CLIENT_SECRET (for M2M OAuth)
        - DATABRICKS_CATALOG (default: main)
        - DATABRICKS_SCHEMA (default: default)

        If client_id and client_secret are provided, M2M OAuth is used.
        Otherwise, access_token (PAT) is used.
        """
        self.server_hostname = server_hostname or os.getenv(
            "DATABRICKS_SERVER_HOSTNAME"
        )
        self.http_path = http_path or os.getenv("DATABRICKS_HTTP_PATH")
        self.access_token = access_token or os.getenv("DATABRICKS_ACCESS_TOKEN")
        self.client_id = client_id or os.getenv("DATABRICKS_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("DATABRICKS_CLIENT_SECRET")
        self.catalog = catalog or os.getenv("DATABRICKS_CATALOG", "main")
        self.schema = schema or os.getenv("DATABRICKS_SCHEMA", "default")

        self.connection = None
        # Connection is lazy-initialized on first use (see execute_query)
        # self._initialize_connection()

    def close(self):
        """Explicitly close connection to prevent GC errors."""
        try:
            if self.connection:
                # Monkey-patch/safeguard against the library's buggy __del__
                # The library checks self.open -> self.session.is_open
                # If self.session is missing, it crashes.
                try:
                    self.connection.close()
                except Exception:
                    pass

                # Forcefully neutralize the object state to prevent __del__ crash
                try:
                    self.connection.open = False
                    self.connection.session = None
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            self.connection = None

    def __del__(self):
        """Destructor to ensure cleanup."""
        self.close()

    def _initialize_connection(self):
        """Initialize Databricks SQL connection."""
        if not self.server_hostname:
            print(
                "Databricks: No server_hostname configured, skipping connection",
                file=sys.stderr,
            )
            return
        try:
            # Use M2M OAuth if client_id and client_secret are provided
            if self.client_id and self.client_secret:
                from databricks.sdk.core import Config, oauth_service_principal

                # Create Config with service principal credentials
                cfg = Config(
                    host=f"https://{self.server_hostname}",
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                )

                # Get the credentials provider from config
                oauth_provider = oauth_service_principal(cfg)

                # Get headers dict and extract access token
                headers = oauth_provider()  # Returns {'Authorization': 'Bearer xxx'}
                auth_header = headers.get("Authorization", "")
                access_token = (
                    auth_header.replace("Bearer ", "") if auth_header else None
                )

                if not access_token:
                    raise ValueError("Failed to obtain OAuth access token")

                self.connection = sql.connect(
                    server_hostname=self.server_hostname,
                    http_path=self.http_path,
                    access_token=access_token,
                    _enable_telemetry=False,
                )
            else:
                # Fallback to access token (PAT)
                self.connection = sql.connect(
                    server_hostname=self.server_hostname,
                    http_path=self.http_path,
                    access_token=self.access_token,
                    _enable_telemetry=False,
                )
            print(
                f"✓ Connected to Databricks: {self.catalog}.{self.schema}",
                file=sys.stderr,
            )
        except Exception as e:
            print(f"Error connecting to Databricks: {e}", file=sys.stderr)
            # Ensure connection is None on failure
            self.connection = None

    def _ensure_connection(self):
        """Lazy initialization of the connection."""
        if not self.connection:
            print(
                "DEBUG: Initializing Databricks connection (lazy)...", file=sys.stderr
            )
            self._initialize_connection()

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        if not sql or sql.isspace():
            return pd.DataFrame()

        self._ensure_connection()

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
        """Read a SQL template file from the templates/databricks directory."""
        try:
            base_path = os.path.dirname(__file__)
            template_path = os.path.join(
                base_path, "templates", "databricks", template_name
            )
            with open(template_path, "r") as f:
                return f.read()
        except FileNotFoundError:
            print(
                f"Error: Template {template_name} not found at {template_path}",
                file=sys.stderr,
            )
            return ""

    @staticmethod
    def _safe_jsonpath(key: str) -> str:
        """Escape a field name for Spark JSONPath bracket notation.

        Spark's get_json_object uses single-quoted JSONPath strings,
        so we use bracket notation with single-quote escaping.

        Examples:
            "name"                    -> "$['name']"
            "pseudo_802.1ad_enabled"  -> "$['pseudo_802.1ad_enabled']"
            "field'name"              -> "$['field\\'name']"
        """
        escaped = key.replace("\\", "\\\\").replace("'", "\\'")
        return f"$['{escaped}']"

    def build_select_expression(
        self, field_name: str, safe_field: str, clean_field_name: str, field_type: str
    ) -> str:
        """
        Generate Databricks Spark SQL for JSON field extraction.

        All fields are stored as STRING per design. Uses get_json_object()
        for extraction. Avoids to_json() which doesn't work on STRING type.
        """
        # Use bracket notation for JSONPath to handle dots, brackets, quotes
        jp = self._safe_jsonpath(safe_field)
        extract_expr = f"get_json_object(`{field_name}`, '{jp}')"

        if field_type == "array":
            # Arrays: extract as STRING, let downstream handle parsing
            return f"""
            CASE 
                WHEN {extract_expr} IS NULL OR {extract_expr} = 'null'
                THEN NULL
                ELSE CAST({extract_expr} AS STRING)
            END AS `{clean_field_name}`
            """
        elif field_type == "object":
            # Objects: wrap in array brackets for consistent EXPLODE later
            # Per user requirement: all objects stored as arrays
            return f"""
            CASE 
                WHEN {extract_expr} IS NULL OR {extract_expr} = 'null'
                THEN NULL
                ELSE CONCAT('[', CAST({extract_expr} AS STRING), ']')
            END AS `{clean_field_name}`
            """
        else:
            # Scalar field - cast to STRING for type safety
            return f"""CAST({extract_expr} AS STRING) AS `{clean_field_name}`"""

    def get_keys(
        self, table_name: str, field_name: str, is_array: bool
    ) -> pd.DataFrame:
        """Get JSON keys from a field.

        Since all fields are stored as STRING (JSON strings),
        we can use schema_of_json() directly.

        Returns DataFrame with 'keys' column containing array of key names,
        matching the format expected by forge_engine.

        Handles:
        - STRUCT<...> - extracts field names
        - ARRAY<...> - returns empty (no named fields to extract)
        - STRING, BIGINT, etc. - returns empty (scalar types)
        """

        # Get DISTINCT schemas from ALL rows (no LIMIT)
        # This captures keys that may only exist in some rows
        sql = f"""
            SELECT DISTINCT schema_of_json({field_name}) as schema
            FROM {table_name}
            WHERE {field_name} IS NOT NULL 
              AND {field_name} != '[]'
              AND LENGTH({field_name}) > 10
        """
        df = self.execute_query(sql)

        if df.empty:
            return pd.DataFrame({"keys": [[]]})

        # Collect all unique keys from all distinct schemas
        all_keys = set()

        for _, row in df.iterrows():
            schema_string = str(row["schema"])

            # Handle ARRAY<STRUCT<...>> - extract inner STRUCT keys
            if schema_string.startswith("ARRAY<STRUCT<"):
                schema_string = schema_string[6:-1]

            # Handle ARRAY of primitives - skip
            if schema_string.startswith("ARRAY<"):
                continue

            # Only parse STRUCT schemas
            if not schema_string.startswith("STRUCT<"):
                continue

            # Parse STRUCT<field1: TYPE, ...> - remove outer wrapper
            inner = schema_string[7:-1]

            # Parse field names (handle nested STRUCTs)
            current_field = ""
            depth = 0

            for char in inner:
                if char == "<":
                    depth += 1
                    current_field += char
                elif char == ">":
                    depth -= 1
                    current_field += char
                elif char == ":" and depth == 0:
                    all_keys.add(current_field.strip())
                    current_field = ""
                elif char == "," and depth == 0:
                    current_field = ""
                else:
                    current_field += char

        # Return DataFrame with all unique keys
        return pd.DataFrame({"keys": [list(all_keys)]})

    def get_types_sql(
        self, table_name: str, field_name: str, key: str, is_array: bool
    ) -> str:
        """Generate SQL to get field types.

        Wrapped in subquery so LIMIT works with UNION DISTINCT.
        For arrays, uses $[0] to access first element of the array.

        Filters for rows where the specific key exists to get accurate
        type detection for sparse fields.

        Always uses $[0] path since root/parent fields are stored as arrays.
        """
        # Use bracket notation for the key to handle dots and special chars
        escaped = key.replace("\\", "\\\\").replace("'", "\\'")
        path = f"$[0]['{escaped}']"

        sql = f"""
            SELECT * FROM (
                SELECT
                    '{key}' as field,
                    CASE 
                        WHEN get_json_object({field_name}, '{path}') LIKE '{{%' THEN 'object'
                        WHEN get_json_object({field_name}, '{path}') LIKE '[%' THEN 'array'
                        ELSE 'scalar'
                    END as type
                FROM {table_name}
                WHERE {field_name} IS NOT NULL 
                  AND LENGTH({field_name}) > 10
                  AND get_json_object({field_name}, '{path}') IS NOT NULL
                LIMIT 1
            )
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
        """Generate CREATE TABLE SQL for Databricks Delta Lake using templates.

        Per design: ALL objects are stored as arrays, even single objects.
        Uses LATERAL VIEW EXPLODE for consistency.
        """
        # Read the appropriate template
        template = self._read_template("create_table.sql")

        if not template:
            # Fallback if template not found
            print("Warning: Template not found, using fallback SQL", file=sys.stderr)
            return ""

        # Replace placeholders in template
        sql = template.replace("~TABLE_NAME~", table_name)
        sql = sql.replace("~JSON_FIELD~", field_name)
        sql = sql.replace("~DBT_SELECT~", selects_sql)

        return sql

    def get_root_model_sql(self, source_table: str, json_field: str) -> str:
        """Generate root model SQL for Databricks using create_root.sql template.

        Creates an incremental Delta Lake table with:
        - idx: Row number within each unique ingestion_hash
        - ingestion_hash: MD5 hash of the JSON data
        - ingestion_timestamp: Current timestamp
        - table_path: 'frg__root'
        - The original JSON field as a STRING column
        """
        template = self._read_template("create_root.sql")

        if not template:
            print("Warning: create_root.sql template not found", file=sys.stderr)
            return ""

        # Replace placeholders
        sql = template.replace("~TABLE_NAME~", source_table)
        sql = sql.replace("~JSON_FIELD~", json_field)

        return sql

    def validate_source(self, table_name: str) -> bool:
        """Validate connection to source table."""
        try:
            # Databricks uses unquoted catalog.schema.table format
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
        """Check if a column contains JSON data."""
        query = f"""
            SELECT from_json({column_name}, 'string') IS NOT NULL
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            LIMIT 1
        """
        try:
            result_df = self.execute_query(query)
            if not result_df.empty and result_df.iloc[0, 0]:
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
        """Generate SQL that formats table row as single JSON STRING.

        Per design: All fields are stored as STRING (JSON strings).
        This ensures consistent processing where interpreter parses at each loop.
        """
        columns = self.get_table_columns(table_name)

        # Single JSON column case - keep as string
        # Single JSON column case - keep as string
        if len(columns) == 1:
            col = columns[0]
            # Check if the single column wraps a 'root' object
            try:
                keys_df = self.get_keys(table_name, col, is_array=False)
                keys = keys_df.iloc[0, 0] if not keys_df.empty else []
                if len(keys) == 1 and keys[0] == "root":
                    # Unnest the root object
                    return f"SELECT get_json_object({col}, '$.root') AS `root` FROM {table_name} LIMIT 10"
            except Exception as e:
                print(f"Warning: Failed to check for root key: {e}", file=sys.stderr)

            # Return the column as-is (as a STRING), aliased as root
            return f"SELECT CAST({col} AS STRING) AS `root` FROM {table_name} LIMIT 10"

        # Multi-column case: build JSON object as STRING using to_json(named_struct(...))
        struct_parts = []
        for col in columns:
            struct_parts.append(f"'{col}', {col}")

        # Use to_json() to convert struct to JSON STRING
        sql = f'SELECT to_json(named_struct({", ".join(struct_parts)})) AS `root` FROM {table_name} LIMIT 10'
        return sql

    def get_root_table_sql(
        self,
        table_name: str,
        limit: Optional[int] = None,
    ) -> str:
        """Generate root table SQL using create_root.sql template.

        Creates an incremental Delta Lake table with:
        - idx: Row number within each unique ingestion_hash
        - ingestion_hash: MD5 hash of the JSON data
        - ingestion_timestamp: Current timestamp
        - table_path: 'frg__root'
        - root: The original JSON field as STRING
        """
        # Read the template
        template = self._read_template("create_root.sql")

        if not template:
            print(
                "Warning: create_root.sql template not found, using fallback",
                file=sys.stderr,
            )
            # Fallback to simple view
            sql = self.get_json_column_mapping(table_name)
            if limit:
                sql = sql.replace("LIMIT 10", f"LIMIT {limit}")
            return sql

        # Get the JSON column name (first/only column that's JSON)
        columns = self.get_table_columns(table_name)
        json_field = columns[0] if columns else "data"

        # Replace placeholders - use alias 'root' as that's what downstream expects
        sql = template.replace("~TABLE_NAME~", table_name)
        sql = sql.replace("~JSON_FIELD~", json_field)

        # Add limit if specified
        if limit:
            # Insert limit before the closing of the subquery
            sql = sql.replace(
                "WHERE `" + json_field + "` IS NOT NULL",
                f"WHERE `{json_field}` IS NOT NULL LIMIT {limit}",
            )

        return sql

    def get_rows_processed_sql(
        self, project: str, dataset: str, table: str, timestamp: str
    ) -> str:
        """Generate SQL to count rows processed.

        For Databricks, uses unquoted catalog.schema.table format.
        Note: Databricks views may not have ingestion_timestamp, so we count all rows.
        """
        # Databricks uses catalog.schema.table (project=catalog, dataset=schema)
        sql = f"""
            SELECT COUNT(*) AS rows_processed
            FROM {project}.{dataset}.{table}
        """
        return sql

    def generate_rollup_sql(
        self,
        metadata_list: List[Dict[str, Any]],
        target_dataset: str,
        model_prefix: str = "",
    ) -> str:
        """
        Generate dbt rollup SQL for Databricks.

        Key translations:
        - STRUCT → named_struct
        - ARRAY_AGG → collect_list
        - SPLIT(...)[OFFSET(n)] → split(...)[n]
        - Backticks preserved for Spark SQL
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
                        # Databricks: split is 0-indexed like arrays
                        join_conditions.append(
                            f"split(t.idx, '_')[{i}] = split({child_cte_name}.idx, '_')[{i}]"
                        )

                    joins.append(
                        f"LEFT JOIN {child_cte_name} ON t.ingestion_hash = {child_cte_name}.ingestion_hash AND {' AND '.join(join_conditions)}"
                    )

                # Build named_struct fields
                struct_parts = []

                # Helper to extract field name from either string or dict format
                def get_field_name(field):
                    if isinstance(field, dict):
                        return field.get("name", str(field))
                    return str(field)

                for field in model["scalar_fields"]:
                    field_name = get_field_name(field)
                    struct_parts.append(f"'{field_name}', t.`{field_name}`")

                for child in model["children"]:
                    child_model_name = f"{model_name}__{child['model_suffix']}"
                    if child_model_name in models:
                        child_cte_name = f"{child_model_name}_agg"
                        if child["type"] == "ARRAY":
                            struct_parts.append(
                                f"'{child['field_name']}', collect_list({child_cte_name}.`{child['field_name']}_struct`)"
                            )
                        else:
                            struct_parts.append(
                                f"'{child['field_name']}', first({child_cte_name}.`{child['field_name']}_struct`)"
                            )

                # Extract field names for GROUP BY clause
                scalar_field_names = [get_field_name(f) for f in model["scalar_fields"]]
                group_by_suffix = (
                    ", " + ", ".join([f"t.`{f}`" for f in scalar_field_names])
                    if scalar_field_names
                    else ""
                )

                cte_sql = f"""{model_name}_agg AS (
    SELECT 
        t.ingestion_hash,
        t.idx,
        named_struct({', '.join(struct_parts)}) as `{model['field_name']}_struct`
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
        root_struct_parts = []

        for child in root["children"]:
            child_model_name = f"{root['model_name']}__{child['model_suffix']}"
            if child_model_name not in models:
                continue

            child_cte_name = f"{child_model_name}_agg"
            join_conditions = [
                f"split(r.idx, '_')[0] = split({child_cte_name}.idx, '_')[0]"
            ]

            root_joins.append(
                f"LEFT JOIN {child_cte_name} ON r.ingestion_hash = {child_cte_name}.ingestion_hash AND {' AND '.join(join_conditions)}"
            )

            if child["type"] == "ARRAY":
                root_struct_parts.append(
                    f"'{child['field_name']}', collect_list({child_cte_name}.`{child['field_name']}_struct`)"
                )
            else:
                root_struct_parts.append(
                    f"'{child['field_name']}', first({child_cte_name}.`{child['field_name']}_struct`)"
                )

        config_block = """
{{
    config(
        materialized='view',
        file_format='delta'
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
    named_struct({', '.join(root_struct_parts)}) as `{root['model_name']}`
FROM {{{{ ref('{model_prefix}{root['model_name']}') }}}} r
{' '.join(root_joins)}
GROUP BY r.ingestion_hash, r.ingestion_timestamp, r.idx
"""

        return final_sql

    def clean_dataset(self, dataset: str) -> bool:
        """Drop all tables and views using Databricks SDK (not SQL).

        Uses the Databricks workspace client API instead of INFORMATION_SCHEMA
        to avoid SQL query hanging issues.
        """
        try:
            print(
                f"🔧 Starting cleanup using Databricks SDK: {self.catalog}.{dataset}",
                file=sys.stderr,
            )

            # Use Databricks SDK instead of SQL queries
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.core import Config

            # Create workspace client using same credentials
            cfg = Config(
                host=f"https://{self.server_hostname}",
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            w = WorkspaceClient(config=cfg)

            # List tables using SDK
            print("📊 Listing tables via SDK API...", file=sys.stderr)
            tables = list(w.tables.list(catalog_name=self.catalog, schema_name=dataset))

            if not tables:
                print(f"✓ No tables found in {self.catalog}.{dataset}", file=sys.stderr)
                return True

            print(f"🗑️  Found {len(tables)} tables to drop", file=sys.stderr)

            # Drop each table using SQL (SDK doesn't have drop method)
            self._ensure_connection()
            if self.connection:
                cursor = self.connection.cursor()
            else:
                print("❌ Failed to establish connection for cleaning", file=sys.stderr)
                return False

            dropped_count = 0
            for table in tables:
                table_name = table.name
                table_type = str(table.table_type).upper()

                print(f"  Dropping {table_type}: {table_name}", file=sys.stderr)
                try:
                    # Use appropriate DROP command based on type
                    if "VIEW" in table_type:
                        drop_sql = f"DROP VIEW IF EXISTS `{self.catalog}`.`{dataset}`.`{table_name}`"
                    else:
                        drop_sql = f"DROP TABLE IF EXISTS `{self.catalog}`.`{dataset}`.`{table_name}`"

                    cursor.execute(drop_sql)
                    dropped_count += 1
                except Exception as e:
                    print(f"  ❌ Error dropping {table_name}: {e}", file=sys.stderr)
                    # Don't fail completely - continue dropping other tables
                    continue
            cursor.close()

            print(
                f"✅ Successfully cleaned {dropped_count} tables from {self.catalog}.{dataset}",
                file=sys.stderr,
            )
            return True

        except Exception as e:
            print(f"❌ Error cleaning dataset {dataset}: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
            # If schema doesn't exist, that's OK
            if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                print("ℹ️  Schema does not exist, nothing to clean", file=sys.stderr)
                return True
            return False

    def apply_descriptions_to_table(
        self,
        table_ref: str,
        col_descriptions: Dict[str, str],
        logger=None,
    ) -> bool:
        """
        Apply column descriptions to a single Databricks table.

        Args:
            table_ref: Fully qualified table reference (catalog.schema.table)
            col_descriptions: Flat dict of field_name -> description
            logger: Optional logger

        Returns:
            True if any descriptions were applied
        """
        self._ensure_connection()
        if not self.connection or not col_descriptions:
            return False

        try:
            updated_any = False
            cursor = self.connection.cursor()

            for col_name, description in col_descriptions.items():
                safe_desc = description.replace("'", "''")
                alter_sql = f"""
                    ALTER TABLE {table_ref}
                    ALTER COLUMN `{col_name}` COMMENT '{safe_desc}'
                """
                try:
                    cursor.execute(alter_sql)
                    updated_any = True
                except Exception:
                    pass

            cursor.close()

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
        Apply column descriptions to Databricks tables using ALTER TABLE syntax.

        Args:
            dataset: Target Databricks schema
            model_descriptions: Dict of model_name -> {column_name: description}
            logger: Optional logger for output

        Returns:
            Number of tables updated
        """
        self._ensure_connection()
        if not self.connection:
            if logger:
                logger.warning("Databricks connection not initialized")
            return 0

        tables_updated = 0

        for model_name, col_descriptions in model_descriptions.items():
            try:
                # Check if table exists using SDK
                try:
                    from databricks.sdk import WorkspaceClient
                    from databricks.sdk.core import Config

                    cfg = Config(
                        host=f"https://{self.server_hostname}",
                        client_id=self.client_id,
                        client_secret=self.client_secret,
                    )
                    w = WorkspaceClient(config=cfg)

                    # Try to get table - will throw if not exists
                    table = w.tables.get(f"{self.catalog}.{dataset}.{model_name}")
                    existing_cols = set(c.name.lower() for c in table.columns)
                except Exception:
                    continue

                # Apply descriptions to matching columns
                updated_any = False
                cursor = self.connection.cursor()

                for col_name, description in col_descriptions.items():
                    if col_name.lower() in existing_cols:
                        # Escape single quotes in description
                        safe_desc = description.replace("'", "''")
                        # Databricks: ALTER TABLE ... ALTER COLUMN ... COMMENT
                        alter_sql = f"""
                            ALTER TABLE `{self.catalog}`.`{dataset}`.`{model_name}`
                            ALTER COLUMN `{col_name}` COMMENT '{safe_desc}'
                        """
                        try:
                            cursor.execute(alter_sql)
                            updated_any = True
                        except Exception:
                            pass

                cursor.close()

                if updated_any:
                    tables_updated += 1
                    if logger:
                        logger.info(f"✓ Applied descriptions to: {model_name}")

            except Exception as e:
                if logger:
                    logger.warning(f"Failed to apply descriptions to {model_name}: {e}")

        return tables_updated
