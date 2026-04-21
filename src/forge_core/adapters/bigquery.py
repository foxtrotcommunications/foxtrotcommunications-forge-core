import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from .base import WarehouseAdapter
from typing import List, Dict, Any, Optional
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests.adapters


class BigQueryAdapter(WarehouseAdapter):
    """
    BigQuery implementation of the WarehouseAdapter.
    """

    def __init__(self, key_path: str = None):
        self.client = None
        self.key_path = key_path
        # Lazy init: Do not initialize client here to allow offline SQL generation
        # self._initialize_client()

    def _initialize_client(self):
        try:
            if self.key_path and os.path.exists(self.key_path):
                # Use explicit service account key file
                creds = service_account.Credentials.from_service_account_file(
                    self.key_path,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
                self.client = bigquery.Client(
                    credentials=creds, project=creds.project_id
                )
            else:
                # Use Application Default Credentials (gcloud auth, ADC, etc.)
                self.client = bigquery.Client()

            # Increase connection pool size to support 20+ parallel workers
            if hasattr(self.client, "_http") and self.client._http:
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=30, pool_maxsize=30
                )
                self.client._http.mount("https://", adapter)

        except Exception as e:
            print(f"Error initializing BigQuery client: {e}", file=sys.stderr)
            # We don't raise here to allow for potential retry or environment fix

    def _ensure_client(self):
        """Lazy initialization of the client."""
        if not self.client:
            self._initialize_client()

    def execute_query(self, sql: str) -> pd.DataFrame:
        if not sql or sql.isspace():
            return pd.DataFrame()

        self._ensure_client()

        if not self.client:
            print(
                "Error: BigQuery client not initialized (check credentials).",
                file=sys.stderr,
            )
            return pd.DataFrame()

        query_job = self.client.query(sql)
        return query_job.result().to_dataframe()

    def _read_template(self, template_name: str) -> str:
        # Templates are stored in the templates/bigquery directory relative to this file.
        try:
            # Construct path relative to the current file
            base_path = os.path.dirname(__file__)
            template_path = os.path.join(
                base_path, "templates", "bigquery", template_name
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
        template = "get_keys_array.sql" if is_array else "get_keys_array.sql"
        sql = self._read_template(template)
        sql = sql.replace("~JSON_FIELD~", field_name)
        sql = sql.replace("~TABLE_NAME~", table_name)
        df = self.execute_query(sql)
        return df

    @staticmethod
    def _safe_jsonpath(key: str) -> str:
        """Escape a field name for BigQuery JSONPath dot notation.

        BigQuery JSON_QUERY only supports $."key" notation (not brackets).
        Dots inside quoted keys are treated as literal characters, not path
        separators.

        Examples:
            "name"                    -> '$."name"'
            "pseudo_802.1ad_enabled"  -> '$."pseudo_802.1ad_enabled"'
            'field"name'              -> '$."field\\"name"'
        """
        escaped = key.replace("\\", "\\\\").replace('"', '\\"')
        return f'$."{ escaped }"'

    def get_types_sql(
        self, table_name: str, field_name: str, key: str, is_array: bool
    ) -> str:
        template = "get_types_array.sql" if is_array else "get_types_array.sql"
        sql = self._read_template(template)
        sql = sql.replace("~JSON_FIELD~", field_name)
        sql = sql.replace("~KEY~", key)
        sql = sql.replace("~JSONPATH_KEY~", self._safe_jsonpath(key))
        sql = sql.replace("~TABLE_NAME~", table_name)
        return sql

    def build_select_expression(
        self, field_name: str, safe_field: str, clean_field_name: str, field_type: str
    ) -> str:
        # Escape quotes in field name for JSONPath $."key" notation
        escaped = safe_field.replace("\\", "\\\\").replace('"', '\\"')
        if field_type == "array":
            return f"""
            COALESCE(
                JSON_EXTRACT_SCALAR(
                    IF(
                        TO_JSON_STRING(
                        SAFE.PARSE_JSON(REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON_STRING(JSON_QUERY(`{field_name}`, '$."{escaped}"')), r'^\[\[+', '['),r'\]\]+$', ']'))
                        )
                        = 'null'
                        ,null
                        ,REGEXP_REPLACE(REGEXP_REPLACE(JSON_EXTRACT(JSON_QUERY(`{field_name}`, '$."{escaped}"'), '$.'), r'^\[\[+', '['),r'\]\]+$', ']') 
                    )
                , '$.')
                ,IF(
                    TO_JSON_STRING(
                    SAFE.PARSE_JSON(REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON_STRING(JSON_QUERY(`{field_name}`, '$."{escaped}"')), r'^\[\[+', '['),r'\]\]+$', ']'))
                    )
                    = 'null'
                    ,null
                    ,REGEXP_REPLACE(REGEXP_REPLACE(JSON_EXTRACT(JSON_QUERY(`{field_name}`, '$."{escaped}"'), '$.'), r'^\[\[+', '['),r'\]\]+$', ']') 
                )
            ) AS `{clean_field_name}`"""
        elif field_type == "object":
            return f"""
            REGEXP_REPLACE(REGEXP_REPLACE("[" ||
            COALESCE(
                JSON_EXTRACT_SCALAR(
                    IF(
                        TO_JSON_STRING(
                            JSON_QUERY(`{field_name}`, '$."{escaped}"')
                            )
                            = 'null'
                            ,null
                            ,CAST(JSON_EXTRACT(JSON_QUERY(`{field_name}`, '$."{escaped}"'), '$') AS STRING)
                        )
                , '$.') 
                ,IF(
                    TO_JSON_STRING(
                        JSON_QUERY(`{field_name}`, '$."{escaped}"')
                        )
                        = 'null'
                        ,null
                        ,CAST(JSON_EXTRACT(JSON_QUERY(`{field_name}`, '$."{escaped}"'), '$') AS STRING)
                    ) 
            ) || "]", r'^\[\[+', '['),r'\]\]+$', ']') AS `{clean_field_name}` """
        else:
            return f"""JSON_EXTRACT_SCALAR(JSON_QUERY(`{field_name}`, '$."{escaped}"'), '$.') AS `{clean_field_name}`"""

    def get_create_table_sql(
        self,
        table_name: str,
        field_name: str,
        selects_sql: str,
        is_array: bool,
        table_path: str,
    ) -> str:
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

        sql = sql.replace("~LABELS_CONFIG~", "")

        return sql

    def validate_source(self, table_name: str) -> bool:
        """Validate connection to source table."""
        try:
            self._ensure_client()

            # Auto-detect location from source table if possible
            try:
                # Strip backticks for get_table
                clean_name = table_name.replace("`", "")
                table = self.client.get_table(clean_name)

                if table.location:
                    client_location = self.client.location or ""
                    if table.location.lower() != client_location.lower():
                        print(
                            f"✓ Auto-detected source location: {table.location} (was {self.client.location or 'not set'}). Updating client.",
                            file=sys.stderr,
                        )
                    # Re-initialize client with correct location to avoid 404/403 errors
                    # Keep same credentials
                    creds = self.client._credentials
                    project = self.client.project
                    self.client = bigquery.Client(
                        credentials=creds, project=project, location=table.location
                    )

                    # Re-mount adapter for pool size
                    if hasattr(self.client, "_http") and self.client._http:
                        adapter = requests.adapters.HTTPAdapter(
                            pool_connections=30, pool_maxsize=30
                        )
                        self.client._http.mount("https://", adapter)
            except Exception as e:
                # If get_table fails, just proceed to query check
                # It might fail due to permissions, but the query might still work
                print(f"Debug: Could not auto-detect location: {e}", file=sys.stderr)
                pass

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
        Checks if a specific column in a table contains what could be a JSON string.
        It does this by attempting to parse the first non-null value in the column as JSON.

        Only considers a column as JSON if the value starts with '{' or '[' — i.e.,
        it looks like a JSON object or array. Bare scalars (numbers, booleans, plain
        strings like CVX codes '140') are NOT treated as JSON even though PARSE_JSON
        would accept them. This prevents false positives that break get_json_column_mapping.
        """
        query = f"""
            SELECT SAFE.PARSE_JSON(CAST({column_name} AS STRING)) IS NOT NULL
              AND REGEXP_CONTAINS(LTRIM(CAST({column_name} AS STRING)), r'^[{{\\[]')
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            LIMIT 1
        """
        try:
            result_df = self.execute_query(query)
            if not result_df.empty and result_df.iloc[0, 0] == True:
                return True
        except Exception as e:
            # print(f"Could not check if {column_name} is JSON for table {table_name}. Error: {e}", file=sys.stderr)
            return False
        return False

    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Retrieves all column names from a specified table.
        """
        query = f"SELECT * FROM {table_name} LIMIT 1"
        df = self.execute_query(query)
        return df.columns.tolist()

    def get_json_column_mapping(self, table_name: str) -> str:
        """
        Analyzes a table and returns a SQL select statement that formats the row as a single JSON object.
        If a column is a json string, it's wrapped in parse_json(field),
        otherwise it's just selected.
        """
        columns = self.get_table_columns(table_name)

        # Special case: if the source is a single JSON column, we want to treat its content as the root.
        if len(columns) == 1 and self.check_column_is_json(table_name, columns[0]):
            # This query checks the keys of the top-level object in the first row.
            keys_query = f"""
                SELECT JSON_KEYS(PARSE_JSON(CAST({columns[0]} AS STRING)))
                FROM {table_name}
                WHERE {columns[0]} IS NOT NULL
                LIMIT 1
            """
            try:
                keys_df = self.execute_query(keys_query)
                # If we found keys, and there's only one, and it's named 'root'
                if (
                    not keys_df.empty
                    and len(keys_df.iloc[0, 0]) == 1
                    and keys_df.iloc[0, 0][0] == "root"
                ):
                    # Unnest the 'root' key to prevent the recursive loop.
                    # The result is still aliased as `root` for the downstream template.
                    return f"SELECT JSON_QUERY(PARSE_JSON(CAST({columns[0]} AS STRING)), '$.root') AS `root` \n\t\tFROM {table_name}\n\t ~LIMITER~"
            except Exception:
                # If the query fails for any reason, fall back to the default safe behavior.
                pass

            # Default behavior for a single JSON column: use the whole object.
            return f"SELECT PARSE_JSON(CAST({columns[0]} AS STRING)) AS `root` \n\t\tFROM {table_name}\n\t ~LIMITER~"

        # Original logic for multi-column tables, which correctly wraps them into a single JSON object.
        mapping = {}
        for col in columns:
            mapping[col] = self.check_column_is_json(table_name, col)

        select_parts = []
        for column_name, is_json in mapping.items():
            if is_json:
                select_parts.append(
                    f"'{column_name}', PARSE_JSON(CAST({column_name} AS STRING))"
                )
            else:
                select_parts.append(f"'{column_name}', {column_name}")

        sql = f"SELECT JSON_OBJECT( {', '.join(select_parts)} ) `root` \n\t\tFROM {table_name}\n\t ~LIMITER~"

        return sql

    def get_root_table_sql(
        self,
        table_name: str,
        limit: Optional[int] = None,
    ) -> str:
        template = "create_root_aggregate.sql"
        sql = self._read_template(template)
        sql = sql.replace("~SQL_SELECTS~", self.get_json_column_mapping(table_name))

        if limit is not None:
            sql = sql.replace("~LIMITER~", f"\tLIMIT {limit}")
        else:
            sql = sql.replace("~LIMITER~", f"")

        sql = sql.replace("~LABELS_CONFIG~", "")

        return sql

    def get_rows_processed_sql(
        self, project: str, dataset: str, table: str, timestamp: str
    ) -> str:
        sql = self._read_template("get_rows_processed.sql")
        sql = sql.replace("~PROJECT~", project)
        sql = sql.replace("~DATASET~", dataset)
        sql = sql.replace("~TABLE_NAME~", table)
        sql = sql.replace("~BEGINNING_TS~", timestamp)
        return sql

    def generate_rollup_sql(
        self,
        metadata_list: List[Dict[str, Any]],
        target_dataset: str,
        model_prefix: str = "",
    ) -> str:
        # This logic was previously in forge.py.
        # Since it generates BigQuery-specific SQL (ARRAY_AGG, STRUCT, SPLIT, OFFSET),
        # it belongs here in the adapter.

        # 1. Organize metadata by model name for easy access
        models = {m["model_name"]: m for m in metadata_list}

        # 2. Group by depth to process bottom-up
        depth_groups = {}
        max_depth = 0
        for m in metadata_list:
            d = m["depth"]
            if d not in depth_groups:
                depth_groups[d] = []
            depth_groups[d].append(m)
            if d > max_depth:
                max_depth = d

        # 3. Generate CTEs
        ctes = []

        # FIRST PASS: Determine which models will have valid (non-empty) CTEs
        # A model has a valid CTE if it has scalar fields OR has children with valid CTEs
        # We process bottom-up so child validity is known before parent
        models_with_valid_ctes = set()

        for depth in range(max_depth, 0, -1):
            if depth not in depth_groups:
                continue
            for model in depth_groups[depth]:
                model_name = model["model_name"]
                has_scalars = bool(model.get("scalar_fields"))

                # Check if any children have valid CTEs
                has_valid_children = False
                for child in model.get("children", []):
                    child_model_name = f"{model_name}__{child['model_suffix']}"
                    if child_model_name in models_with_valid_ctes:
                        has_valid_children = True
                        break

                if has_scalars or has_valid_children:
                    models_with_valid_ctes.add(model_name)

        # SECOND PASS: Generate CTEs only for valid models
        for depth in range(max_depth, 0, -1):
            if depth not in depth_groups:
                continue

            for model in depth_groups[depth]:
                model_name = model["model_name"]

                # Skip models without valid CTEs
                if model_name not in models_with_valid_ctes:
                    continue

                # We need to join with children CTEs (only those that are valid)
                joins = []
                for child in model["children"]:
                    child_model_name = f"{model_name}__{child['model_suffix']}"

                    # Check if child model exists AND has a valid CTE
                    if (
                        child_model_name not in models
                        or child_model_name not in models_with_valid_ctes
                    ):
                        continue

                    child_cte_name = f"{child_model_name}_agg"

                    join_conditions = []
                    # Join on all segments up to parent's depth + 1
                    for i in range(depth + 1):
                        join_conditions.append(
                            f"SPLIT(t.idx, '_')[OFFSET({i})] = SPLIT({child_cte_name}.idx, '_')[OFFSET({i})]"
                        )

                    joins.append(
                        f"LEFT JOIN {child_cte_name} ON t.ingestion_hash = {child_cte_name}.ingestion_hash AND {' AND '.join(join_conditions)}"
                    )

                struct_fields = []

                # Helper to extract field name from either string or dict format
                def get_field_name(field):
                    if isinstance(field, dict):
                        return field.get("name", str(field))
                    return str(field)

                for field in model["scalar_fields"]:
                    field_name = get_field_name(field)
                    struct_fields.append(f"t.`{field_name}`")
                for child in model["children"]:
                    child_model_name = f"{model_name}__{child['model_suffix']}"
                    # Only include child if it exists AND has a valid CTE
                    if (
                        child_model_name in models
                        and child_model_name in models_with_valid_ctes
                    ):
                        child_cte_name = f"{child_model_name}_agg"
                        if child["type"] == "ARRAY":
                            struct_fields.append(
                                f"ARRAY_AGG({child_cte_name}.`{child['field_name']}_struct` IGNORE NULLS) as `{child['field_name']}`"
                            )
                        else:
                            struct_fields.append(
                                f"ANY_VALUE({child_cte_name}.`{child['field_name']}_struct`) as `{child['field_name']}`"
                            )

                # This should not happen after first pass, but safety check
                if not struct_fields:
                    continue

                # Extract field names for GROUP BY clause
                scalar_field_names = [get_field_name(f) for f in model["scalar_fields"]]
                group_by_suffix = (
                    ", " + ", ".join([f"t.`{f}`" for f in scalar_field_names])
                    if scalar_field_names
                    else ""
                )

                cte_sql = f"{model_name}_agg AS (\n    SELECT \n        t.ingestion_hash,\n        t.idx,\n        STRUCT(\n            {', '.join(struct_fields)}\n        ) as `{model['field_name']}_struct`\n    FROM {{{{ ref('{model_prefix}{model_name}') }}}} t\n    {' '.join(joins)}\n    GROUP BY t.ingestion_hash, t.idx{group_by_suffix}\n)"
                ctes.append(cte_sql)

        # 4. Final Root Select
        root_model = [m for m in metadata_list if m["depth"] == 0]
        if not root_model:
            return ""

        root = root_model[0]
        root_joins = []
        root_struct_fields = []

        for child in root["children"]:
            child_model_name = f"{root['model_name']}__{child['model_suffix']}"

            # Check if child model exists AND has a valid CTE (non-empty struct)
            if (
                child_model_name not in models
                or child_model_name not in models_with_valid_ctes
            ):
                continue

            child_cte_name = f"{child_model_name}_agg"

            join_conditions = [
                f"SPLIT(r.idx, '_')[OFFSET(0)] = SPLIT({child_cte_name}.idx, '_')[OFFSET(0)]"
            ]

            root_joins.append(
                f"LEFT JOIN {child_cte_name} ON r.ingestion_hash = {child_cte_name}.ingestion_hash AND {' AND '.join(join_conditions)}"
            )

            if child["type"] == "ARRAY":
                root_struct_fields.append(
                    f"ARRAY_AGG({child_cte_name}.`{child['field_name']}_struct` IGNORE NULLS) as `{child['field_name']}`"
                )
            else:
                root_struct_fields.append(
                    f"ANY_VALUE({child_cte_name}.`{child['field_name']}_struct`) as `{child['field_name']}`"
                )

        config_block = """
{{
    config(
        materialized='view',
        unique_key=['ingestion_hash','idx'],
        on_schema_change='append_new_columns',
        partition_by={
            "field": "ingestion_timestamp",
            "data_type": "timestamp",
            "granularity": "month"
        },
        time_ingestion_column=true,
        cluster_by=["ingestion_timestamp"]~LABELS_CONFIG~
    )
}}
"""

        config_block = config_block.replace("~LABELS_CONFIG~", "")

        final_sql = f"""{config_block}

WITH 
{', '.join(ctes)}

SELECT
    r.ingestion_hash,
    r.ingestion_timestamp,
    r.idx,
    STRUCT(
        {', '.join(root_struct_fields)}
    ) as `{root['model_name']}`
FROM {{{{ ref('{model_prefix}{root['model_name']}') }}}} r
{' '.join(root_joins)}
GROUP BY r.ingestion_hash, r.ingestion_timestamp, r.idx
"""
        return final_sql

    def clean_dataset(self, dataset: str) -> bool:
        """
        Drops all tables and views in the target dataset using parallel execution.
        """
        try:
            # 1. Get list of drop statements
            get_object_sql = f"""
                SELECT concat("DROP ", CASE WHEN table_type = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END, " `", table_schema, '.', table_name, '`;') as _drop 
                FROM {dataset}.INFORMATION_SCHEMA.TABLES 
                WHERE table_type IN ('BASE TABLE', 'VIEW')
            """

            object_deletes_df = self.execute_query(get_object_sql)

            if object_deletes_df.empty or len(object_deletes_df) == 0:
                return True

            # 2. Execute drop statements in parallel using ThreadPoolExecutor
            drop_statements = [
                object_deletes_df.iloc[i, 0] for i in range(len(object_deletes_df))
            ]

            def drop_table(drop_sql: str) -> bool:
                """Helper function to drop a single table."""
                try:
                    self.execute_query(drop_sql)
                    return True
                except Exception as e:
                    print(
                        f"Error dropping table with SQL '{drop_sql}': {e}",
                        file=sys.stderr,
                    )
                    return False

            # Use ThreadPoolExecutor with 10 workers
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [
                    executor.submit(drop_table, drop_sql)
                    for drop_sql in drop_statements
                ]

                # Wait for all futures to complete and check results
                for future in as_completed(futures):
                    if not future.result():
                        return False

            return True
        except Exception as e:
            print(f"Error cleaning dataset {dataset}: {e}", file=sys.stderr)
            return False

    def load_json_file(
        self,
        file_path: str,
        project: str,
        dataset: str,
        table_name: str,
        source_format: str = "NEWLINE_DELIMITED_JSON",
        autodetect: bool = True,
    ) -> bool:
        """
        Loads a local JSON file into a BigQuery table.

        Args:
            file_path: Path to the local JSON file.
            project: Target Google Cloud project.
            dataset: Target BigQuery dataset.
            table_name: Target BigQuery table name.
            source_format: Format of the JSON file. Defaults to "NEWLINE_DELIMITED_JSON".
                           Options: "NEWLINE_DELIMITED_JSON", "JSON".
            autodetect: Whether to autodetect the schema. Defaults to True.

        Returns:
            True if successful, False otherwise.
        """
        try:
            self._ensure_client()

            if not self.client:
                print("Error: BigQuery client not initialized.", file=sys.stderr)
                return False

            dataset_ref = bigquery.DatasetReference(project, dataset)
            table_ref = dataset_ref.table(table_name)

            job_config = bigquery.LoadJobConfig()
            job_config.autodetect = autodetect

            if source_format == "JSON":
                job_config.source_format = bigquery.SourceFormat.JSON
            else:
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

            with open(file_path, "rb") as source_file:
                job = self.client.load_table_from_file(
                    source_file, table_ref, job_config=job_config
                )

            job.result()  # Waits for the job to complete.

            print(
                f"Loaded {job.output_rows} rows into {project}.{dataset}.{table_name}.",
                file=sys.stderr,
            )
            return True

        except Exception as e:
            print(f"Error loading JSON file to BigQuery: {e}", file=sys.stderr)
            return False

    def apply_column_descriptions(
        self,
        dataset: str,
        model_descriptions: Dict[str, Dict[str, str]],
        logger=None,
        max_workers: int = 10,
    ) -> int:
        """
        Apply column descriptions to BigQuery tables in parallel.

        Args:
            dataset: Target BigQuery dataset (e.g., 'project.dataset')
            model_descriptions: Dict of model_name -> {column_name: description}
            logger: Optional logger for output
            max_workers: Number of parallel workers (default 10, safe for BQ rate limits)

        Returns:
            Number of tables updated
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        self._ensure_client()
        if not self.client:
            if logger:
                logger.warning("BigQuery client not initialized")
            return 0

        if not model_descriptions:
            return 0

        # Process tables in parallel
        tables_updated = 0
        total_tables = len(model_descriptions)

        if logger:
            logger.info(
                f"Applying column descriptions to {total_tables} tables (parallel, {max_workers} workers)..."
            )

        def process_single_table(
            model_name: str, col_descriptions: Dict[str, str]
        ) -> bool:
            """Process a single table's column descriptions. Returns True if updated."""
            try:
                table_ref = f"{dataset}.{model_name}"

                try:
                    table = self.client.get_table(table_ref)
                except Exception:
                    return False  # Table doesn't exist

                # Build new schema with descriptions
                new_schema = []
                schema_changed = False

                for field in table.schema:
                    new_field = self._update_field_description(field, col_descriptions)
                    new_schema.append(new_field)
                    if new_field != field:
                        schema_changed = True

                if schema_changed:
                    table.schema = new_schema
                    self.client.update_table(table, ["schema"])
                    return True
                return False
            except Exception as e:
                if logger:
                    logger.warning(f"Failed to apply descriptions to {model_name}: {e}")
                return False

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_model = {
                executor.submit(process_single_table, model_name, cols): model_name
                for model_name, cols in model_descriptions.items()
            }

            # Collect results as they complete
            for future in as_completed(future_to_model):
                model_name = future_to_model[future]
                try:
                    if future.result():
                        tables_updated += 1
                        if logger:
                            logger.info(f"✓ Applied descriptions to: {model_name}")
                except Exception as e:
                    if logger:
                        logger.warning(f"Error processing {model_name}: {e}")

        return tables_updated

    def _update_field_description(
        self,
        field: bigquery.SchemaField,
        col_descriptions: Dict[str, str],
    ) -> bigquery.SchemaField:
        """
        Recursively update field descriptions.

        Args:
            field: BigQuery schema field
            col_descriptions: Dict of column_name -> description

        Returns:
            Updated SchemaField with description
        """
        # Get description for this field
        description = col_descriptions.get(field.name, field.description)

        # Recursively update nested fields
        if field.field_type == "RECORD" and field.fields:
            updated_subfields = []
            for subfield in field.fields:
                updated_subfields.append(
                    self._update_field_description(subfield, col_descriptions)
                )
            return bigquery.SchemaField(
                name=field.name,
                field_type=field.field_type,
                mode=field.mode,
                description=description,
                fields=tuple(updated_subfields),
            )
        else:
            return bigquery.SchemaField(
                name=field.name,
                field_type=field.field_type,
                mode=field.mode,
                description=description,
            )

    def apply_descriptions_to_table(
        self,
        table_ref: str,
        col_descriptions: Dict[str, str],
        logger=None,
    ) -> bool:
        """
        Apply column descriptions to a single BigQuery table in one API call.

        Recursively walks nested STRUCT fields to match descriptions by field
        name. This is required because dbt persist_docs cannot apply descriptions
        to nested STRUCT fields in BigQuery views.

        Args:
            table_ref: Fully qualified table reference (project.dataset.table)
            col_descriptions: Flat dict of field_name -> description
            logger: Optional logger

        Returns:
            True if schema was updated
        """
        self._ensure_client()
        if not self.client or not col_descriptions:
            return False

        try:
            table = self.client.get_table(table_ref)
            new_schema = [
                self._update_field_description(f, col_descriptions)
                for f in table.schema
            ]
            if new_schema != list(table.schema):
                table.schema = new_schema
                self.client.update_table(table, ["schema"])
                if logger:
                    logger.info(
                        f"✓ Applied {len(col_descriptions)} descriptions to {table_ref}"
                    )
                return True
            return False
        except Exception as e:
            if logger:
                logger.warning(f"Failed to apply descriptions to {table_ref}: {e}")
            return False
