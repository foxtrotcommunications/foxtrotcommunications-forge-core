"""
Forge Core — Root Processing Module

Handles root model creation, execution, and validation.
"""

from dataclasses import dataclass
from typing import Optional
import logging

from forge_core.engine.dbt_runner import run_dbt_command
from forge_core.engine.model_generator import create_file_in_models, tag_models_as_excluded

logger = logging.getLogger(__name__)


@dataclass
class RootBuildResult:
    """Result of root model build"""

    model_name: str
    rows_processed: int
    sql: str
    success: bool = True


def create_and_build_root_model(
    adapter,
    qualified_table_name: str,
    target_dataset: str,
    source_type: str,
    limit: Optional[int] = None,
) -> RootBuildResult:
    """
    Create root model SQL, execute dbt build, and track rows processed.

    Args:
        adapter: Warehouse adapter instance
        qualified_table_name: Fully qualified source table name
        target_dataset: Target dataset for dbt
        source_type: Type of warehouse ('bigquery', 'snowflake', etc.)
        limit: Optional row limit for root query

    Returns:
        RootBuildResult with model name, rows processed, and SQL

    Raises:
        RuntimeError: If root build fails
    """
    create_root_sql = adapter.get_root_table_sql(qualified_table_name, limit)

    root_model_name = "FRG" if source_type == "snowflake" else "frg"

    create_file_in_models(root_model_name, create_root_sql)

    dbt_command = (
        f"dbt build --profile forge --profiles-dir . "
        f"--target {target_dataset}"
    )

    result = run_dbt_command(dbt_command)

    if result.returncode != 0:
        logger.error(f"Root build failed. Generated SQL was:\n{create_root_sql}")
        logger.error(f"STDERR:\n{result.stderr}")
        logger.error(f"STDOUT:\n{result.stdout}")
        raise RuntimeError("Root build failed")

    logger.info(f"Root Build Success for {qualified_table_name}")

    tag_models_as_excluded([root_model_name])

    return RootBuildResult(
        model_name=root_model_name,
        rows_processed=0,
        sql=create_root_sql,
        success=True,
    )


def create_and_build_root_model_batched(
    adapter,
    qualified_table_name: str,
    target_dataset: str,
    source_type: str,
    batch_size: int,
    limit: Optional[int] = None,
) -> RootBuildResult:
    """
    Create root model in batches to cap per-query memory usage.

    Processes the source table in chunks of `batch_size` rows using
    LIMIT/OFFSET. Each chunk is an incremental dbt build, so rows
    accumulate in the target table without duplication.

    This prevents OOM kills on Postgres when multiple forge builds
    run concurrently against the same database.

    Args:
        adapter: Warehouse adapter instance
        qualified_table_name: Fully qualified source table name
        target_dataset: Target dataset for dbt
        source_type: Type of warehouse
        batch_size: Maximum rows per batch
        limit: Optional total row limit

    Returns:
        RootBuildResult with model name and SQL from last batch

    Raises:
        RuntimeError: If any batch fails
    """
    root_model_name = "FRG" if source_type == "snowflake" else "frg"

    # Get total rows to process
    total_rows = adapter.get_source_row_count(qualified_table_name)
    if limit is not None:
        total_rows = min(total_rows, limit)

    logger.info(f"Batched root build: {total_rows:,} rows in batches of {batch_size:,}")

    offset = 0
    batch_num = 0
    last_sql = ""

    while offset < total_rows:
        batch_num += 1
        current_limit = min(batch_size, total_rows - offset)

        logger.info(f"  Batch {batch_num}: rows {offset:,}–{offset + current_limit:,}")

        create_root_sql = adapter.get_root_table_sql(
            qualified_table_name,
            limit=current_limit,
            offset=offset,
        )
        last_sql = create_root_sql

        create_file_in_models(root_model_name, create_root_sql)

        dbt_command = (
            f"dbt build --profile forge --profiles-dir . "
            f"--target {target_dataset}"
        )

        result = run_dbt_command(dbt_command)

        if result.returncode != 0:
            logger.error(f"Root batch {batch_num} failed at offset {offset}")
            logger.error(f"STDERR:\n{result.stderr}")
            logger.error(f"STDOUT:\n{result.stdout}")
            raise RuntimeError(f"Root build failed at batch {batch_num}")

        offset += current_limit

    logger.info(f"Root Build Success: {batch_num} batches for {qualified_table_name}")

    tag_models_as_excluded([root_model_name])

    return RootBuildResult(
        model_name=root_model_name,
        rows_processed=0,
        sql=last_sql,
        success=True,
    )


def get_rows_processed(
    adapter,
    target_project: str,
    target_dataset: str,
    model_name: str,
    job_created_at_str: str,
) -> int:
    """
    Get the number of rows processed for a model.
    """
    rows_processed_sql = adapter.get_rows_processed_sql(
        target_project, target_dataset, model_name, job_created_at_str
    )

    result_df = adapter.execute_query(rows_processed_sql)
    rows_processed = int(result_df.iloc[0, 0])

    return rows_processed


def has_root_keys(
    adapter,
    root_table_name: str,
) -> bool:
    """
    Check if the root table has any keys to process.
    """
    keys_df = adapter.get_keys(root_table_name, "root", False)

    has_keys = not (keys_df.empty or len(keys_df.iloc[0, 0]) == 0)

    if not has_keys:
        logger.info("No new data to process in the root object. Gracefully exiting.")

    return has_keys


def build_root_table_name_for_keys(
    source_type: str,
    target_project: str,
    target_dataset: str,
) -> str:
    """
    Build the fully qualified root table name for key discovery.
    """
    if source_type == "snowflake":
        return f'"{target_project}"."{target_dataset}"."FRG"'
    elif source_type == "databricks":
        return f"{target_project}.{target_dataset}.frg"
    elif source_type in ("redshift", "postgres"):
        return f'"{target_dataset}"."frg"'
    else:  # BigQuery
        return f"`{target_project}.{target_dataset}.frg`"
