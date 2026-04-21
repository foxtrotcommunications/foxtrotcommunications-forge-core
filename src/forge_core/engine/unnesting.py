"""
Forge Core — Unnesting Coordinator Module

Handles iterative JSON unnesting workflow.
Processes nested JSON structures level-by-level until all fields are flattened.
"""

from dataclasses import dataclass, field
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Optional
import logging

from forge_core.engine.dbt_runner import run_dbt_command
from forge_core.engine.model_generator import tag_models_as_excluded
from forge_core.engine.discovery import process_table_task

logger = logging.getLogger(__name__)


@dataclass
class UnnestingResult:
    """Result of iterative unnesting process"""

    all_metadata: List[Dict] = field(default_factory=list)
    total_rows_processed: int = 0
    levels_processed: int = 0


def create_root_metadata(
    root_model_name: str,
    qualified_table_name: str,
) -> Dict:
    """
    Create metadata entry for the root model.

    Args:
        root_model_name: Name of root model (frg or FRG)
        qualified_table_name: Fully qualified source table name

    Returns:
        Root metadata dictionary
    """
    return {
        "model_name": root_model_name,
        "parent_model": None,
        "field_name": "root",
        "is_array": False,
        "scalar_fields": [],
        "children": [
            {
                "field_name": "root",
                "type": "STRUCT",
                "model_suffix": "root",
            }
        ],
        "depth": 0,
        "table_path": "frg",
        "source_table": qualified_table_name,
    }


def execute_iterative_unnesting(
    adapter,
    root_table_name_for_keys: str,
    root_model_name: str,
    qualified_table_name: str,
    target_dataset: str,
    target_project: str,
    job_created_at_str: str,
) -> UnnestingResult:
    """
    Execute iterative unnesting until no more nested fields are found.

    Processes JSON structures level-by-level in parallel, building dbt models
    at each level and tracking metadata for all discovered fields.

    Args:
        adapter: Warehouse adapter instance
        root_table_name_for_keys: Qualified root table name for key discovery
        root_model_name: Name of root model (frg or FRG)
        qualified_table_name: Fully qualified source table name
        target_dataset: Target dataset/schema
        target_project: Target project/database
        job_created_at_str: ISO timestamp of job creation

    Returns:
        UnnestingResult with metadata and row counts
    """
    result = UnnestingResult()

    # Create root metadata
    root_metadata = create_root_metadata(root_model_name, qualified_table_name)
    result.all_metadata.append(root_metadata)

    # Initialize queue with the root table
    queue = deque(
        [
            {
                "table_name": root_table_name_for_keys,
                "field_name": "root",
                "is_array": False,
                "table_index": (
                    "ROOT" if "SnowflakeAdapter" in str(type(adapter)) else "root"
                ),
                "path": root_model_name,
            }
        ]
    )

    # Process levels iteratively
    while queue:
        pre_model_name = ""
        next_batch = deque()

        # Process all items in the current level (batch) in PARALLEL
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(process_table_task, row) for row in queue]

            for future in as_completed(futures):
                task_result = future.result()
                if task_result:
                    model_name, next_items, metadata = task_result
                    pre_model_name = pre_model_name + " " + model_name
                    next_batch.extend(next_items)
                    result.all_metadata.append(metadata)

        # If no new models were created in this batch, we are done
        if pre_model_name.strip() == "":
            break

        # Execute dbt build for this level
        dbt_command = (
            f"dbt build --profile forge --profiles-dir . "
            f"--exclude tag:exclude "
            f"--target {target_dataset}"
        )
        dbt_result = run_dbt_command(dbt_command)

        if dbt_result.returncode != 0:
            logger.error(f"Error in Processing! {dbt_result}")
            raise RuntimeError(f"Intermediate build failed: {dbt_result.stderr}")

        # Track rows processed for each model in this level
        tables = pre_model_name.split(" ")

        for table in tables:
            if table == "":
                continue

            rows_processed_sql = adapter.get_rows_processed_sql(
                target_project, target_dataset, table, job_created_at_str
            )
            rows_processed = int(adapter.execute_query(rows_processed_sql).iloc[0, 0])
            result.total_rows_processed += rows_processed

        # Tag the models we just built as excluded for the next iteration
        tag_models_as_excluded(tables)

        # Move to the next level
        queue = next_batch
        result.levels_processed += 1

    return result
