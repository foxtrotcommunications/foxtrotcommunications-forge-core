"""
Forge Core — Discovery Module

Handles type inference and table task processing for JSON unnesting.
Stripped of AI enrichment — produces pure structural metadata.
"""

import re
import pandas
import logging
import os
from forge_core.adapters import get_adapter
from forge_core.engine.context import get_warehouse_adapter
from forge_core.engine.model_generator import create_file_in_models

logger = logging.getLogger(__name__)


def _get_existing_name_assignments(parent_table_name, prefix_groups):
    """
    Probe existing child tables to learn which field → rank assignments
    are already locked from previous runs.

    Instead of querying INFORMATION_SCHEMA (which many enterprise customers
    restrict), we directly attempt to read the table_path column from
    candidate child tables. If the table exists, we learn the mapping.
    If it doesn't, the query fails and we move on.

    All queries stay within the same dataset — no cross-schema access needed.

    Args:
        parent_table_name: Qualified parent table (e.g. `proj.dataset.frg__root__raw_1`)
        prefix_groups: Set of 4-char prefixes to probe (e.g. {"exte", "extr"})

    Returns:
        dict mapping field_name → existing_rank (int)
        e.g. {"extension": 1, "extensionString": 2}
    """
    adapter = get_warehouse_adapter()
    assignments = {}

    if not prefix_groups:
        return assignments

    try:
        cleaned = parent_table_name.replace('`', '').replace('"', '')
        parts = cleaned.split('.')
        parent_short = parts[-1]  # e.g. "frg__root__raw_1"

        is_snowflake = 'SnowflakeAdapter' in str(type(adapter))
        is_databricks = 'DatabricksAdapter' in str(type(adapter))
        is_postgres = 'PostgresAdapter' in str(type(adapter))
        is_redshift = 'RedshiftAdapter' in str(type(adapter))

        def _qualify(child_name):
            """Build a fully-qualified reference for a child table."""
            if is_snowflake:
                return f'"{parts[0]}"."{parts[1]}"."{child_name}"'
            elif is_databricks:
                return f'{parts[0]}.{parts[1]}.{child_name}'
            elif is_postgres or is_redshift:
                schema = parts[0] if len(parts) >= 2 else 'public'
                return f'"{schema}"."{child_name}"'
            else:
                # BigQuery
                dataset_ref = '.'.join(parts[:-1])
                return f'`{dataset_ref}.{child_name}`'

        # For each prefix group, probe rank 1, 2, 3, ... until we miss.
        # Most groups have 1-2 members; cap at 10 for safety.
        MAX_PROBE_RANK = 10

        for prefix in prefix_groups:
            for rank in range(1, MAX_PROBE_RANK + 1):
                child_name = f"{parent_short}__{prefix}{rank}"
                fq_child = _qualify(child_name)

                try:
                    path_sql = f"SELECT DISTINCT table_path FROM {fq_child} LIMIT 1"
                    path_df = adapter.execute_query(path_sql)

                    if path_df.empty:
                        # Table exists but is empty — stop probing this prefix
                        break

                    table_path = path_df.iloc[0, 0]
                    # table_path is e.g. "frg__root__extension"
                    # The field name is the last segment
                    field_name = table_path.split('__')[-1]
                    assignments[field_name] = rank
                    logger.debug(
                        f"Locked existing name: {field_name} -> "
                        f"{prefix}{rank} (from {child_name})"
                    )
                except Exception:
                    # Table doesn't exist — no more ranks for this prefix
                    break

    except Exception as e:
        logger.debug(f"Could not probe existing child tables: {e}")

    return assignments


def types_builder(table_name, field_name, keys_df, is_array):
    """
    Builds a DataFrame containing type information for fields within a table.
    """
    adapter = get_warehouse_adapter()
    keys = keys_df.loc[0]["keys"]

    get_types_list = []
    for key in keys:
        # Strip any embedded JSON quotes from key names
        # (at deeper nesting levels, JSON_KEYS can return quoted keys)
        clean_key = key.strip('"')
        get_types_list.append(
            adapter.get_types_sql(table_name, field_name, clean_key, is_array)
        )

    df = pandas.DataFrame(
        {
            "field": pandas.Series(dtype="str"),
            "type": pandas.Series(dtype="str"),
            "table_key": pandas.Series(dtype="int"),
            "table_index": pandas.Series(dtype="str"),
        }
    )

    sub_object_count = 0
    get_types_sql = ""
    for i, sql in enumerate(get_types_list):

        if sub_object_count == 0:
            get_types_sql += sql + "\n"
            sub_object_count += 1
            continue

        if sub_object_count < 50:
            get_types_sql += "union distinct " + "\n" + sql + "\n"
            sub_object_count += 1

        if sub_object_count == 50:
            get_types_sql += "union distinct " + "\n" + sql + "\n"
            get_types_super = f"""
{get_types_sql}
"""
            get_types_sql = ""
            sub_object_count = 0

            if get_types_super.isspace() == False:
                df = pandas.concat(
                    [df, adapter.execute_query(get_types_super)], ignore_index=True
                )

    if sub_object_count == 1 or sub_object_count < 50:
        get_types_super = f"""
{get_types_sql}
"""
        if get_types_super.isspace() == False:
            df = pandas.concat(
                [df, adapter.execute_query(get_types_super)], ignore_index=True
            )

    df["table_index"] = df["field"].str.replace(r"[^a-zA-Z0-9]", "_", regex=True)
    df["table_index"] = df["field"].str.slice(0, 4)

    # Sort by field to ensure deterministic ranking
    df = df.sort_values(by=["field"])

    # ── Stable naming: preserve existing table_path → rank assignments ──
    # Query child tables from previous runs to learn which field → rank
    # mappings are already locked. New fields get the next available rank
    # within their prefix group. This prevents rank shifts when new fields
    # with colliding 4-char prefixes appear alphabetically before existing
    # fields in incremental mode.
    # Extract the set of 4-char prefixes that have more than one field
    # (only these can suffer from rank-shift collisions)
    prefix_counts = df["table_index"].value_counts()
    collision_prefixes = set(prefix_counts[prefix_counts > 1].index)
    existing_assignments = _get_existing_name_assignments(table_name, collision_prefixes)

    if existing_assignments:
        logger.info(
            f"Found {len(existing_assignments)} existing name assignments "
            f"for {table_name}: {existing_assignments}"
        )

    def _stable_rank(group):
        """Rank fields within a prefix group, preserving existing assignments."""
        prefix = group.name  # the 4-char prefix
        fields = group["field"].tolist()

        # Separate locked vs new fields
        locked = {f: existing_assignments[f] for f in fields
                  if f in existing_assignments}
        new_fields = [f for f in fields if f not in existing_assignments]

        # Find next available rank (after all locked ranks)
        used_ranks = set(locked.values())
        next_rank = max(used_ranks, default=0) + 1

        # Assign ranks: locked fields keep theirs, new fields get next available
        ranks = {}
        for f, r in locked.items():
            ranks[f] = r
        for f in sorted(new_fields):  # alphabetical for determinism among new
            while next_rank in used_ranks:
                next_rank += 1
            ranks[f] = next_rank
            used_ranks.add(next_rank)
            next_rank += 1

        return group["field"].map(ranks)

    df["table_key"] = (
        df.groupby("table_index", group_keys=False)
        .apply(_stable_rank)
        .astype(int)
    )

    df["table_index"] = df["table_index"].astype(str) + df["table_key"].astype(str)

    # field name must start with a letter or underscore
    t_index = df["table_index"]
    if t_index[0].isdigit() == True:
        df["table_index"] = "_" + t_index

    return df


def process_table_task(row):
    """
    Processes a single table task, typically running in a separate thread.
    Produces structural metadata without AI enrichment.
    """

    try:
        adapter = get_warehouse_adapter()
        logger.info(f"Processing table task: {row['table_name']}.{row['field_name']}")

        keys_df = adapter.get_keys(
            row["table_name"], row["field_name"], row["is_array"]
        )

        # next table name must be 4 characters, use 0's to fill if not
        table_index_s = row["table_index"]
        table_index_s = re.sub(r"[^a-zA-Z0-9_]", "", table_index_s)

        is_snowflake = (
            getattr(adapter, "__class__", None).__name__ == "SnowflakeAdapter"
        )
        is_databricks = (
            getattr(adapter, "__class__", None).__name__ == "DatabricksAdapter"
        )
        is_postgres = (
            getattr(adapter, "__class__", None).__name__ == "PostgresAdapter"
        )
        is_redshift = (
            getattr(adapter, "__class__", None).__name__ == "RedshiftAdapter"
        )
        safe_table_base = row["table_name"].replace("`", "").replace('"', "")
        if is_snowflake:
            base_model_name = safe_table_base.split(".")[-1].upper()
            model_name = base_model_name + "__" + table_index_s.upper()
            model_ref = f"{{{{ref('{base_model_name}')}}}}"
        elif is_databricks:
            base_model_name = safe_table_base.split(".")[-1]
            model_name = base_model_name + "__" + table_index_s
            model_ref = f"{{{{ref('{base_model_name}')}}}}"
        elif is_postgres or is_redshift:
            base_model_name = safe_table_base.split(".")[-1]
            model_name = base_model_name + "__" + table_index_s
            model_ref = f"{{{{ref('{base_model_name}')}}}}"
        else:
            base_model_name = row["table_name"].replace("`", "").split(".")[2]
            model_name = base_model_name + "__" + table_index_s
            model_ref = f"{{{{ref('{base_model_name}')}}}}"

        if keys_df.empty == True or keys_df.iloc[0, 0] is None or len(keys_df.iloc[0, 0]) == 0:
            return None

        current_table_path = row["path"] + "__" + row["field_name"]

        types_df = types_builder(
            row["table_name"], row["field_name"], keys_df, row["is_array"]
        )
        cleaned_ref = row["table_name"].replace("`", "").replace('"', "")

        if is_snowflake:
            parts = cleaned_ref.split(".")
            if len(parts) >= 3:
                next_table_name = f'"{parts[0]}"."{parts[1]}"."{model_name}"'
            else:
                next_table_name = f'"{model_name}"'
        elif is_databricks:
            parts = cleaned_ref.split(".")
            if len(parts) >= 3:
                next_table_name = f"{parts[0]}.{parts[1]}.{model_name}"
            else:
                next_table_name = model_name
        elif is_postgres or is_redshift:
            parts = cleaned_ref.split(".")
            if len(parts) >= 2:
                next_table_name = f'"{parts[0]}"."{model_name}"'
            else:
                next_table_name = f'"{model_name}"'
        else:
            next_table_name = f"`{cleaned_ref}__{table_index_s}`"

        # Build selects using adapter logic
        types_df["clean_field_name"] = types_df["field"].str.replace(
            r"[^a-zA-Z0-9]", "_", regex=True
        )
        types_df["clean_field_name"] = (
            types_df["clean_field_name"]
            .str.replace(r"([a-z0-9])([A-Z])", r"\1_\2", regex=True)
            .str.lower()
        )
        types_df["table_key"] = (
            types_df.groupby("clean_field_name")["clean_field_name"]
            .rank(method="first", ascending=True, na_option="top")
            .astype(int)
        )

        mask_digit = types_df["clean_field_name"].str[0].str.isdigit().fillna(False)
        types_df.loc[mask_digit, "clean_field_name"] = (
            "_" + types_df.loc[mask_digit, "clean_field_name"]
        )

        mask_dup = types_df["table_key"] > 1
        types_df.loc[mask_dup, "clean_field_name"] = types_df.loc[
            mask_dup, "clean_field_name"
        ] + types_df.loc[mask_dup, "table_key"].astype(str)

        selects_array = []
        for r in types_df.itertuples():
            safe_field = r.field.replace('"', '"')
            source_field = "exploded_value" if is_databricks else row["field_name"]
            select_sql = adapter.build_select_expression(
                source_field, safe_field, r.clean_field_name, r.type
            )
            selects_array.append(select_sql)

        selects_sql_str = ""
        for i, select in enumerate(selects_array):
            if i == 0:
                selects_sql_str += "    " + select
            else:
                selects_sql_str += "\n   ," + select

        create_table_sql = adapter.get_create_table_sql(
            model_ref,
            row["field_name"],
            selects_sql_str,
            row["is_array"],
            current_table_path,
        )

        # Write model for active warehouse only (no cross-warehouse compilation)
        create_file_in_models(model_name, create_table_sql)

        next_batch_items = []
        children_metadata = []
        scalar_fields = []

        for row_2 in types_df.itertuples():
            item = {
                "table_name": next_table_name,
                "field_name": row_2.clean_field_name,
                "is_array": False,
                "table_index": row_2.table_index,
                "path": current_table_path,
            }

            if row_2.type == "object":
                item["is_array"] = True
                next_batch_items.append(item)
                children_metadata.append(
                    {
                        "field_name": row_2.clean_field_name,
                        "type": "ARRAY",
                        "model_suffix": row_2.table_index,
                    }
                )
            elif row_2.type == "array":
                item["is_array"] = True
                next_batch_items.append(item)
                children_metadata.append(
                    {
                        "field_name": row_2.clean_field_name,
                        "type": "ARRAY",
                        "model_suffix": row_2.table_index,
                    }
                )
            else:
                scalar_fields.append(
                    {
                        "name": row_2.clean_field_name,
                        "original_type": row_2.type,
                    }
                )

        metadata = {
            "model_name": model_name,
            "parent_model": row["table_name"]
            .replace("`", "")
            .replace('"', "")
            .split(".")[-1],
            "field_name": row["field_name"],
            "is_array": row["is_array"],
            "type": "ARRAY" if row["is_array"] else "STRUCT",
            "scalar_fields": scalar_fields,
            "children": children_metadata,
            "table_path": current_table_path,
            "depth": len(current_table_path.split("__")) - 1,
        }



        return (model_name, next_batch_items, metadata)

    except Exception as e:
        logger.error(f"Error processing table {row['table_name']}: {e}", exc_info=True)
        raise e
