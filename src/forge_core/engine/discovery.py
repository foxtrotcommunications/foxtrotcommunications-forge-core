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

    df["table_key"] = (
        df.groupby("table_index")["field"]
        .rank(method="first", ascending=True, na_option="top")
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
        safe_table_base = row["table_name"].replace("`", "").replace('"', "")
        if is_snowflake:
            base_model_name = safe_table_base.split(".")[-1].upper()
            model_name = base_model_name + "__" + table_index_s.upper()
            model_ref = f"{{{{ref('{base_model_name}')}}}}"
        elif is_databricks:
            base_model_name = safe_table_base.split(".")[-1]
            model_name = base_model_name + "__" + table_index_s
            model_ref = f"{{{{ref('{base_model_name}')}}}}"
        else:
            base_model_name = row["table_name"].replace("`", "").split(".")[2]
            model_name = base_model_name + "__" + table_index_s
            model_ref = f"{{{{ref('{base_model_name}')}}}}"

        if keys_df.empty == True or len(keys_df.iloc[0, 0]) == 0:
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
            .split(".")[2],
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
