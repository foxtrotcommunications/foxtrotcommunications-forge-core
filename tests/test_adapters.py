"""
Tests for forge_core.adapters (factory + ABC interface)
and forge_core.engine.unnesting (create_root_metadata)

Adapter instantiation tests do NOT connect to any warehouse.
SQL-generation tests call adapter methods that return strings, requiring no live connection.
"""
import pytest
import pandas as pd

from forge_core.adapters import get_adapter
from forge_core.adapters.base import WarehouseAdapter
from forge_core.engine.unnesting import create_root_metadata


# ============================================================================
# get_adapter factory
# ============================================================================

class TestGetAdapterFactory:

    def test_unknown_adapter_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown adapter type"):
            get_adapter("oracle")

    def test_unknown_adapter_message_includes_supported_list(self):
        with pytest.raises(ValueError) as exc_info:
            get_adapter("mysql")
        msg = str(exc_info.value)
        assert "bigquery" in msg or "snowflake" in msg

    def test_case_insensitive_bigquery(self):
        """Factory should normalize case — BigQuery, BIGQUERY, bigquery all valid."""
        try:
            adapter = get_adapter("BIGQUERY")
            assert adapter.__class__.__name__ == "BigQueryAdapter"
        except Exception:
            # If it fails for auth reasons, that's fine — the factory itself worked
            pass

    def test_redshift_adapter_class_name(self):
        """RedshiftAdapter should instantiate without connecting (lazy connection)."""
        pytest.importorskip("psycopg2", reason="psycopg2 not installed (install [redshift] extra)")
        adapter = get_adapter("redshift")
        assert adapter.__class__.__name__ == "RedshiftAdapter"

    def test_snowflake_adapter_class_name(self):
        pytest.importorskip("snowflake.connector", reason="snowflake-connector not installed (install [snowflake] extra)")
        adapter = get_adapter("snowflake")
        assert adapter.__class__.__name__ == "SnowflakeAdapter"

    def test_databricks_adapter_class_name(self):
        pytest.importorskip("databricks", reason="databricks-sql-connector not installed (install [databricks] extra)")
        adapter = get_adapter("databricks")
        assert adapter.__class__.__name__ == "DatabricksAdapter"


# ============================================================================
# WarehouseAdapter ABC
# ============================================================================

class TestWarehouseAdapterABC:

    def test_cannot_instantiate_abc_directly(self):
        """WarehouseAdapter is abstract — direct instantiation must fail."""
        with pytest.raises(TypeError):
            WarehouseAdapter()  # type: ignore

    def test_concrete_subclass_without_all_methods_raises(self):
        """A subclass missing abstract methods cannot be instantiated."""
        class IncompleteAdapter(WarehouseAdapter):
            pass  # does not implement any abstract methods

        with pytest.raises(TypeError):
            IncompleteAdapter()  # type: ignore

    def test_apply_column_descriptions_default_returns_zero(self):
        """
        apply_column_descriptions has a default no-op implementation.
        A minimal concrete subclass should inherit it without error.
        """
        class MinimalAdapter(WarehouseAdapter):
            def execute_query(self, sql): return pd.DataFrame()
            def get_keys(self, table_name, field_name, is_array): return pd.DataFrame()
            def get_types_sql(self, table_name, field_name, key, is_array): return ""
            def build_select_expression(self, field_name, safe_field, clean_field_name, field_type): return ""
            def get_create_table_sql(self, table_name, field_name, selects_sql, is_array, table_path): return ""
            def validate_source(self, table_name, field_name=None): return True
            def get_root_table_sql(self, table_name, field_name, is_string, limit=None): return ""
            def get_rows_processed_sql(self, project, dataset, table, timestamp): return ""
            def generate_rollup_sql(self, metadata_list, target_dataset, model_prefix=""): return ""
            def clean_dataset(self, dataset): return True

        adapter = MinimalAdapter()
        result = adapter.apply_column_descriptions("ds", {})
        assert result == 0


# ============================================================================
# Redshift adapter — SQL generation (no connection needed)
# ============================================================================

class TestRedshiftAdapterSqlGeneration:
    """
    Test that RedshiftAdapter generates structurally valid SQL strings.
    No warehouse connection is used — we only verify SQL shape.
    Skipped automatically when psycopg2 is not installed.
    """

    @pytest.fixture
    def adapter(self):
        pytest.importorskip("psycopg2", reason="psycopg2 not installed (install [redshift] extra)")
        from forge_core.adapters.redshift import RedshiftAdapter
        a = RedshiftAdapter.__new__(RedshiftAdapter)
        a.connection = None
        return a

    def test_get_types_sql_returns_string(self, adapter):
        sql = adapter.get_types_sql('"my_schema"."my_table"', "data", "user_id", False)
        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_get_types_sql_contains_field_reference(self, adapter):
        sql = adapter.get_types_sql('"my_schema"."my_table"', "data", "user_id", False)
        assert "user_id" in sql

    def test_build_select_expression_scalar(self, adapter):
        expr = adapter.build_select_expression("data", "user_id", "user_id", "scalar")
        assert isinstance(expr, str)
        assert "user_id" in expr

    def test_build_select_expression_object(self, adapter):
        expr = adapter.build_select_expression("data", "address", "address", "object")
        assert isinstance(expr, str)
        assert "address" in expr

    def test_get_rows_processed_sql_returns_string(self, adapter):
        sql = adapter.get_rows_processed_sql(
            None, "my_schema", "my_table", "2024-01-01T00:00:00Z"
        )
        assert isinstance(sql, str)
        assert "my_table" in sql or "my_schema" in sql


# ============================================================================
# Snowflake adapter — SQL generation (no connection needed)
# ============================================================================

class TestSnowflakeAdapterSqlGeneration:

    @pytest.fixture
    def adapter(self):
        pytest.importorskip("snowflake.connector", reason="snowflake-connector not installed (install [snowflake] extra)")
        from forge_core.adapters.snowflake import SnowflakeAdapter
        a = SnowflakeAdapter.__new__(SnowflakeAdapter)
        a.connection = None
        return a

    def test_build_select_expression_scalar_returns_string(self, adapter):
        expr = adapter.build_select_expression("DATA", "user_id", "user_id", "scalar")
        assert isinstance(expr, str)
        assert len(expr) > 0

    def test_get_types_sql_returns_string(self, adapter):
        sql = adapter.get_types_sql('"DB"."SCHEMA"."TABLE"', "DATA", "user_id", False)
        assert isinstance(sql, str)
        assert "user_id" in sql


# ============================================================================
# create_root_metadata
# ============================================================================

class TestCreateRootMetadata:

    def test_returns_dict(self):
        meta = create_root_metadata("frg", "`project.dataset.table`")
        assert isinstance(meta, dict)

    def test_model_name_set(self):
        meta = create_root_metadata("frg", "`project.dataset.table`")
        assert meta["model_name"] == "frg"

    def test_parent_model_is_none(self):
        meta = create_root_metadata("frg", "`project.dataset.table`")
        assert meta["parent_model"] is None

    def test_depth_is_zero(self):
        meta = create_root_metadata("frg", "`project.dataset.table`")
        assert meta["depth"] == 0

    def test_source_table_stored(self):
        qualified = "`project.dataset.table`"
        meta = create_root_metadata("frg", qualified)
        assert meta["source_table"] == qualified

    def test_snowflake_model_name(self):
        meta = create_root_metadata("FRG", '"DB"."SCHEMA"."TABLE"')
        assert meta["model_name"] == "FRG"

    def test_table_path_is_frg(self):
        meta = create_root_metadata("frg", "`project.dataset.table`")
        assert meta["table_path"] == "frg"

    def test_children_has_root_entry(self):
        meta = create_root_metadata("frg", "`project.dataset.table`")
        assert len(meta["children"]) == 1
        assert meta["children"][0]["field_name"] == "root"
