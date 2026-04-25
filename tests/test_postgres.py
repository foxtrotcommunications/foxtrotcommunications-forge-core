"""
Tests for PostgreSQL adapter integration in forge-core.

Tests cover:
- Adapter factory registration
- SQL generation methods (no live connection needed)
- Profile generation
- Build context with postgres source type
- Qualified table name format

All pure-function tests — no warehouse connections required.
"""
import os
import yaml
import pytest
from types import SimpleNamespace

from forge_core.adapters import get_adapter
from forge_core.adapters.base import WarehouseAdapter
from forge_core.engine.build_context import (
    BuildContext,
    build_qualified_table_name,
    build_root_table_name,
    validate_build_context,
)
from forge_core.profiles import generate_profiles_yml


# ============================================================================
# Adapter Factory
# ============================================================================

class TestPostgresAdapterFactory:

    def test_factory_returns_postgres_adapter(self):
        adapter = get_adapter("postgres")
        assert adapter.__class__.__name__ == "PostgresAdapter"

    def test_factory_case_insensitive(self):
        adapter = get_adapter("POSTGRES")
        assert adapter.__class__.__name__ == "PostgresAdapter"

    def test_factory_postgres_is_warehouse_adapter(self):
        adapter = get_adapter("postgres")
        assert isinstance(adapter, WarehouseAdapter)

    def test_factory_updated_error_message(self):
        with pytest.raises(ValueError) as exc_info:
            get_adapter("mysql")
        assert "postgres" in str(exc_info.value)


# ============================================================================
# PostgresAdapter — SQL generation (no connection needed)
# ============================================================================

class TestPostgresAdapterSqlGeneration:

    @pytest.fixture
    def adapter(self):
        from forge_core.adapters.postgres import PostgresAdapter
        a = PostgresAdapter.__new__(PostgresAdapter)
        a.connection = None
        a.host = None
        a.port = 5432
        a.database = None
        a.user = None
        a.password = None
        a.schema = "public"
        return a

    def test_build_select_expression_scalar(self, adapter):
        expr = adapter.build_select_expression("data", "user_id", "user_id", "string")
        assert isinstance(expr, str)
        assert "user_id" in expr
        assert "json_extract_path_text" in expr

    def test_build_select_expression_object(self, adapter):
        expr = adapter.build_select_expression("data", "address", "address", "object")
        assert isinstance(expr, str)
        assert "'['" in expr or "|| '['" in expr or "'[' ||" in expr
        assert "address" in expr

    def test_build_select_expression_array(self, adapter):
        expr = adapter.build_select_expression("data", "items", "items", "array")
        assert isinstance(expr, str)
        assert "items" in expr
        assert "json_extract_path" in expr

    def test_get_rows_processed_sql(self, adapter):
        sql = adapter.get_rows_processed_sql(
            None, "my_schema", "my_table", "2024-01-01T00:00:00Z"
        )
        assert isinstance(sql, str)
        assert "my_table" in sql
        assert "my_schema" in sql

    def test_generate_rollup_sql_returns_empty(self, adapter):
        """Postgres rollup is not supported — must return empty string."""
        result = adapter.generate_rollup_sql([], "my_schema")
        assert result == ""

    def test_generate_rollup_sql_with_metadata_still_empty(self, adapter):
        """Even with real metadata, rollup returns empty for Postgres."""
        metadata = [
            {
                "model_name": "frg",
                "parent_model": None,
                "depth": 0,
                "children": [{"field_name": "items", "type": "ARRAY", "model_suffix": "item1"}],
                "scalar_fields": ["id"],
            }
        ]
        result = adapter.generate_rollup_sql(metadata, "my_schema")
        assert result == ""

    def test_default_port_is_5432(self):
        from forge_core.adapters.postgres import PostgresAdapter
        adapter = PostgresAdapter()
        assert adapter.port == 5432

    def test_default_schema_is_public(self):
        from forge_core.adapters.postgres import PostgresAdapter
        adapter = PostgresAdapter()
        assert adapter.schema == "public"

    def test_lazy_connection(self):
        """Adapter should NOT connect during __init__ (lazy init)."""
        from forge_core.adapters.postgres import PostgresAdapter
        adapter = PostgresAdapter()
        assert adapter.connection is None


# ============================================================================
# Build Context — Postgres
# ============================================================================

class TestBuildContextPostgres:

    def test_qualified_table_name_default_schema(self):
        result = build_qualified_table_name(
            "postgres", None, "my_db", "my_table", None
        )
        assert result == '"public"."my_table"'

    def test_qualified_table_name_explicit_schema(self):
        result = build_qualified_table_name(
            "postgres", None, "my_db", "my_table", "analytics"
        )
        assert result == '"analytics"."my_table"'

    def test_root_table_name(self):
        result = build_root_table_name("postgres", None, "my_schema")
        assert result == '"my_schema"."frg"'

    def test_context_computes_qualified_name(self):
        ctx = BuildContext(
            source_type="postgres",
            source_database="my_db",
            source_table_name="my_table",
            target_dataset="target_schema",
        )
        assert ctx.qualified_table_name == '"public"."my_table"'

    def test_context_root_model_is_frg(self):
        ctx = BuildContext(
            source_type="postgres",
            source_database="my_db",
            source_table_name="my_table",
            target_dataset="target_schema",
        )
        assert ctx.root_model_name == "frg"

    def test_validate_passes(self):
        ctx = BuildContext(
            source_type="postgres",
            source_database="my_db",
            source_table_name="my_table",
            target_dataset="target_schema",
        )
        ok, err = validate_build_context(ctx)
        assert ok is True
        assert err is None

    def test_validate_missing_database_fails(self):
        ctx = BuildContext(
            source_type="postgres",
            source_database="",
            source_table_name="my_table",
            target_dataset="target_schema",
        )
        ok, err = validate_build_context(ctx)
        assert ok is False


# ============================================================================
# Profiles — Postgres
# ============================================================================

class TestPostgresProfile:

    def test_creates_file(self, tmp_path):
        path = generate_profiles_yml(
            source_type="postgres",
            target_project=None,
            target_dataset="my_schema",
            project_dir=str(tmp_path),
        )
        assert os.path.exists(path)

    def test_valid_yaml(self, tmp_path):
        path = generate_profiles_yml(
            source_type="postgres",
            target_project=None,
            target_dataset="my_schema",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        assert parsed is not None

    def test_type_field(self, tmp_path):
        path = generate_profiles_yml(
            source_type="postgres",
            target_project=None,
            target_dataset="my_schema",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        output = parsed["forge"]["outputs"]["my_schema"]
        assert output["type"] == "postgres"

    def test_default_port(self, tmp_path):
        path = generate_profiles_yml(
            source_type="postgres",
            target_project=None,
            target_dataset="my_schema",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        output = parsed["forge"]["outputs"]["my_schema"]
        assert output["port"] == 5432

    def test_has_forge_key(self, tmp_path):
        path = generate_profiles_yml(
            source_type="postgres",
            target_project=None,
            target_dataset="my_schema",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        assert "forge" in parsed
        assert "outputs" in parsed["forge"]
        assert "target" in parsed["forge"]

    def test_threads_present(self, tmp_path):
        path = generate_profiles_yml(
            source_type="postgres",
            target_project=None,
            target_dataset="my_schema",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        output = parsed["forge"]["outputs"]["my_schema"]
        assert "threads" in output
        assert output["threads"] >= 1
