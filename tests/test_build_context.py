"""
Tests for forge_core.engine.build_context

All pure-function tests — no network or warehouse connections required.
"""
import pytest
from forge_core.engine.build_context import (
    BuildContext,
    build_qualified_table_name,
    build_root_table_name,
    validate_build_context,
)


# ============================================================================
# build_qualified_table_name
# ============================================================================

class TestBuildQualifiedTableName:

    def test_bigquery(self):
        result = build_qualified_table_name(
            "bigquery", "my-project", "my_dataset", "my_table", None
        )
        assert result == "`my-project.my_dataset.my_table`"

    def test_bigquery_requires_project(self):
        with pytest.raises(ValueError, match="source_project is required"):
            build_qualified_table_name("bigquery", None, "my_dataset", "my_table", None)

    def test_snowflake_default_schema(self):
        result = build_qualified_table_name(
            "snowflake", None, "MY_DB", "MY_TABLE", None
        )
        assert result == '"MY_DB"."PUBLIC"."MY_TABLE"'

    def test_snowflake_explicit_schema(self):
        result = build_qualified_table_name(
            "snowflake", None, "MY_DB", "MY_TABLE", "MY_SCHEMA"
        )
        assert result == '"MY_DB"."MY_SCHEMA"."MY_TABLE"'

    def test_databricks_default_schema(self):
        result = build_qualified_table_name(
            "databricks", None, "my_catalog", "my_table", None
        )
        assert result == "my_catalog.default.my_table"

    def test_databricks_explicit_schema(self):
        result = build_qualified_table_name(
            "databricks", None, "my_catalog", "my_table", "my_schema"
        )
        assert result == "my_catalog.my_schema.my_table"

    def test_redshift_default_schema(self):
        result = build_qualified_table_name(
            "redshift", None, "my_db", "my_table", None
        )
        assert result == '"public"."my_table"'

    def test_redshift_explicit_schema(self):
        result = build_qualified_table_name(
            "redshift", None, "my_db", "my_table", "analytics"
        )
        assert result == '"analytics"."my_table"'


# ============================================================================
# build_root_table_name
# ============================================================================

class TestBuildRootTableName:

    def test_bigquery(self):
        result = build_root_table_name("bigquery", "my-project", "my_dataset")
        assert result == "`my-project.my_dataset.frg`"

    def test_snowflake(self):
        result = build_root_table_name("snowflake", "MY_DB", "MY_DATASET")
        assert result == '"MY_DB"."MY_DATASET"."FRG"'

    def test_databricks(self):
        result = build_root_table_name("databricks", "my_catalog", "my_schema")
        assert result == "my_catalog.my_schema.frg"

    def test_redshift(self):
        result = build_root_table_name("redshift", None, "my_schema")
        assert result == '"my_schema"."frg"'


# ============================================================================
# BuildContext (computed fields)
# ============================================================================

class TestBuildContext:

    def test_bigquery_context_computes_qualified_name(self):
        ctx = BuildContext(
            source_type="bigquery",
            source_project="my-project",
            source_database="my_dataset",
            source_table_name="my_table",
            target_dataset="target_ds",
            target_project="my-project",
        )
        assert ctx.qualified_table_name == "`my-project.my_dataset.my_table`"

    def test_bigquery_root_model_name_is_frg(self):
        ctx = BuildContext(
            source_type="bigquery",
            source_project="my-project",
            source_database="my_dataset",
            source_table_name="my_table",
            target_dataset="target_ds",
            target_project="my-project",
        )
        assert ctx.root_model_name == "frg"

    def test_snowflake_root_model_name_is_FRG(self):
        ctx = BuildContext(
            source_type="snowflake",
            source_database="MY_DB",
            source_table_name="MY_TABLE",
            target_dataset="MY_TARGET",
            target_project="MY_DB",
        )
        assert ctx.root_model_name == "FRG"

    def test_context_is_immutable(self):
        ctx = BuildContext(
            source_type="bigquery",
            source_project="my-project",
            source_database="my_dataset",
            source_table_name="my_table",
            target_dataset="target_ds",
            target_project="my-project",
        )
        with pytest.raises((AttributeError, TypeError)):
            ctx.source_type = "snowflake"  # type: ignore


# ============================================================================
# validate_build_context
# ============================================================================

class TestValidateBuildContext:

    def _valid_bq_ctx(self, **overrides):
        defaults = dict(
            source_type="bigquery",
            source_project="my-project",
            source_database="my_dataset",
            source_table_name="my_table",
            target_dataset="target_ds",
            target_project="my-project",
        )
        defaults.update(overrides)
        return BuildContext(**defaults)

    def test_valid_bigquery_passes(self):
        ctx = self._valid_bq_ctx()
        ok, err = validate_build_context(ctx)
        assert ok is True
        assert err is None

    def test_bigquery_missing_project_fails(self):
        # BuildContext raises ValueError at construction because __post_init__
        # calls build_qualified_table_name, which enforces the project requirement
        with pytest.raises(ValueError, match="source_project is required"):
            BuildContext(
                source_type="bigquery",
                source_project=None,
                source_database="my_dataset",
                source_table_name="my_table",
                target_dataset="target_ds",
            )

    def test_validate_rejects_missing_project_via_ctx(self):
        # validate_build_context independently checks for missing source_project
        # when source_type is bigquery — simulate via a mock-like valid ctx with
        # source_project explicitly empty and BigQuery type
        # (build_qualified_table_name already caught it, so validate_build_context
        # serves as the belt after the suspenders)
        # We test validate_build_context directly with a valid context first,
        # then confirm it would have caught it
        BuildContext(
            source_type="bigquery",
            source_project="my-project",
            source_database="my_dataset",
            source_table_name="my_table",
            target_dataset="target_ds",
            target_project="my-project",
        )
        # Now artificially blank out source_project to test validate logic
        ctx_dict = {
            "source_type": "bigquery",
            "source_project": None,  # <-- missing
            "source_database": "my_dataset",
            "source_table_name": "my_table",
            "target_dataset": "target_ds",
        }
        # validate_build_context takes a BuildContext — use a simplenamespace mock
        from types import SimpleNamespace
        fake_ctx = SimpleNamespace(**ctx_dict)
        ok, err = validate_build_context(fake_ctx)
        assert ok is False
        assert "source_project" in err

    def test_missing_source_table_fails(self):
        ctx = BuildContext(
            source_type="bigquery",
            source_project="my-project",
            source_database="my_dataset",
            source_table_name="",
            target_dataset="target_ds",
        )
        ok, err = validate_build_context(ctx)
        assert ok is False
        assert err is not None

    def test_missing_target_dataset_fails(self):
        ctx = BuildContext(
            source_type="bigquery",
            source_project="my-project",
            source_database="my_dataset",
            source_table_name="my_table",
            target_dataset="",
        )
        ok, err = validate_build_context(ctx)
        assert ok is False

    def test_valid_snowflake_passes(self):
        ctx = BuildContext(
            source_type="snowflake",
            source_database="MY_DB",
            source_table_name="MY_TABLE",
            target_dataset="MY_TARGET",
            target_project="MY_DB",
        )
        ok, err = validate_build_context(ctx)
        assert ok is True

    def test_valid_redshift_passes(self):
        ctx = BuildContext(
            source_type="redshift",
            source_database="my_db",
            source_table_name="my_table",
            target_dataset="my_target",
        )
        ok, err = validate_build_context(ctx)
        assert ok is True
