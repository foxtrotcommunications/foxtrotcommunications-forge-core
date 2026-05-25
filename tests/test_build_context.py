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
        assert result == "`my-project.my_dataset.root`"

    def test_snowflake(self):
        result = build_root_table_name("snowflake", "MY_DB", "MY_DATASET")
        assert result == '"MY_DB"."MY_DATASET"."ROOT"'

    def test_databricks(self):
        result = build_root_table_name("databricks", "my_catalog", "my_schema")
        assert result == "my_catalog.my_schema.root"

    def test_redshift(self):
        result = build_root_table_name("redshift", None, "my_schema")
        assert result == '"my_schema"."root"'


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

    def test_bigquery_root_model_name_is_root(self):
        ctx = BuildContext(
            source_type="bigquery",
            source_project="my-project",
            source_database="my_dataset",
            source_table_name="my_table",
            target_dataset="target_ds",
            target_project="my-project",
        )
        assert ctx.root_model_name == "root"

    def test_snowflake_root_model_name_is_ROOT(self):
        ctx = BuildContext(
            source_type="snowflake",
            source_database="MY_DB",
            source_table_name="MY_TABLE",
            target_dataset="MY_TARGET",
            target_project="MY_DB",
        )
        assert ctx.root_model_name == "ROOT"

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


# ============================================================================
# Model Prefix
# ============================================================================

class TestModelPrefix:
    """Tests for custom model_prefix behavior."""

    def test_default_no_prefix_bigquery(self):
        ctx = BuildContext(
            source_type="bigquery",
            source_project="my-project",
            source_database="my_dataset",
            source_table_name="my_table",
            target_dataset="target_ds",
            target_project="my-project",
        )
        assert ctx.root_model_name == "root"
        assert ctx.model_prefix is None

    def test_custom_prefix_bigquery(self):
        ctx = BuildContext(
            source_type="bigquery",
            source_project="my-project",
            source_database="my_dataset",
            source_table_name="my_table",
            target_dataset="target_ds",
            target_project="my-project",
            model_prefix="ishgt_job_offers",
        )
        assert ctx.root_model_name == "ishgt_job_offers"

    def test_custom_prefix_snowflake_uppercased(self):
        ctx = BuildContext(
            source_type="snowflake",
            source_database="MY_DB",
            source_table_name="MY_TABLE",
            target_dataset="MY_TARGET",
            target_project="MY_DB",
            model_prefix="ishgt_job_offers",
        )
        assert ctx.root_model_name == "ISHGT_JOB_OFFERS"

    def test_custom_prefix_databricks(self):
        ctx = BuildContext(
            source_type="databricks",
            source_database="my_catalog",
            source_table_name="my_table",
            target_dataset="my_target",
            target_project="my_catalog",
            model_prefix="fhir_patients",
        )
        assert ctx.root_model_name == "fhir_patients"

    def test_custom_prefix_redshift(self):
        ctx = BuildContext(
            source_type="redshift",
            source_database="my_db",
            source_table_name="my_table",
            target_dataset="my_target",
            model_prefix="api_responses",
        )
        assert ctx.root_model_name == "api_responses"

    def test_prefix_changes_root_table_name_bigquery(self):
        ctx = BuildContext(
            source_type="bigquery",
            source_project="my-project",
            source_database="my_dataset",
            source_table_name="my_table",
            target_dataset="target_ds",
            target_project="my-project",
            model_prefix="ishgt_job_offers",
        )
        assert ctx.root_table_name_for_keys == "`my-project.target_ds.ishgt_job_offers`"

    def test_prefix_changes_root_table_name_snowflake(self):
        ctx = BuildContext(
            source_type="snowflake",
            source_database="MY_DB",
            source_table_name="MY_TABLE",
            target_dataset="MY_TARGET",
            target_project="MY_DB",
            model_prefix="ishgt_job_offers",
        )
        assert ctx.root_table_name_for_keys == '"MY_DB"."MY_TARGET"."ISHGT_JOB_OFFERS"'


class TestModelPrefixValidation:
    """Tests for model_prefix validation in validate_build_context."""

    def _ctx_with_prefix(self, prefix):
        return BuildContext(
            source_type="bigquery",
            source_project="my-project",
            source_database="my_dataset",
            source_table_name="my_table",
            target_dataset="target_ds",
            target_project="my-project",
            model_prefix=prefix,
        )

    def test_valid_prefix_passes(self):
        ctx = self._ctx_with_prefix("ishgt_job_offers")
        ok, err = validate_build_context(ctx)
        assert ok is True
        assert err is None

    def test_valid_prefix_with_numbers(self):
        ctx = self._ctx_with_prefix("api_v2_responses")
        ok, err = validate_build_context(ctx)
        assert ok is True

    def test_valid_prefix_single_word(self):
        ctx = self._ctx_with_prefix("patients")
        ok, err = validate_build_context(ctx)
        assert ok is True

    def test_valid_prefix_underscore_start(self):
        ctx = self._ctx_with_prefix("_internal")
        ok, err = validate_build_context(ctx)
        assert ok is True

    def test_double_underscore_rejected(self):
        ctx = self._ctx_with_prefix("foo__bar")
        ok, err = validate_build_context(ctx)
        assert ok is False
        assert "__" in err

    def test_hyphen_rejected(self):
        ctx = self._ctx_with_prefix("foo-bar")
        ok, err = validate_build_context(ctx)
        assert ok is False

    def test_space_rejected(self):
        ctx = self._ctx_with_prefix("foo bar")
        ok, err = validate_build_context(ctx)
        assert ok is False

    def test_starts_with_digit_rejected(self):
        ctx = self._ctx_with_prefix("1_bad_prefix")
        ok, err = validate_build_context(ctx)
        assert ok is False

    def test_none_prefix_passes(self):
        ctx = self._ctx_with_prefix(None)
        ok, err = validate_build_context(ctx)
        assert ok is True


class TestBuildRootTableNameWithPrefix:
    """Tests for build_root_table_name with custom root_model_name."""

    def test_bigquery_default(self):
        result = build_root_table_name("bigquery", "my-project", "my_dataset")
        assert result == "`my-project.my_dataset.root`"

    def test_bigquery_custom(self):
        result = build_root_table_name(
            "bigquery", "my-project", "my_dataset", "ishgt_job_offers"
        )
        assert result == "`my-project.my_dataset.ishgt_job_offers`"

    def test_snowflake_custom(self):
        result = build_root_table_name(
            "snowflake", "MY_DB", "MY_DATASET", "ISHGT_JOB_OFFERS"
        )
        assert result == '"MY_DB"."MY_DATASET"."ISHGT_JOB_OFFERS"'

    def test_databricks_custom(self):
        result = build_root_table_name(
            "databricks", "my_catalog", "my_schema", "fhir_patients"
        )
        assert result == "my_catalog.my_schema.fhir_patients"

    def test_redshift_custom(self):
        result = build_root_table_name(
            "redshift", None, "my_schema", "api_responses"
        )
        assert result == '"my_schema"."api_responses"'

