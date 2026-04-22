"""
Tests for forge_core.profiles (generate_profiles_yml)

Tests use tmp_path to avoid writing to the real filesystem.
No warehouse connections required.
"""
import os
import yaml
import pytest

from forge_core.profiles import generate_profiles_yml


class TestGenerateProfilesYml:

    # -------------------------------------------------------------------------
    # BigQuery
    # -------------------------------------------------------------------------

    def test_bigquery_creates_file(self, tmp_path):
        path = generate_profiles_yml(
            source_type="bigquery",
            target_project="my-project",
            target_dataset="my_dataset",
            project_dir=str(tmp_path),
        )
        assert os.path.exists(path)

    def test_bigquery_valid_yaml(self, tmp_path):
        path = generate_profiles_yml(
            source_type="bigquery",
            target_project="my-project",
            target_dataset="my_dataset",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        assert parsed is not None

    def test_bigquery_type_field(self, tmp_path):
        path = generate_profiles_yml(
            source_type="bigquery",
            target_project="my-project",
            target_dataset="my_dataset",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        output = parsed["forge"]["outputs"]["my_dataset"]
        assert output["type"] == "bigquery"

    def test_bigquery_case_insensitive(self, tmp_path):
        path = generate_profiles_yml(
            source_type="BigQuery",
            target_project="my-project",
            target_dataset="my_dataset",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        output = parsed["forge"]["outputs"]["my_dataset"]
        assert output["type"] == "bigquery"

    def test_bigquery_location_included_when_provided(self, tmp_path):
        path = generate_profiles_yml(
            source_type="bigquery",
            target_project="my-project",
            target_dataset="my_dataset",
            project_dir=str(tmp_path),
            location="US",
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        output = parsed["forge"]["outputs"]["my_dataset"]
        assert output.get("location") == "US"

    def test_bigquery_no_location_omitted(self, tmp_path):
        path = generate_profiles_yml(
            source_type="bigquery",
            target_project="my-project",
            target_dataset="my_dataset",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        output = parsed["forge"]["outputs"]["my_dataset"]
        assert "location" not in output

    # -------------------------------------------------------------------------
    # Snowflake
    # -------------------------------------------------------------------------

    def test_snowflake_creates_file(self, tmp_path):
        path = generate_profiles_yml(
            source_type="snowflake",
            target_project="MY_DB",
            target_dataset="MY_SCHEMA",
            project_dir=str(tmp_path),
        )
        assert os.path.exists(path)

    def test_snowflake_type_field(self, tmp_path):
        path = generate_profiles_yml(
            source_type="snowflake",
            target_project="MY_DB",
            target_dataset="MY_SCHEMA",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        output = parsed["forge"]["outputs"]["MY_SCHEMA"]
        assert output["type"] == "snowflake"

    # -------------------------------------------------------------------------
    # Databricks
    # -------------------------------------------------------------------------

    def test_databricks_creates_file(self, tmp_path):
        path = generate_profiles_yml(
            source_type="databricks",
            target_project="my_catalog",
            target_dataset="my_schema",
            project_dir=str(tmp_path),
        )
        assert os.path.exists(path)

    def test_databricks_type_field(self, tmp_path):
        path = generate_profiles_yml(
            source_type="databricks",
            target_project="my_catalog",
            target_dataset="my_schema",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        output = parsed["forge"]["outputs"]["my_schema"]
        assert output["type"] == "databricks"

    # -------------------------------------------------------------------------
    # Redshift
    # -------------------------------------------------------------------------

    def test_redshift_creates_file(self, tmp_path):
        path = generate_profiles_yml(
            source_type="redshift",
            target_project=None,
            target_dataset="my_schema",
            project_dir=str(tmp_path),
        )
        assert os.path.exists(path)

    def test_redshift_type_field(self, tmp_path):
        path = generate_profiles_yml(
            source_type="redshift",
            target_project=None,
            target_dataset="my_schema",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        output = parsed["forge"]["outputs"]["my_schema"]
        assert output["type"] == "redshift"

    def test_redshift_default_port(self, tmp_path):
        path = generate_profiles_yml(
            source_type="redshift",
            target_project=None,
            target_dataset="my_schema",
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        output = parsed["forge"]["outputs"]["my_schema"]
        assert output["port"] == 5439

    # -------------------------------------------------------------------------
    # Unsupported type
    # -------------------------------------------------------------------------

    def test_unsupported_source_type_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Unsupported source_type"):
            generate_profiles_yml(
                source_type="oracle",
                target_project=None,
                target_dataset="my_schema",
                project_dir=str(tmp_path),
            )

    # -------------------------------------------------------------------------
    # Profile structure common contract
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("source_type,project,dataset", [
        ("bigquery", "my-project", "my_dataset"),
        ("snowflake", "MY_DB", "MY_SCHEMA"),
        ("databricks", "my_catalog", "my_schema"),
        ("redshift", None, "my_schema"),
    ])
    def test_profile_has_forge_key(self, source_type, project, dataset, tmp_path):
        path = generate_profiles_yml(
            source_type=source_type,
            target_project=project,
            target_dataset=dataset,
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        assert "forge" in parsed
        assert "outputs" in parsed["forge"]
        assert "target" in parsed["forge"]

    @pytest.mark.parametrize("source_type,project,dataset", [
        ("bigquery", "my-project", "my_dataset"),
        ("snowflake", "MY_DB", "MY_SCHEMA"),
        ("databricks", "my_catalog", "my_schema"),
        ("redshift", None, "my_schema"),
    ])
    def test_threads_present_in_output(self, source_type, project, dataset, tmp_path):
        path = generate_profiles_yml(
            source_type=source_type,
            target_project=project,
            target_dataset=dataset,
            project_dir=str(tmp_path),
        )
        with open(path) as f:
            parsed = yaml.safe_load(f)
        outputs = parsed["forge"]["outputs"]
        output = list(outputs.values())[0]
        assert "threads" in output
        assert output["threads"] >= 1
