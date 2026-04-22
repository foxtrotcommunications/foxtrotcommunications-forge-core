"""
Tests for forge_core.json_schema (metadata_to_json_schema)
and forge_core.schema_writer (write_schema_yml)

All pure-function / filesystem tests — no warehouse connections required.
"""
import os
import yaml
import pytest
from forge_core.json_schema import metadata_to_json_schema
from forge_core.schema_writer import write_schema_yml


# ============================================================================
# Shared fixtures
# ============================================================================

def _flat_metadata():
    """Single-level metadata: root with scalar fields only."""
    return [
        {
            "model_name": "frg",
            "parent_model": None,
            "field_name": "root",
            "table_path": "frg",
            "is_array": False,
            "scalar_fields": ["user_id", "email", "created_at"],
            "children": [],
            "depth": 0,
        }
    ]


def _nested_metadata():
    """Two-level metadata: root → orders (array) → scalar fields."""
    return [
        {
            "model_name": "frg",
            "parent_model": None,
            "field_name": "root",
            "table_path": "frg",
            "is_array": False,
            "scalar_fields": ["user_id"],
            "children": [
                {"field_name": "orders", "type": "ARRAY", "model_suffix": "orde1"}
            ],
            "depth": 0,
        },
        {
            "model_name": "frg__orde1",
            "parent_model": "frg",
            "field_name": "orders",
            "table_path": "frg__orders",
            "is_array": True,
            "scalar_fields": [
                {"name": "order_id", "original_type": "string"},
                {"name": "amount", "original_type": "number"},
            ],
            "children": [],
            "depth": 1,
        },
    ]


# ============================================================================
# metadata_to_json_schema
# ============================================================================

class TestMetadataToJsonSchema:

    def test_returns_dict_with_schema_key(self):
        result = metadata_to_json_schema(_flat_metadata())
        assert "$schema" in result
        assert result["$schema"] == "http://json-schema.org/draft-07/schema#"

    def test_root_type_is_object(self):
        result = metadata_to_json_schema(_flat_metadata())
        assert result["type"] == "object"

    def test_scalar_fields_appear_in_properties(self):
        result = metadata_to_json_schema(_flat_metadata())
        props = result["properties"]
        assert "user_id" in props
        assert "email" in props
        assert "created_at" in props

    def test_custom_title_applied(self):
        result = metadata_to_json_schema(_flat_metadata(), schema_title="My API")
        assert result["title"] == "My API"

    def test_nested_array_appears_as_array_type(self):
        result = metadata_to_json_schema(_nested_metadata())
        props = result["properties"]
        assert "orders" in props
        assert props["orders"]["type"] == "array"

    def test_nested_array_has_items(self):
        result = metadata_to_json_schema(_nested_metadata())
        orders = result["properties"]["orders"]
        assert "items" in orders
        assert orders["items"]["type"] == "object"

    def test_nested_scalar_fields_inside_array(self):
        result = metadata_to_json_schema(_nested_metadata())
        order_props = result["properties"]["orders"]["items"]["properties"]
        assert "order_id" in order_props
        assert "amount" in order_props

    def test_no_root_model_raises(self):
        # Metadata with no parent_model=None entry
        bad_metadata = [
            {
                "model_name": "frg__orde1",
                "parent_model": "frg",
                "field_name": "orders",
                "table_path": "frg__orders",
                "scalar_fields": [],
                "children": [],
            }
        ]
        with pytest.raises(ValueError, match="No root model"):
            metadata_to_json_schema(bad_metadata)

    def test_x_forge_source_annotation_present(self):
        result = metadata_to_json_schema(_flat_metadata())
        user_id_prop = result["properties"].get("user_id", {})
        assert "x-forge-source" in user_id_prop
        assert user_id_prop["x-forge-source"]["field"] == "user_id"

    def test_dict_scalar_fields_supported(self):
        """metadata may have scalar_fields as list of dicts (enriched format)."""
        metadata = [
            {
                "model_name": "frg",
                "parent_model": None,
                "field_name": "root",
                "table_path": "frg",
                "scalar_fields": [
                    {"name": "user_id", "original_type": "string"},
                    {"name": "score", "original_type": "float"},
                ],
                "children": [],
            }
        ]
        result = metadata_to_json_schema(metadata)
        assert "user_id" in result["properties"]
        assert "score" in result["properties"]


# ============================================================================
# write_schema_yml
# ============================================================================

class TestWriteSchemaYml:

    def test_creates_file(self, tmp_path):
        output = str(tmp_path / "schema.yml")
        write_schema_yml(_flat_metadata(), output)
        assert os.path.exists(output)

    def test_valid_yaml(self, tmp_path):
        output = str(tmp_path / "schema.yml")
        write_schema_yml(_flat_metadata(), output)
        with open(output) as f:
            parsed = yaml.safe_load(f)
        assert parsed is not None

    def test_version_is_2(self, tmp_path):
        output = str(tmp_path / "schema.yml")
        write_schema_yml(_flat_metadata(), output)
        with open(output) as f:
            parsed = yaml.safe_load(f)
        assert parsed["version"] == 2

    def test_models_list_present(self, tmp_path):
        output = str(tmp_path / "schema.yml")
        write_schema_yml(_flat_metadata(), output)
        with open(output) as f:
            parsed = yaml.safe_load(f)
        assert "models" in parsed
        assert len(parsed["models"]) == 1

    def test_model_name_correct(self, tmp_path):
        output = str(tmp_path / "schema.yml")
        write_schema_yml(_flat_metadata(), output)
        with open(output) as f:
            parsed = yaml.safe_load(f)
        assert parsed["models"][0]["name"] == "frg"

    def test_scalar_columns_listed(self, tmp_path):
        output = str(tmp_path / "schema.yml")
        write_schema_yml(_flat_metadata(), output)
        with open(output) as f:
            parsed = yaml.safe_load(f)
        col_names = [c["name"] for c in parsed["models"][0].get("columns", [])]
        assert "user_id" in col_names
        assert "email" in col_names

    def test_nested_metadata_produces_two_models(self, tmp_path):
        output = str(tmp_path / "schema.yml")
        write_schema_yml(_nested_metadata(), output)
        with open(output) as f:
            parsed = yaml.safe_load(f)
        assert len(parsed["models"]) == 2

    def test_dict_format_scalar_fields(self, tmp_path):
        """Enriched dict format for scalar_fields should produce correct column names."""
        metadata = [
            {
                "model_name": "frg",
                "parent_model": None,
                "field_name": "root",
                "table_path": "frg",
                "scalar_fields": [
                    {"name": "order_id", "original_type": "STRING"},
                    {"name": "amount", "original_type": "FLOAT"},
                ],
                "children": [],
            }
        ]
        output = str(tmp_path / "schema.yml")
        write_schema_yml(metadata, output)
        with open(output) as f:
            parsed = yaml.safe_load(f)
        col_names = [c["name"] for c in parsed["models"][0].get("columns", [])]
        assert "order_id" in col_names
        assert "amount" in col_names

    def test_child_struct_appears_in_columns(self, tmp_path):
        output = str(tmp_path / "schema.yml")
        write_schema_yml(_nested_metadata(), output)
        with open(output) as f:
            parsed = yaml.safe_load(f)
        root_model = next(m for m in parsed["models"] if m["name"] == "frg")
        col_names = [c["name"] for c in root_model.get("columns", [])]
        assert "orders" in col_names

    def test_returns_path(self, tmp_path):
        output = str(tmp_path / "schema.yml")
        returned = write_schema_yml(_flat_metadata(), output)
        assert returned == output
