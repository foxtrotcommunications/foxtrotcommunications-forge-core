"""
Tests for forge_core.engine.schema

All pure-function tests — no network or warehouse connections required.
"""
import pytest
from forge_core.engine.schema import (
    compare_schemas,
    generate_mermaid_diagram,
    generate_schema_graph,
)


# ============================================================================
# compare_schemas
# ============================================================================

class TestCompareSchemas:

    def test_no_changes_returns_empty(self):
        schema = {
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
            }
        }
        changes = compare_schemas(schema, schema)
        assert changes == []

    def test_field_added(self):
        old = {"properties": {"name": {"type": "string"}}}
        new = {"properties": {"name": {"type": "string"}, "email": {"type": "string"}}}
        changes = compare_schemas(old, new)
        assert len(changes) == 1
        assert changes[0]["event_type"] == "field_added"
        assert changes[0]["field_name"] == "email"

    def test_field_removed(self):
        old = {"properties": {"name": {"type": "string"}, "email": {"type": "string"}}}
        new = {"properties": {"name": {"type": "string"}}}
        changes = compare_schemas(old, new)
        assert len(changes) == 1
        assert changes[0]["event_type"] == "field_removed"
        assert changes[0]["field_name"] == "email"

    def test_type_changed(self):
        old = {"properties": {"amount": {"type": "string"}}}
        new = {"properties": {"amount": {"type": "number"}}}
        changes = compare_schemas(old, new)
        assert len(changes) == 1
        assert changes[0]["event_type"] == "type_changed"
        assert changes[0]["field_name"] == "amount"
        assert changes[0]["old_type"] == "string"
        assert changes[0]["new_type"] == "number"

    def test_old_schema_none_treats_everything_as_added(self):
        new = {"properties": {"name": {"type": "string"}, "age": {"type": "integer"}}}
        changes = compare_schemas(None, new)
        event_types = {c["event_type"] for c in changes}
        assert event_types == {"field_added"}
        field_names = {c["field_name"] for c in changes}
        assert field_names == {"name", "age"}

    def test_nested_object_recurse(self):
        old = {
            "properties": {
                "address": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                }
            }
        }
        new = {
            "properties": {
                "address": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string"},
                        "zip": {"type": "string"},  # new nested field
                    },
                }
            }
        }
        changes = compare_schemas(old, new)
        assert any(c["field_name"] == "zip" for c in changes)
        assert any(c["event_type"] == "field_added" for c in changes)

    def test_multiple_changes(self):
        old = {
            "properties": {
                "a": {"type": "string"},
                "b": {"type": "integer"},
                "c": {"type": "string"},
            }
        }
        new = {
            "properties": {
                "a": {"type": "string"},   # unchanged
                "b": {"type": "number"},   # type changed
                "d": {"type": "boolean"},  # added (c removed)
            }
        }
        changes = compare_schemas(old, new)
        event_types = [c["event_type"] for c in changes]
        assert "type_changed" in event_types
        assert "field_added" in event_types
        assert "field_removed" in event_types


# ============================================================================
# generate_mermaid_diagram
# ============================================================================

# Minimal metadata fixture used across diagram tests
SIMPLE_METADATA = [
    {
        "model_name": "frg",
        "parent_model": None,
        "field_name": "root",
        "table_path": "frg",
        "scalar_fields": [],
        "children": [{"field_name": "items", "type": "ARRAY"}],
    },
    {
        "model_name": "frg__item1",
        "parent_model": "frg",
        "field_name": "items",
        "table_path": "frg__items",
        "scalar_fields": [{"name": "item_id", "original_type": "string"}],
        "children": [],
    },
]


class TestGenerateMermaidDiagram:

    def test_returns_string(self):
        result = generate_mermaid_diagram(SIMPLE_METADATA)
        assert isinstance(result, str)

    def test_starts_with_erdiagram(self):
        result = generate_mermaid_diagram(SIMPLE_METADATA)
        assert result.startswith("erDiagram")

    def test_contains_table_names(self):
        result = generate_mermaid_diagram(SIMPLE_METADATA)
        assert "frg" in result

    def test_contains_relationship_marker(self):
        result = generate_mermaid_diagram(SIMPLE_METADATA)
        # Relationship between parent and child should be present
        assert "contains" in result

    def test_empty_metadata_returns_base(self):
        result = generate_mermaid_diagram([])
        assert result == "erDiagram"

    def test_scalar_field_appears_in_diagram(self):
        result = generate_mermaid_diagram(SIMPLE_METADATA)
        assert "item_id" in result

    def test_legacy_string_scalar_field(self):
        """Ensure backward-compatible string scalar fields work."""
        metadata = [
            {
                "model_name": "frg",
                "parent_model": None,
                "field_name": "root",
                "table_path": "frg",
                "scalar_fields": ["legacy_field"],  # old string format
                "children": [],
            }
        ]
        result = generate_mermaid_diagram(metadata)
        assert "legacy_field" in result


# ============================================================================
# generate_schema_graph
# ============================================================================

class TestGenerateSchemaGraph:

    def test_returns_dict_with_tables_and_relationships(self):
        result = generate_schema_graph(SIMPLE_METADATA)
        assert "tables" in result
        assert "relationships" in result

    def test_tables_count_matches_metadata(self):
        result = generate_schema_graph(SIMPLE_METADATA)
        assert len(result["tables"]) == len(SIMPLE_METADATA)

    def test_relationship_from_parent_to_child(self):
        result = generate_schema_graph(SIMPLE_METADATA)
        assert len(result["relationships"]) == 1
        rel = result["relationships"][0]
        assert rel["type"] == "contains"
        assert rel["from"] != rel["to"]

    def test_columns_populated_from_scalar_fields(self):
        result = generate_schema_graph(SIMPLE_METADATA)
        # Find the child table
        child = next(t for t in result["tables"] if "item" in t["name"].lower() or "frg__" in t["name"].lower())
        col_names = [c["name"] for c in child["columns"]]
        assert "item_id" in col_names

    def test_empty_metadata(self):
        result = generate_schema_graph([])
        assert result["tables"] == []
        assert result["relationships"] == []

    def test_pk_detection_on_id_field(self):
        metadata = [
            {
                "model_name": "frg",
                "parent_model": None,
                "table_path": "frg",
                "field_name": "root",
                "scalar_fields": [{"name": "id", "original_type": "string"}],
                "children": [],
            }
        ]
        result = generate_schema_graph(metadata)
        id_col = next(c for c in result["tables"][0]["columns"] if c["name"] == "id")
        assert id_col["pk"] is True
