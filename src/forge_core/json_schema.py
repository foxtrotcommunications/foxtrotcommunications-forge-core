#!/usr/bin/env python
# coding: utf-8

"""
Converts Forge metadata to standard JSON Schema format.
"""

import json
from typing import Dict, List, Any


def metadata_to_json_schema(
    all_metadata: List[Dict[str, Any]], schema_title: str = "Discovered Schema"
) -> Dict[str, Any]:
    """
    Converts Forge metadata structure to a standard JSON Schema document.

    Args:
        all_metadata: List of metadata dictionaries from Forge's build process
        schema_title: Title for the JSON Schema document

    Returns:
        A JSON Schema document (dict)
    """

    # Create a lookup map by model_name for easy access
    metadata_map = {m["model_name"]: m for m in all_metadata}

    # Find the root model
    root_model = next((m for m in all_metadata if m["parent_model"] is None), None)
    if not root_model:
        raise ValueError("No root model found in metadata")

    # Build the schema recursively
    # Build the schema recursively
    try:
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": schema_title,
            "type": "object",
            "properties": build_properties(root_model, metadata_map),
        }
    except Exception as e:
        print(f"Error building schema: {e}")
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": schema_title,
            "type": "object",
            "properties": {},
        }

    return schema


def build_properties(
    model: Dict[str, Any], metadata_map: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Recursively builds the properties section of a JSON Schema from a model.

    Args:
        model: The current model metadata
        metadata_map: Map of all models by model_name

    Returns:
        Dictionary of properties for this model
    """
    properties = {}

    # Add scalar fields
    for field in model["scalar_fields"]:
        # Handle field being a dict (enriched) or plain string
        if isinstance(field, dict):
            field_name = field.get("name") or field.get("field_name", "")
        else:
            field_name = field

        if not field_name:
            continue

        # Use table_path from metadata if available, otherwise fallback to model_name
        base_path = model.get("table_path", model["model_name"])

        properties[field_name] = {
            "type": "string",
            "description": f"Scalar field from {model['model_name']}",
            "x-forge-source": {
                "model": model["model_name"],
                "field": field_name,
                "path": f"{base_path}.{field_name}",
            },
        }

    # Add child objects and arrays
    for child in model["children"]:
        field_name = child["field_name"]
        child_model_name = f"{model['model_name']}__{child['model_suffix']}"

        # Use table_path from metadata if available, otherwise fallback to model_name
        base_path = model.get("table_path", model["model_name"])

        if child_model_name in metadata_map:
            child_model = metadata_map[child_model_name]

            if child["type"] == "STRUCT":
                # Nested object
                properties[field_name] = {
                    "type": "object",
                    "description": f"Nested object from {child_model_name}",
                    "x-forge-source": {
                        "model": model["model_name"],
                        "field": field_name,
                        "child_model": child_model_name,
                        "path": f"{base_path}.{field_name}",
                    },
                    "properties": build_properties(child_model, metadata_map),
                }
            elif child["type"] == "ARRAY":
                # Array of objects
                properties[field_name] = {
                    "type": "array",
                    "description": f"Array from {child_model_name}",
                    "x-forge-source": {
                        "model": model["model_name"],
                        "field": field_name,
                        "child_model": child_model_name,
                        "path": f"{base_path}.{field_name}",
                    },
                    "items": {
                        "type": "object",
                        "properties": build_properties(child_model, metadata_map),
                    },
                }
        else:
            # Child model not found, create a placeholder
            if child["type"] == "STRUCT":
                properties[field_name] = {
                    "type": "object",
                    "description": f"Nested object (model not found: {child_model_name})",
                }
            elif child["type"] == "ARRAY":
                properties[field_name] = {
                    "type": "array",
                    "description": f"Array (model not found: {child_model_name})",
                    "items": {"type": "object"},
                }

    return properties


def save_json_schema(
    all_metadata: List[Dict[str, Any]],
    output_file: str,
    schema_title: str = "Discovered Schema",
):
    """
    Converts metadata to JSON Schema and saves it to a file.

    Args:
        all_metadata: List of metadata dictionaries
        output_file: Path to save the JSON Schema file
        schema_title: Title for the schema
    """
    schema = metadata_to_json_schema(all_metadata, schema_title)

    with open(output_file, "w") as f:
        json.dump(schema, f, indent=2)

    print(f"JSON Schema saved to: {output_file}")
    return schema


# Example usage
if __name__ == "__main__":
    # Example metadata (from the user's Untitled-1 file)
    example_metadata = [
        {
            "model_name": "frg__root",
            "parent_model": None,
            "field_name": "root",
            "is_array": False,
            "scalar_fields": [],
            "children": [
                {
                    "field_name": "JSON_FIELD",
                    "type": "STRUCT",
                    "model_suffix": "JSON_FIELD",
                }
            ],
            "depth": 0,
        },
        {
            "model_name": "frg__root__JSON_FIELD",
            "parent_model": "frg__root",
            "field_name": "JSON_FIELD",
            "is_array": False,
            "scalar_fields": ["patientblood_pressure"],
            "children": [
                {"field_name": "drug", "type": "STRUCT", "model_suffix": "drug1"},
                {"field_name": "patient", "type": "STRUCT", "model_suffix": "pati1"},
            ],
            "depth": 1,
        },
        {
            "model_name": "frg__root__JSON_FIELD__drug1",
            "parent_model": "frg__root__JSON_FIELD",
            "field_name": "drug",
            "is_array": False,
            "scalar_fields": ["company", "name_brand", "name_generic"],
            "children": [],
            "depth": 2,
        },
        {
            "model_name": "frg__root__JSON_FIELD__pati1",
            "parent_model": "frg__root__JSON_FIELD",
            "field_name": "patient",
            "is_array": False,
            "scalar_fields": ["name", "id", "race", "age"],
            "children": [],
            "depth": 2,
        },
    ]

    # Convert and print
    schema = metadata_to_json_schema(example_metadata, "Pharma Testing Schema")
    print(json.dumps(schema, indent=2))
