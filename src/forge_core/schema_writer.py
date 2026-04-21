"""
Forge Core — Structural Schema Writer

Generates a minimal schema.yml listing models and columns without AI descriptions.
Replaces the 646-line schema_yaml_generator.py from Forge SaaS.
"""

import os
import yaml
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def write_schema_yml(
    all_metadata: List[Dict[str, Any]],
    output_path: str = "./forge_project/models/schema.yml",
) -> str:
    """
    Generate a structural schema.yml from build metadata.

    Lists model names and column names/types only. No AI-generated descriptions,
    no AI descriptions or enrichment.

    Args:
        all_metadata: List of metadata dictionaries from the build process
        output_path: Path to write schema.yml

    Returns:
        Path to the generated schema.yml
    """
    models = []

    for meta in all_metadata:
        model_name = meta.get("model_name", "")
        if not model_name:
            continue

        columns = []

        # Add scalar fields
        for field in meta.get("scalar_fields", []):
            if isinstance(field, dict):
                field_name = field.get("name") or field.get("field_name", "")
                field_type = field.get("original_type", "STRING")
            else:
                field_name = str(field)
                field_type = "STRING"

            if field_name:
                columns.append({
                    "name": field_name,
                    "data_type": field_type.upper(),
                })

        # Add child (array/struct) fields
        for child in meta.get("children", []):
            child_name = child.get("field_name", "")
            child_type = child.get("type", "ARRAY")
            if child_name:
                columns.append({
                    "name": child_name,
                    "data_type": child_type.upper(),
                })

        model_entry = {"name": model_name}
        if columns:
            model_entry["columns"] = columns

        models.append(model_entry)

    schema = {
        "version": 2,
        "models": models,
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w") as f:
        yaml.dump(schema, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Generated structural schema.yml with {len(models)} models")
    return output_path
