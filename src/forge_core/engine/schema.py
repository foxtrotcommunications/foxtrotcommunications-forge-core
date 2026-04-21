"""
Forge Engine - Schema Management Module

Handles JSON schema comparison and ER diagram generation.
"""

import re


def compare_schemas(old_schema, new_schema, path=""):
    """
    Recursively compares two JSON schemas to identify changes.

    Args:
        old_schema (dict): The previous version of the JSON schema.
        new_schema (dict): The new version of the JSON schema.
        path (str): Current path in the schema hierarchy.

    Returns:
        list: A list of dictionaries representing change events.
    """
    changes = []
    if old_schema is None:
        old_schema = {}

    old_props = old_schema.get("properties", {})
    new_props = new_schema.get("properties", {})

    # Check for added fields and changed types
    for prop_name, prop_details in new_props.items():
        if prop_name not in old_props:
            changes.append(
                {
                    "event_type": "field_added",
                    "table_path": path if path else None,
                    "field_name": prop_name,
                    "type": prop_details.get("type") if prop_details else None,
                }
            )
        else:
            old_prop_details = old_props[prop_name] or {}
            prop_details = prop_details or {}

            old_type = old_prop_details.get("type")
            new_type = prop_details.get("type")
            if old_type != new_type:
                changes.append(
                    {
                        "event_type": "type_changed",
                        "table_path": path if path else None,
                        "field_name": prop_name,
                        "old_type": old_type,
                        "new_type": new_type,
                    }
                )

            # Recurse into nested structures
            new_path = f"{path}__{prop_name}" if path else prop_name
            if new_type == "object":
                nested_changes = compare_schemas(
                    old_prop_details, prop_details, path=new_path
                )
                changes.extend(nested_changes)
            elif (
                new_type == "array"
                and prop_details.get("items", {}).get("type") == "object"
            ):
                old_items = old_prop_details.get("items", {})
                new_items = prop_details.get("items", {})
                nested_changes = compare_schemas(old_items, new_items, path=new_path)
                changes.extend(nested_changes)

    # Check for removed fields
    for prop_name, old_prop_details in old_props.items():
        if prop_name not in new_props:
            changes.append(
                {
                    "event_type": "field_removed",
                    "table_path": path if path else None,
                    "field_name": prop_name,
                    "type": old_prop_details.get("type") if old_prop_details else None,
                }
            )

    return changes


def generate_mermaid_diagram(all_metadata):
    """
    Generates a Mermaid ER diagram from discovered table metadata.

    Args:
        all_metadata (list): List of metadata dictionaries for all discovered tables.

    Returns:
        str: Mermaid diagram markup.
    """
    mermaid_lines = ["erDiagram"]
    node_map = {}

    def get_node_name(table_path):
        if table_path not in node_map:
            safe_name = re.sub(r"[^a-zA-Z0-9_]", "", table_path)
            if safe_name in node_map.values():
                i = 1
                while f"{safe_name}_{i}" in node_map.values():
                    i += 1
                safe_name = f"{safe_name}_{i}"
            node_map[table_path] = safe_name
        return node_map[table_path]

    for table_meta in all_metadata:
        model_name = table_meta.get("model_name")
        parent_model_table = table_meta.get("parent_model")
        table_path = table_meta.get("table_path", model_name)
        table_label = table_path.replace("`", "")
        node_name = get_node_name(table_label)

        mermaid_lines.append(f"    {node_name} {{")
        for field in table_meta.get("scalar_fields", []):
            # Handle both dict format (new) and string format (legacy)
            if isinstance(field, dict):
                field_name = field.get("name", str(field))
                field_type = field.get("original_type", "string").upper()
                # Format: TYPE field_name
                mermaid_lines.append(f"        {field_type} {field_name}")
            else:
                # Legacy string format: just the field name
                field_name = str(field)
                field_type = "STRING"
                mermaid_lines.append(f"        {field_type} {field_name}")
        for child in table_meta.get("children", []):
            mermaid_lines.append(
                f'        {child.get("type")} {child.get("field_name")}'
            )
        mermaid_lines.append("    }")

        if "__" in table_label:
            parent_path = table_label.rsplit("__", 1)[0]
            parent_node = get_node_name(parent_path)
            mermaid_lines.append(f'    {parent_node} ||--o{{ {node_name} : "contains"')

    return "\n".join(mermaid_lines)


def generate_schema_graph(all_metadata):
    """
    Generates a structured JSON schema graph from discovered table metadata.
    This is used directly by the React Flow frontend for interactive diagrams.
    
    Args:
        all_metadata (list): List of metadata dictionaries for all discovered tables.

    Returns:
        dict: A dictionary containing 'tables' and 'relationships'.
    """
    graph = {
        "tables": [],
        "relationships": []
    }
    
    node_map = {}

    def get_node_name(t_path):
        if t_path not in node_map:
            safe_name = re.sub(r"[^a-zA-Z0-9_]", "", t_path)
            if safe_name in node_map.values():
                i = 1
                while f"{safe_name}_{i}" in node_map.values():
                    i += 1
                safe_name = f"{safe_name}_{i}"
            node_map[t_path] = safe_name
        return node_map[t_path]

    for table_meta in all_metadata:
        model_name = table_meta.get("model_name")
        table_path = table_meta.get("table_path", model_name)
        table_label = table_path.replace("`", "")
        node_name = get_node_name(table_label)

        table_node = {
            "name": node_name,
            "columns": []
        }

        # Add scalar fields
        for field in table_meta.get("scalar_fields", []):
            if isinstance(field, dict):
                field_name = field.get("name", str(field))
                field_type = field.get("original_type", "string").upper()
            else:
                field_name = str(field)
                field_type = "STRING"
            
            # Simple heuristic for pk/fk based on name
            f_lower = field_name.lower()
            table_node["columns"].append({
                "name": field_name,
                "type": field_type,
                "pk": f_lower in ["id", "_id", f"{node_name.lower()}_id"],
                "fk": f_lower.endswith("_id") and f_lower not in ["id", "_id", f"{node_name.lower()}_id"],
                "pii": False  # No native PII detection yet
            })

        # Add child relationships (array of structs)
        for child in table_meta.get("children", []):
            table_node["columns"].append({
                "name": child.get("field_name"),
                "type": child.get("type", "ARRAY"),
                "pk": False,
                "fk": False,
                "pii": False
            })

        graph["tables"].append(table_node)

        # nested tables relationships
        if "__" in table_label:
            parent_path = table_label.rsplit("__", 1)[0]
            parent_node = get_node_name(parent_path)
            
            graph["relationships"].append({
                "from": parent_node,
                "to": node_name,
                "type": "contains"
            })
            
    return graph
