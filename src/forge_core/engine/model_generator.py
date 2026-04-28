"""
Forge Core — Model Generator Module

Handles dbt model file I/O and tagging.
Core functions only — no cross-warehouse compilation.
"""

import os
import logging

logger = logging.getLogger(__name__)


def create_file_in_models(
    file_name: str, content: str, models_dir: str = None
):
    """
    Creates a SQL file in the dbt models directory.

    Args:
        file_name: The name of the file (e.g., "users" or "users.sql")
        content: The SQL content to write to the file
        models_dir: Directory for model files (default: ./forge_project/models)
    """
    if models_dir is None:
        models_dir = "./forge_project/models"

    os.makedirs(models_dir, exist_ok=True)

    if not file_name.endswith(".sql"):
        file_name = f"{file_name}.sql"

    file_path = os.path.join(models_dir, file_name)

    try:
        with open(file_path, "w") as w:
            w.write(content)
    except Exception:
        logger.error(f"Error file write: {file_path}", exc_info=True)
        raise


def tag_models_as_excluded(model_names, models_dir: str = None):
    """
    Updates the specified models to include tags=['exclude'] in their config block.

    Args:
        model_names: A list of model names (without .sql extension) to update.
        models_dir: Directory containing model files
    """
    if models_dir is None:
        models_dir = "./forge_project/models"

    for model_name in model_names:
        if not model_name or model_name.strip() == "":
            continue

        file_path = os.path.join(models_dir, f"{model_name}.sql")
        try:
            if not os.path.exists(file_path):
                logger.warning(f"Model file not found: {file_path}")
                continue

            with open(file_path, "r") as f:
                content = f.read()

            if "tags=['exclude']" not in content and 'tags=["exclude"]' not in content:
                new_content = content.replace("config(", "config( tags=['exclude'], ")

                with open(file_path, "w") as f:
                    f.write(new_content)
        except Exception as e:
            logger.error(f"Error tagging model {model_name}: {e}", exc_info=True)
