"""
Forge Core — Profiles Generator

Auto-generates dbt profiles.yml from adapter configuration and environment variables.
"""

import os
import yaml
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def generate_profiles_yml(
    source_type: str,
    target_project: str,
    target_dataset: str,
    project_dir: str = "./forge_project",
    location: Optional[str] = None,
) -> str:
    """
    Generate a dbt profiles.yml for the specified warehouse.

    Auth follows dbt-core conventions:
    - BigQuery: ADC (gcloud auth) or GOOGLE_APPLICATION_CREDENTIALS env var
    - Snowflake: SNOWFLAKE_* env vars + PEM key
    - Databricks: DATABRICKS_* env vars
    - Redshift: REDSHIFT_* env vars

    Args:
        source_type: Warehouse type (bigquery, snowflake, databricks, redshift)
        target_project: Target project/database
        target_dataset: Target dataset/schema
        project_dir: dbt project directory
        location: BigQuery dataset location (optional, auto-detected)

    Returns:
        Path to the generated profiles.yml
    """
    source_type = source_type.lower()

    if source_type == "bigquery":
        profile = _bigquery_profile(target_project, target_dataset, location)
    elif source_type == "snowflake":
        profile = _snowflake_profile(target_project, target_dataset)
    elif source_type == "databricks":
        profile = _databricks_profile(target_project, target_dataset)
    elif source_type == "redshift":
        profile = _redshift_profile(target_dataset)
    elif source_type == "postgres":
        profile = _postgres_profile(target_dataset)
    else:
        raise ValueError(f"Unsupported source_type: {source_type}")

    profiles_path = os.path.join(project_dir, "profiles.yml")
    os.makedirs(project_dir, exist_ok=True)

    with open(profiles_path, "w") as f:
        yaml.dump(profile, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Generated profiles.yml at {profiles_path}")
    return profiles_path


def _bigquery_profile(project: str, dataset: str, location: Optional[str] = None) -> dict:
    """BigQuery profile using Application Default Credentials."""
    output = {
        "type": "bigquery",
        "method": "oauth",
        "project": project,
        "dataset": dataset,
        "threads": 4,
        "timeout_seconds": 300,
        "priority": "interactive",
    }

    if location:
        output["location"] = location

    # If a service account key is specified, use it
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if key_path and os.path.exists(key_path):
        output["method"] = "service-account"
        output["keyfile"] = key_path

    return {
        "forge": {
            "target": dataset,
            "outputs": {
                dataset: output,
            },
        }
    }


def _snowflake_profile(database: str, schema: str) -> dict:
    """Snowflake profile using environment variables."""
    account = os.environ.get("SNOWFLAKE_ACCOUNT", "")
    user = os.environ.get("SNOWFLAKE_USER", "")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "")
    role = os.environ.get("SNOWFLAKE_ROLE", "")
    private_key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")

    output = {
        "type": "snowflake",
        "account": account,
        "user": user,
        "database": database,
        "warehouse": warehouse,
        "schema": schema,
        "role": role,
        "threads": 4,
    }

    if private_key_path and os.path.exists(private_key_path):
        output["private_key_path"] = private_key_path

    return {
        "forge": {
            "target": schema,
            "outputs": {
                schema: output,
            },
        }
    }


def _databricks_profile(catalog: str, schema: str) -> dict:
    """Databricks profile using environment variables."""
    host = os.environ.get("DATABRICKS_SERVER_HOSTNAME", "")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    token = os.environ.get("DATABRICKS_ACCESS_TOKEN", "")

    output = {
        "type": "databricks",
        "host": host,
        "http_path": http_path,
        "catalog": catalog,
        "schema": schema,
        "threads": 4,
    }

    if token:
        output["token"] = token
    else:
        # Try M2M OAuth
        client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
        client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
        if client_id and client_secret:
            output["auth_type"] = "oauth-m2m"
            output["client_id"] = client_id
            output["client_secret"] = client_secret

    return {
        "forge": {
            "target": schema,
            "outputs": {
                schema: output,
            },
        }
    }


def _redshift_profile(schema: str) -> dict:
    """Redshift profile using environment variables."""
    host = os.environ.get("REDSHIFT_HOST", "")
    port = int(os.environ.get("REDSHIFT_PORT", "5439"))
    user = os.environ.get("REDSHIFT_USER", "")
    password = os.environ.get("REDSHIFT_PASSWORD", "")
    database = os.environ.get("REDSHIFT_DATABASE", "")

    return {
        "forge": {
            "target": schema,
            "outputs": {
                schema: {
                    "type": "redshift",
                    "host": host,
                    "port": port,
                    "user": user,
                    "password": password,
                    "dbname": database,
                    "schema": schema,
                    "threads": 4,
                },
            },
        }
    }

def _postgres_profile(schema: str) -> dict:
    """PostgreSQL profile using environment variables."""
    host = os.environ.get("POSTGRES_HOST", "")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    user = os.environ.get("POSTGRES_USER", "")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    database = os.environ.get("POSTGRES_DATABASE", "")

    return {
        "forge": {
            "target": schema,
            "outputs": {
                schema: {
                    "type": "postgres",
                    "host": host,
                    "port": port,
                    "user": user,
                    "password": password,
                    "dbname": database,
                    "schema": schema,
                    "threads": 4,
                },
            },
        }
    }
