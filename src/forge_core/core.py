"""
Forge Core — Core Orchestrator

The primary entry point: `build_core()`.
Performs JSON→relational decomposition, dbt model generation, and rollup creation
without any SaaS or cloud dependencies.
"""

import json
import os
import shutil
import importlib.resources
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any

from forge_core.adapters import get_adapter
from forge_core.engine.build_context import (
    BuildContext,
    validate_build_context,
    build_qualified_table_name,
    build_root_table_name,
)
from forge_core.engine.root_processor import (
    create_and_build_root_model,
    get_rows_processed,
    has_root_keys,
)
from forge_core.engine.unnesting import execute_iterative_unnesting
from forge_core.engine.dbt_runner import run_dbt_command
from forge_core.engine.schema import generate_mermaid_diagram, generate_schema_graph
from forge_core.json_schema import metadata_to_json_schema
from forge_core.profiles import generate_profiles_yml
from forge_core.schema_writer import write_schema_yml

logger = logging.getLogger(__name__)


@dataclass
class CoreBuildResult:
    """Result of a forge-core build."""

    total_models_created: int = 0
    total_rows_processed: int = 0
    root_rows_processed: int = 0
    levels_processed: int = 0
    all_metadata: List[Dict[str, Any]] = field(default_factory=list)
    json_schema: Optional[Dict] = None
    mermaid_diagram: Optional[str] = None
    schema_graph: Optional[Dict] = None
    project_dir: str = ""


def build_core(
    *,
    source_type: str,
    source_database: str,
    source_table_name: str,
    target_dataset: str,
    source_project: Optional[str] = None,
    source_schema: Optional[str] = None,
    target_project: Optional[str] = None,
    project_dir: str = "./forge_project",
    limit: Optional[int] = None,
    sample: Optional[int] = None,
    clean: bool = True,
) -> CoreBuildResult:
    """
    Decompose nested JSON into normalized dbt models.

    This is the core BFS unnesting engine. It:
    1. Scaffolds a dbt project directory
    2. Generates profiles.yml from auth env vars
    3. Creates the root model from the source table
    4. Iteratively discovers and unnests nested JSON fields (BFS)
    5. Generates a rollup view, schema.yml, JSON Schema, and dbt docs

    Args:
        source_type: Warehouse type ('bigquery', 'snowflake', 'databricks', 'redshift')
        source_database: Source database/dataset/catalog
        source_table_name: Source table name
        target_dataset: Target dataset/schema for generated models
        source_project: Source project ID (BigQuery only)
        source_schema: Source schema (Snowflake/Databricks/Redshift)
        target_project: Target project/database (defaults to source_project for BQ)
        project_dir: Directory for the dbt project (default: ./forge_project)
        limit: Optional row limit for root query (baked into models)
        sample: Optional sample size for schema discovery only (models are unlimited)
        clean: If True, clean target dataset before building (default: True)

    Returns:
        CoreBuildResult with metadata, diagrams, and paths

    Raises:
        ValueError: If required arguments are missing
        RuntimeError: If dbt build fails
    """

    # Default target_project to source_project for BigQuery
    if target_project is None and source_type == "bigquery":
        target_project = source_project

    # Build context
    ctx = BuildContext(
        source_type=source_type,
        source_database=source_database,
        source_table_name=source_table_name,
        target_dataset=target_dataset,
        source_project=source_project,
        source_schema=source_schema,
        target_project=target_project,
    )

    is_valid, error = validate_build_context(ctx)
    if not is_valid:
        raise ValueError(f"Invalid configuration: {error}")

    job_created_at = datetime.now()
    job_created_at_str = job_created_at.isoformat() + "Z"

    logger.info("=" * 60)
    logger.info(f"Forge Core — Build Starting")
    logger.info(f"Source: {ctx.qualified_table_name}")
    logger.info(f"Target: {target_project}.{target_dataset}")
    logger.info(f"Type: {source_type}")
    logger.info("=" * 60)

    # ===== SCAFFOLD DBT PROJECT =====
    _scaffold_dbt_project(project_dir)

    # ===== GENERATE PROFILES.YML =====
    generate_profiles_yml(
        source_type=source_type,
        target_project=target_project,
        target_dataset=target_dataset,
        project_dir=project_dir,
    )

    # ===== INITIALIZE ADAPTER =====
    adapter_kwargs = {}
    if source_type == "bigquery" and target_project:
        adapter_kwargs["project"] = target_project
    adapter = get_adapter(source_type, **adapter_kwargs)
    adapter._ensure_client()

    # Patch the global singleton so discovery/unnesting modules use this adapter
    import forge_core.engine.context as engine_context
    engine_context.ADAPTER = adapter

    # ===== VALIDATE SOURCE =====
    logger.info("Validating source connection...")
    if not adapter.validate_source(ctx.qualified_table_name):
        raise RuntimeError(f"Cannot connect to source: {ctx.qualified_table_name}")
    logger.info("✓ Source validated")

    # ===== CLEAN TARGET DATASET =====
    if clean:
        logger.info(f"Cleaning target dataset: {target_dataset}...")
        adapter.clean_dataset(target_dataset)
        logger.info("✓ Dataset cleaned")

    # ===== CLEAN MODEL DIRECTORY =====
    models_dir = os.path.join(project_dir, "models")
    _clean_model_directory(models_dir)

    # ===== BUILD ROOT MODEL =====
    # If --sample is set, use it as the limit for discovery but generate
    # unlimited models at the end. If --limit is set, bake it into models.
    discovery_limit = sample or limit
    is_sample_mode = sample is not None

    if is_sample_mode:
        logger.info(f"Sample mode: discovering schema from {sample:,} rows")

    logger.info("Building root model...")
    root_result = create_and_build_root_model(
        adapter=adapter,
        qualified_table_name=ctx.qualified_table_name,
        target_dataset=target_dataset,
        source_type=source_type,
        limit=discovery_limit,
    )
    logger.info(f"✓ Root model built: {root_result.model_name}")

    # ===== CHECK ROOT HAS KEYS =====
    if not has_root_keys(adapter, ctx.root_table_name_for_keys):
        logger.info("No nested data found. Returning empty result.")
        return CoreBuildResult(project_dir=project_dir)

    # Track rows processed for root
    root_rows_processed = get_rows_processed(
        adapter, target_project, target_dataset,
        root_result.model_name, job_created_at_str,
    )
    logger.info(f"Root rows processed: {root_rows_processed}")

    # ===== BFS ITERATIVE UNNESTING =====
    logger.info("Starting iterative unnesting (BFS)...")
    unnesting_result = execute_iterative_unnesting(
        adapter=adapter,
        root_table_name_for_keys=ctx.root_table_name_for_keys,
        root_model_name=ctx.root_model_name,
        qualified_table_name=ctx.qualified_table_name,
        target_dataset=target_dataset,
        target_project=target_project,
        job_created_at_str=job_created_at_str,
    )

    all_metadata = unnesting_result.all_metadata
    total_rows_processed = unnesting_result.total_rows_processed + root_rows_processed

    logger.info(f"✓ Unnesting complete: {len(all_metadata)} models, "
                f"{unnesting_result.levels_processed} levels")

    # ===== SAMPLE MODE: REWRITE ROOT MODEL =====
    # Discovery used a LIMIT, but the output models should be unlimited.
    # Rewrite frg.sql with the full query so `dbt build` processes all rows.
    if is_sample_mode:
        logger.info("Sample mode: rewriting root model without LIMIT (production-ready)")
        unlimited_root_sql = adapter.get_root_table_sql(ctx.qualified_table_name, limit=None)
        root_model_path = os.path.join(
            models_dir, f"{root_result.model_name}.sql"
        )
        with open(root_model_path, "w") as f:
            # Preserve the exclude tag that was added during discovery
            tagged_sql = unlimited_root_sql.replace(
                "config(",
                "config( tags=['exclude'], ",
                1,
            )
            f.write(tagged_sql)
        logger.info("✓ Root model rewritten — models are now production-ready")

    # ===== GENERATE ROLLUP =====
    logger.info("Generating rollup view...")
    rollup_sql = adapter.generate_rollup_sql(all_metadata, target_dataset)
    rollup_path = os.path.join(models_dir, "frg__rollup.sql")
    with open(rollup_path, "w") as f:
        f.write(rollup_sql)
    logger.info("✓ Rollup SQL generated")

    # Build rollup
    dbt_command = (
        f"dbt build --profile forge --profiles-dir . "
        f"--select frg__rollup "
        f"--target {target_dataset}"
    )
    rollup_result = run_dbt_command(dbt_command)
    if rollup_result.returncode != 0:
        logger.warning(f"Rollup build failed: {rollup_result.stderr}")
    else:
        logger.info("✓ Rollup view built")

    # ===== GENERATE ARTIFACTS =====
    logger.info("Generating artifacts...")

    # JSON Schema
    json_schema_obj = metadata_to_json_schema(all_metadata, source_table_name)

    # Mermaid diagram
    mermaid_diagram = generate_mermaid_diagram(all_metadata)

    # Schema graph
    schema_graph = generate_schema_graph(all_metadata)

    # schema.yml (structural)
    schema_yml_path = os.path.join(models_dir, "schema.yml")
    write_schema_yml(all_metadata, schema_yml_path)

    # Save JSON Schema to file
    json_schema_path = os.path.join(project_dir, "target", "schema.json")
    os.makedirs(os.path.dirname(json_schema_path), exist_ok=True)
    with open(json_schema_path, "w") as f:
        json.dump(json_schema_obj, f, indent=2)

    # Save Mermaid diagram
    mermaid_path = os.path.join(project_dir, "target", "schema.mmd")
    with open(mermaid_path, "w") as f:
        f.write(mermaid_diagram)

    logger.info("✓ Artifacts generated")

    # ===== GENERATE DBT DOCS =====
    logger.info("Generating dbt docs...")
    try:
        # Delete cache files to force full reparse
        for cache_file in ["target/manifest.json", "target/partial_parse.msgpack"]:
            fpath = os.path.join(project_dir, cache_file)
            if os.path.exists(fpath):
                os.remove(fpath)

        docs_cmd = (
            f"dbt docs generate --profile forge --profiles-dir . "
            f"--target {target_dataset}"
        )
        docs_result = run_dbt_command(docs_cmd)
        if docs_result.returncode == 0:
            logger.info("✓ dbt docs generated")
        else:
            logger.warning(f"dbt docs generate failed: {docs_result.stderr}")
    except Exception as docs_error:
        logger.warning(f"Failed to generate dbt docs: {docs_error}")

    # ===== RETURN RESULT =====
    result = CoreBuildResult(
        total_models_created=len(all_metadata),
        total_rows_processed=total_rows_processed,
        root_rows_processed=root_rows_processed,
        levels_processed=unnesting_result.levels_processed,
        all_metadata=all_metadata,
        json_schema=json_schema_obj,
        mermaid_diagram=mermaid_diagram,
        schema_graph=schema_graph,
        project_dir=project_dir,
    )

    logger.info("=" * 60)
    logger.info(f"✓ Build complete: {result.total_models_created} models, "
                f"{result.total_rows_processed} rows")
    logger.info("=" * 60)

    return result


def _scaffold_dbt_project(project_dir: str):
    """
    Create the dbt project directory structure with bundled skeleton files.
    """
    os.makedirs(project_dir, exist_ok=True)

    # Create subdirectories
    for subdir in ["models", "macros", "seeds", "tests", "analyses", "target", "logs"]:
        os.makedirs(os.path.join(project_dir, subdir), exist_ok=True)

    # Write dbt_project.yml if it doesn't exist
    dbt_project_path = os.path.join(project_dir, "dbt_project.yml")
    if not os.path.exists(dbt_project_path):
        dbt_project_content = """\
name: 'forge'
version: '1.0.0'

flags:
  send_anonymous_usage_stats: false
  log_file_log_level: debug
  print_log_level: info

models:
  +post-hook:
    - "{{incremental_tmp_table_dropper(this)}}"

analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"
  - "logs"

profile: forge
"""
        with open(dbt_project_path, "w") as f:
            f.write(dbt_project_content)

    # Write macro
    macro_path = os.path.join(project_dir, "macros", "incremental_tmp_table_dropper.sql")
    if not os.path.exists(macro_path):
        macro_content = """\
{% macro incremental_tmp_table_dropper(bigQueryRelationObject) %}
    {% set tmpTableName %}
        {{ bigQueryRelationObject.database + '.' + bigQueryRelationObject.schema + '.' + bigQueryRelationObject.identifier + '__dbt_tmp'}}
    {% endset %}
    {% set query %}
        drop table if exists {{tmpTableName}};
    {% endset %}

    {{ return(query) }}
{% endmacro %}
"""
        with open(macro_path, "w") as f:
            f.write(macro_content)

    logger.info(f"✓ dbt project scaffolded at {project_dir}")


def _clean_model_directory(models_dir: str):
    """Remove all .sql files from the models directory (fresh build)."""
    if not os.path.exists(models_dir):
        return

    for f in os.listdir(models_dir):
        if f.endswith(".sql"):
            os.remove(os.path.join(models_dir, f))

    logger.info(f"✓ Cleaned model directory: {models_dir}")
