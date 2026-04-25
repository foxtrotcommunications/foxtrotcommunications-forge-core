"""
Forge Core — Build Context Module

Immutable configuration dataclass for a build.
Stripped of SaaS-specific fields (pricing, analytics consent, org ID).
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Tuple


@dataclass(frozen=True)
class BuildContext:
    """
    Immutable build configuration for a forge-core run.

    All configuration needed for a build process is captured here.
    """

    # Required fields
    source_type: str  # 'bigquery', 'snowflake', 'databricks', 'redshift'
    source_database: str
    source_table_name: str
    target_dataset: str

    # Optional fields
    source_project: Optional[str] = None  # BigQuery only
    source_schema: Optional[str] = None   # Schema for SF/DB/Redshift
    target_project: Optional[str] = None
    job_created_at: datetime = field(default_factory=datetime.now)

    # Computed fields (set during initialization)
    qualified_table_name: str = field(default="", init=False)
    root_table_name_for_keys: str = field(default="", init=False)
    root_model_name: str = field(default="", init=False)

    def __post_init__(self):
        """Compute derived fields after initialization."""
        qualified_name = build_qualified_table_name(
            self.source_type,
            self.source_project,
            self.source_database,
            self.source_table_name,
            self.source_schema,
        )
        object.__setattr__(self, "qualified_table_name", qualified_name)

        root_for_keys = build_root_table_name(
            self.source_type,
            self.target_project,
            self.target_dataset,
        )
        object.__setattr__(self, "root_table_name_for_keys", root_for_keys)

        root_name = "FRG" if self.source_type == "snowflake" else "frg"
        object.__setattr__(self, "root_model_name", root_name)


# ============================================================================
# Pure Helper Functions
# ============================================================================


def build_qualified_table_name(
    source_type: str,
    source_project: Optional[str],
    source_database: str,
    source_table_name: str,
    source_schema: Optional[str],
) -> str:
    """
    Build warehouse-specific qualified table name.

    Examples:
        BigQuery: `project.dataset.table`
        Snowflake: "DATABASE"."SCHEMA"."TABLE"
        Databricks: catalog.schema.table
        Redshift: "schema"."table"
    """
    if source_type == "snowflake":
        schema = source_schema or "PUBLIC"
        return f'"{source_database}"."{schema}"."{source_table_name}"'

    elif source_type == "databricks":
        schema = source_schema or "default"
        return f"{source_database}.{schema}.{source_table_name}"

    elif source_type == "redshift":
        schema = source_schema or "public"
        return f'"{schema}"."{source_table_name}"'

    elif source_type == "postgres":
        schema = source_schema or "public"
        return f'"{schema}"."{source_table_name}"'

    else:  # BigQuery (default)
        if not source_project:
            raise ValueError("source_project is required for BigQuery")
        return f"`{source_project}.{source_database}.{source_table_name}`"


def build_root_table_name(
    source_type: str,
    target_project: Optional[str],
    target_dataset: str,
) -> str:
    """
    Build the root table name for key discovery.
    """
    if source_type == "snowflake":
        return f'"{target_project}"."{target_dataset}"."FRG"'

    elif source_type == "databricks":
        return f"{target_project}.{target_dataset}.frg"

    elif source_type == "redshift":
        return f'"{target_dataset}"."frg"'

    elif source_type == "postgres":
        return f'"{target_dataset}"."frg"'

    else:  # BigQuery
        return f"`{target_project}.{target_dataset}.frg`"


def validate_build_context(ctx: BuildContext) -> Tuple[bool, Optional[str]]:
    """Validate that build context has all required fields."""
    if not ctx.source_type:
        return False, "source_type is required"

    if not ctx.source_table_name:
        return False, "source_table_name is required"

    if ctx.source_type == "bigquery":
        if not ctx.source_project:
            return False, "source_project is required for BigQuery"
        if not ctx.source_database:
            return False, "source_database (dataset) is required for BigQuery"

    if ctx.source_type in ("snowflake", "databricks", "redshift", "postgres"):
        if not ctx.source_database:
            return False, f"source_database is required for {ctx.source_type}"

    if not ctx.target_dataset:
        return False, "target_dataset is required"

    return True, None
