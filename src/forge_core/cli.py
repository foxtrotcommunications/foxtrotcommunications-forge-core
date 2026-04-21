"""
Forge Core — CLI

Usage:
    forge-core build \\
        --source-type bigquery \\
        --source-project my-gcp-project \\
        --source-database my_dataset \\
        --source-table my_json_table \\
        --target-dataset my_target_dataset
"""

import argparse
import logging
import sys
import json
from forge_core.core import build_core


def main():
    parser = argparse.ArgumentParser(
        prog="forge-core",
        description="Decompose nested JSON into normalized dbt models.",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ===== BUILD command =====
    build_parser = subparsers.add_parser(
        "build",
        help="Run JSON→relational decomposition",
    )

    # Required
    build_parser.add_argument(
        "--source-type",
        required=True,
        choices=["bigquery", "snowflake", "databricks", "redshift"],
        help="Warehouse type",
    )
    build_parser.add_argument(
        "--source-database",
        required=True,
        help="Source database/dataset/catalog",
    )
    build_parser.add_argument(
        "--source-table",
        required=True,
        help="Source table name",
    )
    build_parser.add_argument(
        "--target-dataset",
        required=True,
        help="Target dataset/schema for generated models",
    )

    # Optional
    build_parser.add_argument(
        "--source-project",
        help="Source GCP project (BigQuery only)",
    )
    build_parser.add_argument(
        "--source-schema",
        help="Source schema (Snowflake/Databricks/Redshift)",
    )
    build_parser.add_argument(
        "--target-project",
        help="Target project/database (defaults to source-project for BQ)",
    )
    build_parser.add_argument(
        "--project-dir",
        default="./forge_project",
        help="Directory for dbt project (default: ./forge_project)",
    )
    build_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit root query rows (useful for testing)",
    )
    build_parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Skip cleaning target dataset before build",
    )
    build_parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    # ===== VERSION command =====
    subparsers.add_parser("version", help="Show version")

    args = parser.parse_args()

    if args.command == "version":
        from forge_core import __version__
        print(f"forge-core {__version__}")
        return

    if args.command != "build":
        parser.print_help()
        sys.exit(1)

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        result = build_core(
            source_type=args.source_type,
            source_database=args.source_database,
            source_table_name=args.source_table,
            target_dataset=args.target_dataset,
            source_project=args.source_project,
            source_schema=args.source_schema,
            target_project=args.target_project,
            project_dir=args.project_dir,
            limit=args.limit,
            clean=not args.no_clean,
        )

        print(f"\n{'='*60}")
        print(f"  ✓ Build complete")
        print(f"    Models created:  {result.total_models_created}")
        print(f"    Rows processed:  {result.total_rows_processed}")
        print(f"    Levels:          {result.levels_processed}")
        print(f"    Project dir:     {result.project_dir}")
        print(f"{'='*60}")

    except Exception as e:
        logging.getLogger("forge_core").error(str(e), exc_info=True)
        print(f"\n❌ Build failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
