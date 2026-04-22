#!/usr/bin/env python3
"""
Local test script for forge-core.

Runs a build_core() call against a real warehouse to verify end-to-end behavior.
Requires credentials to be configured for your target warehouse before running.

BigQuery example:
    gcloud auth application-default login
    python test_local.py --source-database my_dataset --source-table my_json_table --target-dataset test_output
"""
import sys
import os
import logging

# Add src to path for local development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from forge_core import build_core

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Test forge-core locally")
    parser.add_argument("--source-type", default="bigquery", choices=["bigquery", "snowflake", "databricks", "redshift"])
    parser.add_argument("--source-project", default=os.environ.get("SOURCE_PROJECT", "your-gcp-project"))
    parser.add_argument("--source-database", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-dataset", required=True)
    parser.add_argument("--target-project", default=None)
    parser.add_argument("--limit", type=int, default=100, help="Row limit for testing")

    args = parser.parse_args()

    target_project = args.target_project or args.source_project

    print(f"\n{'='*60}")
    print(f" Forge Core Test Run")
    print(f" Source: {args.source_project}.{args.source_database}.{args.source_table}")
    print(f" Target: {target_project}.{args.target_dataset}")
    print(f" Limit:  {args.limit}")
    print(f"{'='*60}\n")

    result = build_core(
        source_type=args.source_type,
        source_project=args.source_project,
        source_database=args.source_database,
        source_table_name=args.source_table,
        target_dataset=args.target_dataset,
        target_project=target_project,
        limit=args.limit,
    )

    print(f"\n{'='*60}")
    print(f" ✅ Build complete!")
    print(f"    Models: {result.total_models_created}")
    print(f"    Rows:   {result.total_rows_processed}")
    print(f"    Levels: {result.levels_processed}")
    print(f"    Dir:    {result.project_dir}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
