#!/usr/bin/env python3
"""
Test script for forge-core against BigQuery.

Uses the same test data as the Forge SaaS test harness.
Job profile '4j3JldUdOKe4O7B2gdWq' from forge-poc-452521.

NOTE: Requires gcloud auth application-default login first.
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
    """
    Run forge-core build against the test job profile.
    
    The Firestore job profile '4j3JldUdOKe4O7B2gdWq' maps to:
      source_project = forge-poc-452521
      source_database = (from Firestore)
      source_table_name = (from Firestore)
      target_dataset = (from Firestore)
      target_project = forge-poc-452521

    Provide these values as CLI args or environment variables.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Test forge-core locally")
    parser.add_argument("--source-project", default=os.environ.get("SOURCE_PROJECT", "forge-poc-452521"))
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
        source_type="bigquery",
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
