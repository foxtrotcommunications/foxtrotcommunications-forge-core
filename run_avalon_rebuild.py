#!/usr/bin/env python3
"""
Batch run forge-core against all Avalon FHIR staging tables.
Regenerates normalized tables with the new 'root' naming convention.
"""
import sys
import os
import logging
import time

# Add src to path for local development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from forge_core import build_core

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

PROJECT = "forge-poc-452521"
STAGING_DATASET = "avalon_fhir_staging"

# Map source table → target dataset
RESOURCES = {
    "raw_patient":              "fhir_normalized_patient",
    "raw_encounter":            "fhir_normalized_encounter",
    "raw_condition":            "fhir_normalized_condition",
    "raw_procedure":            "fhir_normalized_procedure",
    "raw_observation":          "fhir_normalized_observation",
    "raw_medicationrequest":    "fhir_normalized_medicationrequest",
    "raw_immunization":         "fhir_normalized_immunization",
    "raw_claim":                "fhir_normalized_claim",
    "raw_explanationofbenefit": "fhir_normalized_explanationofbenefit",
}


def main():
    results = {}
    total_start = time.time()

    for source_table, target_dataset in RESOURCES.items():
        resource = source_table.replace("raw_", "").title()
        print(f"\n{'='*70}")
        print(f"  Processing: {resource}")
        print(f"  Source: {PROJECT}.{STAGING_DATASET}.{source_table}")
        print(f"  Target: {PROJECT}.{target_dataset}")
        print(f"{'='*70}\n")

        start = time.time()
        try:
            result = build_core(
                source_type="bigquery",
                source_project=PROJECT,
                source_database=STAGING_DATASET,
                source_table_name=source_table,
                target_dataset=target_dataset,
                target_project=PROJECT,
                clean=True,
            )
            elapsed = time.time() - start
            results[resource] = {
                "status": "✅ PASS",
                "models": result.total_models_created,
                "rows": result.total_rows_processed,
                "levels": result.levels_processed,
                "time": f"{elapsed:.1f}s",
            }
            print(f"\n  ✅ {resource}: {result.total_models_created} models, "
                  f"{result.total_rows_processed:,} rows in {elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - start
            results[resource] = {
                "status": f"❌ FAIL: {e}",
                "time": f"{elapsed:.1f}s",
            }
            print(f"\n  ❌ {resource} FAILED: {e}")

    # Summary
    total_elapsed = time.time() - total_start
    print(f"\n\n{'='*70}")
    print(f"  FORGE BUILD SUMMARY")
    print(f"{'='*70}")
    for resource, info in results.items():
        status = info["status"]
        time_str = info["time"]
        if "models" in info:
            print(f"  {status} {resource}: {info['models']} models, "
                  f"{info['rows']:,} rows [{time_str}]")
        else:
            print(f"  {status} [{time_str}]")
    print(f"\n  Total time: {total_elapsed:.1f}s")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
