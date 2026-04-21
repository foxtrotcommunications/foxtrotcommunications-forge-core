# Forge Core

**Automatically decompose nested JSON in your data warehouse into normalized dbt models.**

Forge Core is a deterministic BFS engine that reads a single JSON column (or multi-column table), discovers all nested structures, and generates:

- **dbt SQL models** — one per nested object/array
- **Rollup view** — reassembles the full document from normalized tables
- **schema.yml** — structural column inventory
- **JSON Schema** — standard `draft-07` schema of the discovered structure
- **Mermaid ER diagram** — table relationship visualization
- **dbt docs** — browseable documentation site

## Supported Warehouses

| Warehouse  | Install Extra | Status |
|------------|--------------|--------|
| BigQuery   | `forge-core[bigquery]` | ✅ Production |
| Snowflake  | `forge-core[snowflake]` | ✅ Production |
| Databricks | `forge-core[databricks]` | ✅ Production |
| Redshift   | `forge-core[redshift]` | 🚧 Beta |

## Quickstart

```bash
pip install forge-core[bigquery]

forge-core build \
  --source-type bigquery \
  --source-project my-gcp-project \
  --source-database my_dataset \
  --source-table my_json_table \
  --target-dataset my_target
```

Or use the Python API:

```python
from forge_core import build_core

result = build_core(
    source_type="bigquery",
    source_project="my-gcp-project",
    source_database="my_dataset",
    source_table_name="my_json_table",
    target_dataset="my_target",
)

print(f"Created {result.total_models_created} models")
print(f"Processed {result.total_rows_processed} rows")
```

## How It Works

```
┌─────────────────────────────┐
│  Source Table (JSON column)  │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│  1. Root Model (frg)        │  Parse JSON → root SELECT
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│  2. BFS Discovery Loop      │  For each level:
│     - Discover keys          │    • get_keys() → field names
│     - Infer types            │    • get_types() → STRUCT/ARRAY/scalar
│     - Generate SQL model     │    • create_file_in_models()
│     - dbt build              │    • run_dbt_command()
│     - Tag as excluded        │    • tag_models_as_excluded()
│     - Queue children         │    • next_batch.extend()
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│  3. Rollup View              │  JOIN all tables back into
│     (frg__rollup)            │  nested STRUCT/ARRAY form
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│  4. Artifacts                │  schema.yml, JSON Schema,
│                              │  Mermaid diagram, dbt docs
└─────────────────────────────┘
```

## Authentication

Forge Core uses standard warehouse authentication:

- **BigQuery**: Application Default Credentials (`gcloud auth application-default login`) or `GOOGLE_APPLICATION_CREDENTIALS`
- **Snowflake**: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH`, etc.
- **Databricks**: `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_ACCESS_TOKEN`
- **Redshift**: `REDSHIFT_HOST`, `REDSHIFT_USER`, `REDSHIFT_PASSWORD`, `REDSHIFT_DATABASE`

## Project Structure

After a build, your project directory looks like:

```
forge_project/
├── dbt_project.yml
├── profiles.yml          # Auto-generated
├── macros/
│   └── incremental_tmp_table_dropper.sql
├── models/
│   ├── frg.sql           # Root model
│   ├── frg__root__....sql # Unnested models (one per level)
│   ├── frg__rollup.sql   # Rollup view
│   └── schema.yml        # Column inventory
└── target/
    ├── schema.json        # JSON Schema
    ├── schema.mmd         # Mermaid diagram
    └── index.html         # dbt docs
```

## Use in Airflow / Containers

```python
# Airflow PythonOperator
from forge_core import build_core

def forge_task(**context):
    result = build_core(
        source_type="bigquery",
        source_project="my-project",
        source_database="raw",
        source_table_name="api_responses",
        target_dataset="normalized",
        project_dir="/tmp/forge_project",
    )
    return result.total_models_created
```

## License

Apache 2.0
