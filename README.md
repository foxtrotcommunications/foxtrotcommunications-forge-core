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
| BigQuery   | `foxtrotcommunications-forge-core[bigquery]` | ✅ Production |
| Snowflake  | `foxtrotcommunications-forge-core[snowflake]` | ✅ Production |
| Databricks | `foxtrotcommunications-forge-core[databricks]` | ✅ Production |
| PostgreSQL | `foxtrotcommunications-forge-core[postgres]` | ✅ Production |

## Quickstart

### BigQuery

```bash
pip install foxtrotcommunications-forge-core[bigquery]

forge-core build \
  --source-type bigquery \
  --source-project my-gcp-project \
  --source-database my_dataset \
  --source-table my_json_table \
  --target-dataset my_target
```

### PostgreSQL

```bash
pip install foxtrotcommunications-forge-core[postgres]

export POSTGRES_HOST=my-db-host
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=my_database
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=secret

forge-core build \
  --source-type postgres \
  --source-database my_database \
  --source-table my_json_table \
  --target-dataset my_output_schema
```

PostgreSQL maps `--target-dataset` to a Postgres **schema**. All output tables are created in that schema within the same database.

### Python API

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

### Enabling progress output

Forge Core uses Python's standard `logging` module. By default nothing is printed — add this before your `build_core()` call to stream progress to the console:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("forge_core").setLevel(logging.INFO)
```

This works in Jupyter notebooks, plain scripts, Airflow (routes through its own handler automatically), and any CI/CD environment that captures stdout.

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
- **PostgreSQL**: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DATABASE`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_SCHEMA` (default: `public`)

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

## Sample Mode

For large datasets, use `--sample` to discover schema from a representative subset and generate **production-ready models without a row limit**:

```bash
# Discover schema from 5,000 rows — output models process ALL data
forge-core build \
  --source-type postgres \
  --source-database my_db \
  --source-table raw_patients \
  --target-dataset normalized \
  --sample 5000
```

This separates **discovery** (fast, on a sample) from **execution** (full data, via dbt):

1. Forge discovers the nested JSON structure from 5,000 representative rows
2. Generates dbt models with no `LIMIT` clause — ready for production
3. Run `dbt build` on the full dataset whenever you want

```bash
# After forge generates the models, run dbt directly on all data:
cd forge_project && dbt build --profile forge --profiles-dir . --target normalized
```

This is especially useful for FHIR and healthcare workloads where:
- Source data is schema-stable (FHIR resources follow the spec)
- You want to version-control the generated models
- You want to schedule `dbt build` independently from discovery

| Flag | Behavior | Use case |
|------|----------|----------|
| `--sample 5000` | Discover on sample, output unlimited models | Production workflows |
| `--limit 5000` | Bake `LIMIT 5000` into the models permanently | Testing / development |
| _(neither)_ | Process all rows during discovery | Small datasets |

## Incremental Loading

By default, `build_core()` drops and recreates all target tables on every run (`clean=True`). For production pipelines where you want to append only new records, set `clean=False`:

```python
# First run — full load
result = build_core(
    source_type="bigquery",
    source_project="my-project",
    source_database="raw",
    source_table_name="api_responses",
    target_dataset="normalized",
    clean=True,   # default — creates all tables from scratch
)

# Subsequent runs — incremental
result = build_core(
    source_type="bigquery",
    source_project="my-project",
    source_database="raw",
    source_table_name="api_responses",
    target_dataset="normalized",
    clean=False,  # keeps existing tables, appends new rows only
)
```

When `clean=False`, every generated model filters on `ingestion_hash` and `ingestion_timestamp` to skip rows that have already been processed. Only new source records are decomposed across all nesting levels.

| Parameter | Behavior | Use case |
|-----------|----------|----------|
| `clean=True` (default) | Drops target tables, full rebuild | Development, schema changes, first deploy |
| `clean=False` | Appends new rows only | Scheduled pipelines, production ingestion |

> **Note:** If your source schema changes (new nested fields appear), run with `clean=True` once to pick up the new structure. The `on_schema_change='append_new_columns'` setting will add new columns on incremental runs, but will not remove columns that no longer appear in the source.

## Understanding the Generated Schema

### Key Columns

Every table generated by Forge Core contains these system columns:

| Column | Type | Description |
|--------|------|-------------|
| `ingestion_hash` | STRING | Hash of the source row. Groups all decomposed tables that came from the same original JSON document. |
| `idx` | STRING | **Composite positional key.** Encodes the exact path through nested arrays to reach this row. |
| `ingestion_timestamp` | TIMESTAMP | When the row was ingested. |
| `table_path` | STRING | Hierarchical path describing the nesting lineage (e.g., `root__experiments__team`). |

### How `idx` Works

The `idx` column is a `_`-delimited string that grows one segment per nesting level:

```
Depth 0 (root):        idx = "1"
Depth 1 (child):       idx = "1_2"        ← root row 1, child element 2
Depth 2 (grandchild):  idx = "1_2_3"      ← root row 1, child 2, grandchild 3
Depth 3 (great-grand): idx = "1_2_3_1"    ← root row 1, child 2, grandchild 3, great-grandchild 1
```

Each segment represents the array position at that nesting level. This means:
- **Every child row carries its full ancestry in `idx`.**
- To find a child's parent, strip the last segment.
- To join parent ↔ child, match on the parent's depth offset.

### Joining Parent to Child Tables

**The rule:** for each segment in the parent's `idx`, add one equality condition comparing that segment position in both parent and child. A parent at depth N has N segments — you expand N index conditions.

#### BigQuery

```sql
-- Depth 0 → 1: root (idx="1") → experiments (idx="1_2")
-- Parent has 1 segment → 1 index condition
SELECT
    r.*,
    e.experiment_name,
    e.experiment_status
FROM `project.dataset.frg__root` r
JOIN `project.dataset.frg__root__expe1` e
    ON  r.ingestion_hash = e.ingestion_hash
    AND SPLIT(r.idx, '_')[OFFSET(0)] = SPLIT(e.idx, '_')[OFFSET(0)]

-- Depth 1 → 2: experiments (idx="1_2") → team (idx="1_2_3")
-- Parent has 2 segments → 2 index conditions
SELECT
    e.*,
    t.team_name,
    t.team_role
FROM `project.dataset.frg__root__expe1` e
JOIN `project.dataset.frg__root__expe1__team1` t
    ON  e.ingestion_hash = t.ingestion_hash
    AND SPLIT(e.idx, '_')[OFFSET(0)] = SPLIT(t.idx, '_')[OFFSET(0)]
    AND SPLIT(e.idx, '_')[OFFSET(1)] = SPLIT(t.idx, '_')[OFFSET(1)]

-- Depth 2 → 3: team (idx="1_2_3") → lab_results (idx="1_2_3_1")
-- Parent has 3 segments → 3 index conditions
SELECT
    t.*,
    l.lab_name,
    l.result_value
FROM `project.dataset.frg__root__expe1__team1` t
JOIN `project.dataset.frg__root__expe1__team1__lab_1` l
    ON  t.ingestion_hash = l.ingestion_hash
    AND SPLIT(t.idx, '_')[OFFSET(0)] = SPLIT(l.idx, '_')[OFFSET(0)]
    AND SPLIT(t.idx, '_')[OFFSET(1)] = SPLIT(l.idx, '_')[OFFSET(1)]
    AND SPLIT(t.idx, '_')[OFFSET(2)] = SPLIT(l.idx, '_')[OFFSET(2)]

-- Three-level join: root → experiments → team
SELECT
    r.patient_id,
    e.experiment_name,
    t.team_name
FROM `project.dataset.frg__root` r
JOIN `project.dataset.frg__root__expe1` e
    ON  r.ingestion_hash = e.ingestion_hash
    AND SPLIT(r.idx, '_')[OFFSET(0)] = SPLIT(e.idx, '_')[OFFSET(0)]
JOIN `project.dataset.frg__root__expe1__team1` t
    ON  e.ingestion_hash = t.ingestion_hash
    AND SPLIT(e.idx, '_')[OFFSET(0)] = SPLIT(t.idx, '_')[OFFSET(0)]
    AND SPLIT(e.idx, '_')[OFFSET(1)] = SPLIT(t.idx, '_')[OFFSET(1)]
```

#### Snowflake

```sql
-- Depth 0 → 1: root → experiments (1 condition)
SELECT r.*, e."experiment_name"
FROM "DATASET"."FRG__ROOT" r
JOIN "DATASET"."FRG__ROOT__EXPE1" e
    ON  r."ingestion_hash" = e."ingestion_hash"
    AND SPLIT_PART(r."idx", '_', 1) = SPLIT_PART(e."idx", '_', 1)

-- Depth 1 → 2: experiments → team (2 conditions)
SELECT e.*, t."team_name"
FROM "DATASET"."FRG__ROOT__EXPE1" e
JOIN "DATASET"."FRG__ROOT__EXPE1__TEAM1" t
    ON  e."ingestion_hash" = t."ingestion_hash"
    AND SPLIT_PART(e."idx", '_', 1) = SPLIT_PART(t."idx", '_', 1)
    AND SPLIT_PART(e."idx", '_', 2) = SPLIT_PART(t."idx", '_', 2)
```

#### PostgreSQL

```sql
-- Depth 0 → 1: root → category (1 condition)
SELECT r.*, c."code", c."display"
FROM forge_output.frg__root r
JOIN forge_output.frg__root__cate1 c
    ON  r.ingestion_hash = c.ingestion_hash
    AND split_part(r.idx, '_', 1) = split_part(c.idx, '_', 1)

-- Depth 1 → 2: category → coding (2 conditions)
SELECT c.*, cd."code", cd."system"
FROM forge_output.frg__root__cate1 c
JOIN forge_output.frg__root__cate1__codi1 cd
    ON  c.ingestion_hash = cd.ingestion_hash
    AND split_part(c.idx, '_', 1) = split_part(cd.idx, '_', 1)
    AND split_part(c.idx, '_', 2) = split_part(cd.idx, '_', 2)
```

### General Join Formula

For a parent at **depth N** joining to a child at **depth N+1**, expand **N index conditions** — one per segment of the parent's `idx`:

```
parent.ingestion_hash = child.ingestion_hash
AND SPLIT(parent.idx, '_')[OFFSET(0)] = SPLIT(child.idx, '_')[OFFSET(0)]
AND SPLIT(parent.idx, '_')[OFFSET(1)] = SPLIT(child.idx, '_')[OFFSET(1)]
  ...
AND SPLIT(parent.idx, '_')[OFFSET(N-1)] = SPLIT(child.idx, '_')[OFFSET(N-1)]
```

The child always has one more segment than the parent — that final segment is the child's own position within the parent array.

### Table Naming Convention

Table names encode the nesting path with truncated field names:

```
frg__root                          ← root extraction
frg__root__expe1                   ← root.experiments (truncated to 4 chars + counter)
frg__root__expe1__team1            ← root.experiments[].team
frg__root__expe1__team1__lab_1     ← root.experiments[].team[].lab_results
frg__root__hosp1__staf1__nurs1     ← root.hospital[].staff[].nurses
```

### The Rollup View

The `frg__rollup` view automatically reassembles all normalized tables back into nested STRUCT/ARRAY form — reconstructing the original JSON shape as queryable warehouse-native types. Use it when you want the full document without manual joins.

> **Note:** Rollup is not currently supported for PostgreSQL. PostgreSQL cannot handle the CTE-heavy rollup SQL that Forge generates for the other warehouses.

## License

Apache 2.0
