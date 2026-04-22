# Forge vs Manual: JSON Decomposition at Scale

Real run context: `avalon_fhir_staging.raw_claim` — 6.2M rows, 28 models, 5 levels deep, 220M total rows output.

---

## Summary

| | **Forge** | **Manual** |
|---|---|---|
| Time to first model | ~8 min | Days–weeks |
| Models produced | 28 | 28 (if you get them all) |
| Rollup view | Automatic | Write by hand |
| Schema discovery | Automatic (queries live data) | Manual sampling + guessing |
| Schema drift handling | Re-run | Re-audit + rewrite affected models |
| Artifacts (schema.yml, JSON Schema, Mermaid) | Automatic | Separate effort |
| Consistency guarantee | Deterministic BFS | Human error |
| BigQuery → Snowflake port | Change `source_type=` | Rewrite all SQL |

---

## Stage-by-Stage Breakdown

### 1. Schema Discovery

**Forge**

Executes `get_keys()` / `get_types()` SQL against live data at every level. Discovers all fields present in actual data — not just what's in docs or a sample.

**Manual**

You have to write exploratory SQL yourself:

```sql
SELECT DISTINCT JSON_EXTRACT_KEYS(raw_json) FROM raw_claim LIMIT 1000;
```

Then repeat for every nested field. For a schema with 5 levels of nesting you'd run dozens of these queries, manually track what you found, and still miss fields that only appear in rare records.

> **Forge wins:** Live BFS discovery on 6.2M rows — not a sample.

---

### 2. SQL Model Authoring

**Forge**

28 `.sql` files generated in 8 minutes.

**Manual**

Each model requires:
- The correct `SELECT` clause with proper JSON extraction syntax for your warehouse
- The correct `idx` join key construction (one per nesting level)
- The correct `STRUCT` vs `ARRAY` branching logic
- dbt config block with incremental settings, partition, cluster keys

For BigQuery, a single mid-level model (e.g. `frg__root__raw_1__item1`) looks like:

```sql
{{ config(materialized='incremental', unique_key=['ingestion_hash','idx'], ...) }}

SELECT
  p.ingestion_hash,
  CONCAT(p.idx, '_', ROW_NUMBER() OVER (...)) as idx,
  JSON_VALUE(item, '$.net.value')       as net_value,
  JSON_VALUE(item, '$.net.currency')    as net_currency,
  -- ... every field, manually extracted
FROM {{ ref('frg__root__raw_1') }} p,
UNNEST(JSON_EXTRACT_ARRAY(p.raw_json, '$.item')) as item
```

At 28 models, 5 levels deep: conservatively **2–4 hours per model** for an engineer who already knows the schema. That's **56–112 hours** of SQL authoring — and that assumes the schema is fully known upfront.

> **Forge wins:** 8 minutes vs 2–3 weeks of engineering.

---

### 3. Rollup View

**Forge**

Auto-generated. The rollup CTE chain correctly:
- Aggregates each child table back into a STRUCT
- `ARRAY_AGG`s children into parent arrays
- Joins all 28 tables using depth-aware `SPLIT(idx)` conditions

**Manual**

The rollup for 28 tables is the hardest part. You need to:
- Know the full parent-child graph
- Write correct `SPLIT(idx, '_')[OFFSET(N)]` join conditions for each depth
- Get the `ARRAY_AGG` / `STRUCT` nesting order exactly right
- Handle NULL propagation correctly

One mistake in the join depth and the rollup silently produces wrong row counts or duplicate rows. This is where manual approaches most commonly break down — most engineers skip the rollup entirely and leave the data permanently normalized.

> **Forge wins:** Correct rollup for a 5-level, 28-table schema in under 3 seconds.

---

### 4. Schema Drift (Ongoing Maintenance)

FHIR data especially evolves — new claim types introduce new nested fields over time.

**Forge**

```bash
forge-core build --source-type bigquery ...
```

Re-runs the full BFS, detects new fields, generates new models, updates the rollup and all artifacts. `on_schema_change='append_new_columns'` handles incremental additions automatically.

**Manual**

- Detect that a new field appeared (usually because a downstream query failed)
- Identify which model it belongs to
- Add the extraction to that model and all parent rollup CTEs
- Re-test the join logic
- Update `schema.yml` and documentation manually

> **Forge wins:** Single command re-run. Manual requires an audit + partial rewrite every time the source schema changes.

---

### 5. Cross-Warehouse Portability

**Forge**

Change one parameter:

```python
build_core(source_type="snowflake", ...)
```

All SQL, templates, and rollup logic switch to Snowflake syntax automatically.

**Manual**

BigQuery uses `JSON_EXTRACT_ARRAY`, `JSON_VALUE`, `UNNEST()` with `CROSS JOIN`.  
Snowflake uses `PARSE_JSON`, `FLATTEN()`, `value:field::string`.  
Databricks uses `from_json()`, `explode()`, `schema_of_json()`.

Porting a 28-model BigQuery schema to Snowflake is a full rewrite of every `.sql` file.

> **Forge wins:** No rewrite. The adapter pattern handles all warehouse-specific SQL syntax.

---

## Total Effort Estimate

| Task | Forge | Manual |
|---|---|---|
| Schema discovery | 8 min (automated) | 2–5 days |
| SQL model authoring (28 models) | 8 min (automated) | 2–3 weeks |
| Rollup view | 8 min (automated) | 3–5 days |
| schema.yml + JSON Schema + Mermaid | 8 min (automated) | 1–2 days |
| Maintenance per schema change | ~8 min | 4–8 hours per change |
| Cross-warehouse port | ~0 min | Full rewrite |
| **Total (first run)** | **~8 minutes** | **4–6 weeks** |

---

## When Manual Still Makes Sense

- You have a completely flat JSON structure (1 level, no arrays) — the overhead of Forge isn't worth it
- You need non-standard SQL that Forge's templates don't produce
- You only have 2–3 fields to extract and the schema will never change

For anything with 2+ nesting levels, arrays, or schema drift: Forge wins on every dimension.
