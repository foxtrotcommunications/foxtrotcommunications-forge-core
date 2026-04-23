# Forge vs Raw: Querying Decomposed Data vs Nested JSON

A practical comparison of query complexity, performance, and usability when working with Forge-decomposed tables versus querying raw nested JSON directly in the warehouse.

Real context: FHIR `ExplanationOfBenefit` claims — 6.2M rows, 5 levels of nesting, ~80 distinct fields.

---

## The Core Trade-Off

| | **Raw JSON** | **Forge Tables** |
|---|---|---|
| Storage | 1 table | ~28 tables + rollup view |
| Query syntax | `JSON_VALUE`, `JSON_EXTRACT_ARRAY`, `UNNEST` | Standard `SELECT ... FROM ... JOIN` |
| Index/partition support | Limited (only on non-JSON columns) | Full (partitioned on `ingestion_timestamp`, clustered) |
| Schema visibility | Opaque — must inspect sample rows | Explicit — every field is a typed column in `schema.yml` |
| Aggregation across arrays | Complex, error-prone | Standard `GROUP BY` |
| BI tool compatibility | Poor — most tools can't navigate JSON | Full — each table looks like a normal relational table |

---

## Example 1: Simple Scalar Lookup

**Task:** Get the total amount and currency for each claim.

### Raw JSON (BigQuery)

```sql
SELECT
    JSON_VALUE(raw_json, '$.id') AS claim_id,
    JSON_VALUE(raw_json, '$.total.value') AS total_value,
    JSON_VALUE(raw_json, '$.total.currency') AS total_currency
FROM `project.dataset.raw_claim`
```

### Forge

```sql
SELECT
    id AS claim_id,
    total_value,
    total_currency
FROM `project.dataset.frg__root`
```

**Verdict:** Roughly equivalent for flat fields. Forge is slightly cleaner (no `JSON_VALUE` wrappers), but the difference is minor.

---

## Example 2: Single Array Access

**Task:** List all line items for each claim with their sequence number and revenue code.

### Raw JSON (BigQuery)

```sql
SELECT
    JSON_VALUE(raw_json, '$.id') AS claim_id,
    JSON_VALUE(item, '$.sequence') AS sequence,
    JSON_VALUE(item, '$.revenue.coding[0].code') AS revenue_code
FROM `project.dataset.raw_claim`,
UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.item')) AS item
```

### Forge

```sql
SELECT
    r.id AS claim_id,
    i.sequence,
    i.revenue_code
FROM `project.dataset.frg__root` r
JOIN `project.dataset.frg__root__item1` i
    ON r.ingestion_hash = i.ingestion_hash
    AND SPLIT(r.idx, '_')[OFFSET(0)] = SPLIT(i.idx, '_')[OFFSET(0)]
```

**Verdict:** Raw is more concise for a single UNNEST. Forge requires a join but gives you typed columns and the ability to filter/aggregate on the child table independently.

---

## Example 3: Nested Array (2+ Levels Deep)

**Task:** For each claim, list every diagnosis code across all line items.

### Raw JSON (BigQuery)

```sql
SELECT
    JSON_VALUE(raw_json, '$.id') AS claim_id,
    JSON_VALUE(item, '$.sequence') AS item_sequence,
    JSON_VALUE(dx, '$.code') AS diagnosis_code,
    JSON_VALUE(dx, '$.display') AS diagnosis_display
FROM `project.dataset.raw_claim`,
UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.item')) AS item,
UNNEST(JSON_EXTRACT_ARRAY(item, '$.diagnosisCodeableConcept.coding')) AS dx
```

Problems with the raw approach:
- Double `UNNEST` creates a cross-product risk if the path is wrong
- `diagnosisCodeableConcept.coding` must be known exactly — no schema.yml to consult
- If `coding` is sometimes an object and sometimes an array, `JSON_EXTRACT_ARRAY` fails silently
- No way to filter only the second level without re-traversing the first

### Forge

```sql
SELECT
    r.id AS claim_id,
    i.sequence AS item_sequence,
    dx.code AS diagnosis_code,
    dx.display AS diagnosis_display
FROM `project.dataset.frg__root` r
JOIN `project.dataset.frg__root__item1` i
    ON r.ingestion_hash = i.ingestion_hash
    AND SPLIT(r.idx, '_')[OFFSET(0)] = SPLIT(i.idx, '_')[OFFSET(0)]
JOIN `project.dataset.frg__root__item1__diag1` dx
    ON i.ingestion_hash = dx.ingestion_hash
    AND SPLIT(i.idx, '_')[OFFSET(0)] = SPLIT(dx.idx, '_')[OFFSET(0)]
    AND SPLIT(i.idx, '_')[OFFSET(1)] = SPLIT(dx.idx, '_')[OFFSET(1)]
```

**Verdict:** Forge's join syntax is more verbose, but:
- Each table can be queried independently (e.g., `SELECT * FROM frg__root__item1__diag1` — no UNNEST chain needed)
- Types are already resolved — `code` is a `STRING` column, not a JSON path
- No cross-product risk — the idx-based join is structurally correct by construction

---

## Example 4: Aggregation Across Nesting Levels

**Task:** Total paid amount per provider, summing across all claim line items.

### Raw JSON (BigQuery)

```sql
SELECT
    JSON_VALUE(raw_json, '$.provider.reference') AS provider,
    SUM(CAST(JSON_VALUE(item, '$.net.value') AS FLOAT64)) AS total_net
FROM `project.dataset.raw_claim`,
UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.item')) AS item
GROUP BY 1
```

Problems:
- The `CAST` is required because `JSON_VALUE` always returns `STRING`
- If any `net.value` is malformed, the entire query fails
- No type safety — you won't know `net.value` should be `FLOAT64` until you try

### Forge

```sql
SELECT
    r.provider_reference AS provider,
    SUM(i.net_value) AS total_net
FROM `project.dataset.frg__root` r
JOIN `project.dataset.frg__root__item1` i
    ON r.ingestion_hash = i.ingestion_hash
    AND SPLIT(r.idx, '_')[OFFSET(0)] = SPLIT(i.idx, '_')[OFFSET(0)]
GROUP BY 1
```

**Verdict:** Forge wins clearly. `net_value` is already typed as `FLOAT64` by the decomposition — no casting, no risk of runtime failure from malformed values.

---

## Example 5: Direct Child Table Query (Forge Only)

**Task:** Find all diagnosis codes matching "E11" across the entire dataset.

### Raw JSON

```sql
SELECT
    JSON_VALUE(raw_json, '$.id') AS claim_id,
    JSON_VALUE(dx, '$.code') AS dx_code
FROM `project.dataset.raw_claim`,
UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.diagnosis')) AS dx_obj,
UNNEST(JSON_EXTRACT_ARRAY(dx_obj, '$.diagnosisCodeableConcept.coding')) AS dx
WHERE JSON_VALUE(dx, '$.code') LIKE 'E11%'
```

Must traverse from root → diagnosis → coding every time, scanning all 6.2M claims even if only 50K have E11 codes.

### Forge

```sql
SELECT *
FROM `project.dataset.frg__root__diag1__diag2`
WHERE code LIKE 'E11%'
```

One table, one `WHERE` clause, no UNNESTs. BigQuery scans only the diagnosis coding table (~2M rows, partitioned), not the full 6.2M claim table.

**Verdict:** This is Forge's biggest advantage. Decomposed child tables can be queried directly without traversing the full nesting hierarchy.

---

## Performance Comparison

| Metric | Raw JSON | Forge |
|---|---|---|
| **Bytes scanned** (Example 3) | Full table — every row, every byte of the JSON column | Only the columns referenced in the specific child tables |
| **Partition pruning** | Only on non-JSON columns (e.g., ingest date) | On `ingestion_timestamp` in every model |
| **Column pruning** | Not possible — JSON is one opaque column | Full columnar pruning — BigQuery reads only referenced columns |
| **Slot time** (Example 5, 6.2M rows) | ~15–30 sec (full scan + 2× UNNEST) | ~2–5 sec (direct child table scan) |
| **BI tool compatibility** | Requires custom SQL / JSON functions | Works natively with Looker, Tableau, Metabase, etc. |
| **Caching** | Cached per query | Each table cached independently by BigQuery |

---

## When Raw is Better

| Scenario | Why raw wins |
|---|---|
| Ad-hoc exploration | `JSON_VALUE(raw, '$.some.new.field')` is faster than running Forge first |
| One-off extractions | If you need 2 fields once, the UNNEST is simpler than a build |
| Unstable schemas | If the schema changes daily, the Forge rebuild overhead may not be worth it |
| Very small datasets | Under ~1,000 rows, the Forge overhead (dbt build, BFS discovery) exceeds the query savings |

## When Forge is Better

| Scenario | Why Forge wins |
|---|---|
| Repeated querying | Typed columns + partition pruning compound savings over hundreds of queries |
| Deep nesting (3+ levels) | Multi-level UNNEST chains are error-prone and expensive |
| Aggregation pipelines | `GROUP BY` on typed columns with column pruning vs. full JSON scans |
| BI / dashboards | BI tools expect relational tables, not JSON blobs |
| Team access | Analysts can query `frg__root__item1` without knowing JSON path syntax |
| Data quality | `schema.yml` documents every column — impossible with raw JSON |
| Production pipelines | Incremental loading (`clean=False`) makes ongoing updates efficient |

---

## BigQuery Cost Analysis

BigQuery charges for two things: **storage** and **compute** (bytes scanned). Understanding both is essential for evaluating the Forge trade-off.

### Pricing (on-demand, as of 2024)

| Resource | Cost |
|---|---|
| **Active storage** | $0.02 / GB / month |
| **Long-term storage** (table unmodified 90+ days) | $0.01 / GB / month |
| **On-demand queries** | $6.25 / TB scanned |
| **First 1 TB/month** | Free |

### Storage Cost: Raw vs Forge

Using the FHIR claims example (6.2M rows, 28 models):

| | Raw JSON | Forge Tables |
|---|---|---|
| **Source table size** | ~12 GB | ~12 GB (still exists) |
| **Decomposed tables** | — | ~2.4 GB (columnar, typed — smaller than JSON blob) |
| **Rollup view** | — | 0 GB (view, not materialized) |
| **Total storage** | 12 GB | 14.4 GB |
| **Monthly cost** | $0.24 | $0.29 |
| **Delta** | — | **+$0.05/month** |

> **Why Forge tables are smaller than you'd expect:** The raw JSON column stores field names, brackets, quotes, and whitespace in every row. Forge extracts only the values into typed columns — `FLOAT64` (8 bytes) vs the JSON string `"123.45"` (8 bytes + key name + quotes + braces). For deeply nested data, Forge tables are often **smaller** in total than the JSON column they came from.

### Compute Cost: Per-Query Savings

BigQuery charges by bytes scanned. The critical difference:

- **Raw JSON:** Every query scans the **entire JSON column** (~12 GB) regardless of which fields you need. `JSON_VALUE` runs *after* the full column is read.
- **Forge:** BigQuery reads only the **specific columns referenced** in your query. A query touching 3 columns out of 15 reads ~20% of the table.

| Query | Raw JSON (bytes scanned) | Forge (bytes scanned) | Savings |
|---|---|---|---|
| Example 1: Scalar lookup (3 fields) | ~12 GB (full JSON column) | ~0.8 GB (3 typed columns from root) | **93%** |
| Example 3: Nested array (2 levels) | ~12 GB | ~0.4 GB (child table, 4 columns) | **97%** |
| Example 4: Aggregation | ~12 GB | ~0.6 GB (root + child, 3 columns) | **95%** |
| Example 5: Direct child filter | ~12 GB | ~0.15 GB (child table only, 2 columns) | **99%** |

### Cost Per Query

At $6.25/TB:

| Query type | Raw JSON cost | Forge cost |
|---|---|---|
| Scalar lookup | $0.075 | $0.005 |
| Nested array access | $0.075 | $0.003 |
| Aggregation across levels | $0.075 | $0.004 |
| Direct child table filter | $0.075 | $0.001 |

### Break-Even Analysis

The extra storage cost of Forge is **$0.05/month**. Each query saves ~$0.07 in compute.

$$\text{Break-even} = \frac{\$0.05/\text{month}}{\$0.07/\text{query}} \approx 1 \text{ query/month}$$

**If you query the dataset more than once per month, Forge pays for itself.**

For a team running 50 queries/day against the same dataset:

| | Raw JSON | Forge |
|---|---|---|
| Monthly query cost | 50 × 30 × $0.075 = **$112.50** | 50 × 30 × $0.004 = **$6.00** |
| Monthly storage cost | $0.24 | $0.29 |
| **Monthly total** | **$112.74** | **$6.29** |
| **Annual savings** | — | **$1,277/year** |

### Capacity Pricing (Slots)

For organizations using BigQuery Editions (slot-based pricing) instead of on-demand:

| Edition | Cost | 100-slot commitment |
|---|---|---|
| Standard | $0.04/slot-hour | ~$120/month |
| Enterprise | $0.06/slot-hour | ~$180/month |

Under slot pricing, the cost advantage shifts from bytes-scanned to **slot-time**:
- Raw JSON queries consume more slot-seconds because `JSON_VALUE` parsing is CPU-intensive
- Forge queries on typed columns use fewer slots and complete faster
- The same slot budget supports **3–5x more Forge queries** than raw JSON queries

---

## Summary

Raw JSON queries are acceptable for **ad-hoc, shallow, one-off** extractions. Forge-decomposed tables are superior for **repeated, deep, team-shared, production** workloads — primarily because:

1. **Column pruning** — BigQuery reads only the columns you reference, not the entire JSON blob
2. **Direct child access** — query any nesting level without traversing from root
3. **Type safety** — no `CAST(JSON_VALUE(...) AS FLOAT64)` chains
4. **BI compatibility** — every table is a standard relational table
5. **Incremental efficiency** — only new rows are processed on subsequent runs
6. **Cost** — 93–99% reduction in bytes scanned per query; breaks even at 1 query/month

