# Forge: Space and Time Complexity Analysis

Formal complexity bounds for Forge's BFS decomposition algorithm on arbitrary JSON input.

---

## 1. Input Model

We characterize a JSON document $J$ by five parameters:

| Symbol | Meaning |
|:---|:---|
| $N$ | Total number of rows (records) in the source table |
| $D$ | Maximum nesting depth of the JSON structure |
| $K$ | Maximum number of keys at any single nesting level |
| $B$ | Maximum branching factor — number of nested (non-scalar) fields at any level |
| $W$ | Total number of scalar values across all rows and all nesting levels (the "data volume") |

These are related but independent. A wide, shallow document has large $K$, small $D$. A deeply nested document with few fields per level has large $D$, small $K$.

**Derived quantities:**

| Symbol | Definition | Meaning |
|:---|:---|:---|
| $M$ | $\leq \frac{B^{D+1} - 1}{B - 1}$ | Upper bound on total models generated (internal nodes of the schema tree) |
| $R$ | $\sum_{\text{model}} \lvert\text{rows}\rvert$ | Total rows materialized across all models |
| $S$ | Total scalar columns across all models | Determines warehouse storage width |

---

## 2. Time Complexity

### 2.1 BFS Traversal — Schema Discovery

The BFS processes exactly $D$ levels. At each level, two operations run per field:

| Operation | Cost | Invocations per level |
|:---|:---|:---|
| `GET_KEYS` | One SQL query scanning the parent table | Once per parent model at that level |
| `GET_TYPES` | One SQL query per key (batched in groups of 50 via `UNION DISTINCT`) | $\lceil K / 50 \rceil$ queries per parent model |

> **Note:** The batch size of 50 is a hardcoded implementation constant in `discovery.py` (`sub_object_count`), chosen to stay within BigQuery's query-length limits for `UNION DISTINCT` chains. It is not a BigQuery API limit.

**Total schema discovery queries:**

$$Q_{\text{discovery}} = \sum_{d=0}^{D} m_d \cdot \left(1 + \left\lceil \frac{K_d}{50} \right\rceil\right)$$

where $m_d$ is the number of models at depth $d$ and $K_d$ is the key count at that depth.

**Worst case:** $m_d = B^d$ (every field branches), $K_d = K$ at every level:

$$Q_{\text{discovery}} \leq \sum_{d=0}^{D} B^d \cdot \left(1 + \left\lceil \frac{K}{50} \right\rceil\right) = \frac{B^{D+1} - 1}{B - 1} \cdot \left(1 + \left\lceil \frac{K}{50} \right\rceil\right)$$

$$\boxed{Q_{\text{discovery}} = O\!\left(M \cdot \frac{K}{50}\right) = O\!\left(\frac{B^D \cdot K}{50}\right)}$$

**Typical case (FHIR):** $D \approx 4$, $B \approx 5$, $K \approx 20$ → $M \approx 780$, $Q \approx 780$ queries.

---

### 2.2 Model Materialization — dbt Build

At each BFS level, Forge runs one `dbt build` command that materializes all models at that level. Each model is an incremental SQL `INSERT ... SELECT` with `UNNEST`.

| Operation | Cost per model |
|:---|:---|
| SQL `UNNEST` + `SELECT` | $O(N_{\text{parent}} \cdot A)$ where $A$ = average array length |
| Write to warehouse | $O(\text{rows} \times \text{cols})$ |

**Total rows materialized across all models:**

At depth $d$, each model's rows are derived from its parent at depth $d - 1$ by unnesting arrays of average length $A_d$. The row count at depth $d$ is:

$$R_d = R_{d-1} \cdot A_d \qquad R_0 = N$$

Summing across all levels:

$$R = \sum_{d=0}^{D} R_d = N + N \cdot A_1 + N \cdot A_1 \cdot A_2 + \cdots = N \cdot \sum_{d=0}^{D} \prod_{l=1}^{d} A_l$$

**Worst case** (all arrays, uniform length $A$):

$$\boxed{R = N \cdot \frac{A^{D+1} - 1}{A - 1}}$$

The $O(N \cdot A^D)$ term is the **deepest-level contribution** only. The geometric sum above is the exact total across all levels. For $A \geq 2$, the deepest level dominates, so $R = \Theta(N \cdot A^D)$.

**Typical case (FHIR):** $N = 1000$, $A \approx 3$, $D = 4$ → $R = 1000 \cdot \frac{3^5 - 1}{2} = 121{,}000$ total rows.

---

### 2.3 dbt Invocations

Forge runs `dbt build` once per BFS level, not once per model:

$$\boxed{\text{dbt invocations} = D}$$

This is a critical optimization. Without level-batching, it would be $M$ invocations (one per model), each with dbt startup overhead (~2-5 seconds).

---

### 2.4 Parallelism — ThreadPoolExecutor

Within each BFS level, schema discovery tasks run in parallel with `max_workers=20`. The wall-clock time for level $d$ is:

$$T_d = \underbrace{\left\lceil \frac{m_d}{20} \right\rceil \cdot t_{\text{query}}}_{\text{discovery}} + \underbrace{T_{\text{dbt}}(R_d)}_{\text{materialization}}$$

where $t_{\text{query}}$ is the average discovery query latency and $T_{\text{dbt}}(R_d)$ is the dbt build time for materializing $R_d$ rows at level $d$. The dbt time scales with data volume: $T_{\text{dbt}}(R_d) = O(R_d) = O(N \cdot A^d)$.

Total wall-clock:

$$\boxed{T_{\text{wall}} = \sum_{d=0}^{D} \left(\left\lceil \frac{m_d}{20} \right\rceil \cdot t_{\text{query}} + T_{\text{dbt}}(N \cdot A^d)\right)}$$

The dbt term dominates at deeper levels since $R_d$ grows exponentially.

---

### 2.5 Summary — Time

| Phase | Complexity | Dominant cost |
|:---|:---|:---|
| Schema discovery | $O(B^D \cdot K / 50)$ | SQL queries to warehouse |
| Materialization | $O(N \cdot A^D)$ total rows | Warehouse write throughput |
| dbt orchestration | $O(D)$ invocations | dbt startup overhead |
| Rollup generation | $O(M) = O(B^D)$ | One SQL file per model |
| **Total** | $O(N \cdot A^D + B^D \cdot K)$ | **Two independent exponentials** |

> **Note on dominance:** The two exponential terms have different bases ($A$ for materialization, $B$ for schema discovery) and different linear factors ($N$ vs. $K$). Which dominates depends on the data shape:
> - **Large arrays, sparse branching** ($A \gg B$): materialization dominates.
> - **Dense branching, small arrays** ($B \gg A$): schema discovery dominates.
> - **Typical FHIR data** ($A \approx 3$, $B \approx 5$, $N \gg K$): materialization dominates.

---

## 3. Space Complexity

### 3.1 Warehouse Storage (BigQuery)

Each model is a table in the target dataset. The total storage is:

$$\text{Storage} = \sum_{i=1}^{M} \text{rows}_i \times \text{cols}_i \times \bar{s}$$

where $\bar{s}$ is the average bytes per scalar value.

**Column count per model:** Each model stores the scalar fields at that nesting level, plus two metadata columns (`ingestion_hash`, `idx`):

$$\text{cols}_i = \lvert S_i \rvert + 2$$

where $S_i$ is the set of scalar fields at model $i$'s nesting level.

**Total columns across all models:**

$$\boxed{S_{\text{total}} = \sum_{i=1}^{M} (\lvert S_i \rvert + 2) = S + 2M}$$

**Storage bound (worst case):**

$$\boxed{\text{Storage} = O(R \cdot S_{\text{avg}} \cdot \bar{s}) = O(N \cdot A^D \cdot K \cdot \bar{s})}$$

**Typical case (FHIR):** $N = 1000$, $R = 121{,}000$, $S_{\text{avg}} = 8$ cols, $\bar{s} = 50$ bytes → ~48 MB.

---

### 3.2 idx Column Overhead

The `idx` column stores a string of length proportional to the depth. The positional index at each level is bounded differently:

- **Level 0 (root):** bounded by $N$ (number of source records)
- **Level $j > 0$:** bounded by $A_j$ (array length at that level)

$$\lvert\text{idx}\rvert = \underbrace{(\lfloor\log_{10}(N)\rfloor + 1)}_{\text{root component}} + \sum_{j=1}^{d} \underbrace{(\lfloor\log_{10}(A_j)\rfloor + 1)}_{\text{level } j \text{ component}} + d$$

where the $d$ term accounts for underscore delimiters.

**Worst case** (uniform array length $A$):

$$\boxed{\lvert\text{idx}\rvert_{\max} = (\lfloor\log_{10}(N)\rfloor + 1) + D \cdot (\lfloor\log_{10}(A)\rfloor + 1) + D}$$

For $D = 4$, $N = 10{,}000$, $A = 10$: idx = `"10000_10_10_10_10"` = 19 bytes. This is less than 0.1% of a typical row's storage.

---

### 3.3 Storage Amplification Factor

The ratio of total materialized storage to the original source size:

$$\alpha = \frac{\text{total rows across all models}}{\text{source rows}} = \frac{R}{N}$$

**For uniform arrays of length $A$ and depth $D$:**

$$\boxed{\alpha = \frac{A^{D+1} - 1}{A - 1}}$$

| Depth $D$ | Array length $A$ | Amplification $\alpha$ | Interpretation |
|:---|:---|:---|:---|
| 1 | 3 | 4x | Shallow, small arrays |
| 2 | 3 | 13x | Moderate nesting |
| 3 | 5 | 156x | Deep nesting, medium arrays |
| 4 | 10 | 11,111x | Deep nesting, large arrays — rare in practice |
| 4 | 3 | 121x | Typical FHIR depth, small arrays |

> **Note:** In practice, not every field is an array and array lengths vary. Real-world amplification for FHIR data is typically 10–50x, not the theoretical worst case.

---

### 3.4 Local Disk (dbt Project)

Forge generates SQL model files on disk:

| Artifact | Count | Size each | Total |
|:---|:---|:---|:---|
| Model SQL files | $M$ | ~500 bytes–2 KB | $O(M \cdot K)$ bytes |
| `schema.yml` | 1 | $O(M \cdot K)$ | One file |
| Rollup SQL | 1 | $O(M \cdot K)$ | One file |
| `profiles.yml` | 1 | ~200 bytes | Constant |

$$\boxed{\text{Disk} = O(M \cdot K) \text{ bytes}}$$

For $M = 100$, $K = 20$: ~200 KB — less than 0.01% of warehouse storage for any non-trivial dataset.

---

### 3.5 Runtime Memory (Python Process)

| Data structure | Size |
|:---|:---|
| BFS queue | $O(B^d)$ entries at depth $d$, max $O(B^D)$ |
| Metadata list | $O(M)$ entries, each $O(K)$ |
| Types DataFrame | $O(K)$ per model, recycled per level |
| ThreadPoolExecutor | 20 threads, each holding one discovery task |

$$\boxed{\text{Memory} = O(M \cdot K + B^D)}$$

Since $M = O(B^D)$, this simplifies to $O(B^D \cdot K)$.

For $D = 4$, $B = 5$, $K = 20$: ~780 × 20 = 15,600 metadata entries — approximately 1 MB, which is under 0.1% of typical system memory.

---

## 4. Rollup Complexity

The rollup query reconstructs the original nested JSON from the decomposed models.

**Structure:** A cascade of CTEs, one per model, joined bottom-up:

$$\text{CTEs} = M \qquad \text{JOINs} = M - 1$$

**Join cost per step:** Each join matches on `(ingestion_hash, idx prefix)`. With proper indexing (BigQuery partitioning on `ingestion_hash`):

$$\text{Join cost} = O(R_{\text{parent}} + R_{\text{child}})$$

**Total rollup cost:**

$$\boxed{T_{\text{rollup}} = O(R) = O\!\left(N \cdot \frac{A^{D+1} - 1}{A - 1}\right)}$$

Each row is read once across the CTE cascade.

> **Caveat:** BigQuery may not cache intermediate CTEs. In the worst case, a CTE referenced by multiple downstream CTEs could be re-evaluated, multiplying the constant factor. In Forge's rollup structure, each CTE is referenced exactly once by its parent, so re-evaluation does not occur — the cost is a single pass over $R$.

---

## 5. End-to-End Summary

For a JSON source with $N$ rows, max depth $D$, max keys $K$, branching factor $B$, and average array length $A$:

| Metric | Complexity | Typical FHIR value |
|:---|:---|:---|
| **Models generated** | $O(B^D)$ | ~50–200 |
| **Schema queries** | $O(B^D \cdot K / 50)$ | ~200–800 |
| **Total rows materialized** | $O(N \cdot A^D)$ | ~50K–500K |
| **dbt invocations** | $O(D)$ | 3–5 |
| **Warehouse storage** | $O(N \cdot A^D \cdot K \cdot \bar{s})$ | 10–500 MB |
| **Storage amplification** | $(A^{D+1} - 1) / (A - 1)$ | 10–50x |
| **Local disk** | $O(B^D \cdot K)$ bytes | ~200 KB |
| **Python memory** | $O(B^D \cdot K)$ | ~1 MB |
| **Wall-clock time** | $\sum_d T_{\text{dbt}}(N \cdot A^d)$ | 2–10 minutes |

---

## 6. Scaling Behavior

### 6.1 What scales linearly

- **Rows ($N$):** Doubling source rows doubles materialization time and storage. All models scale by the same factor. This is the best-case scaling property.
- **Keys ($K$):** Adding a scalar field to one level adds one column to one model. Cost: $O(N)$ additional bytes.

### 6.2 What scales exponentially

- **Depth ($D$):** Each additional nesting level multiplies total rows by $A$ (the array length). Going from $D = 3$ to $D = 4$ with $A = 5$ increases total rows by 5x.
- **Array length ($A$):** Larger arrays at deep levels are the primary cost driver. A single array of 1,000 elements at depth 3 creates $N \times 1000$ rows in that model alone.

### 6.3 Practical mitigations

| Concern | Mitigation |
|:---|:---|
| Deep nesting ($D > 5$) | Rare in real-world APIs. FHIR maxes at $D \approx 4$. |
| Large arrays at depth | Use `LIMIT` parameter to sample during development |
| Storage cost | BigQuery charges ~\$0.02/GB/month; even 500 MB costs \$0.01/month |
| Materialization time | dbt incremental mode only processes new rows after first run |
