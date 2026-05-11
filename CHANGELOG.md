# Changelog

All notable changes to `foxtrotcommunications-forge-core` are documented here.

This project follows [Semantic Versioning](https://semver.org/).

---

## [0.1.3] — 2026-05-11

### Breaking
- **Naming convention:** Removed `frg__` prefix from all generated table names — models
  now use the root table name directly. Existing runs with `frg__` prefixed tables will
  need a clean rebuild.

### Fixed
- **Critical:** Prevent NaN in table name truncation ranks when `groupby.apply` returns
  float values — replaced with an iterative loop and `fillna(1)` safety net.
- Fix alias collision in BigQuery array templates — parent table now aliased to prevent
  root column name conflicts.
- Respect `FORGE_PROJECT_DIR` env var for model and dbt output paths.
- Accept `**kwargs` in `execute_iterative_unnesting` for forward compatibility.
- Stable table naming — prevent rank shifts on incremental runs.

### Changed
- Removed PostgreSQL adapter support (added and removed during this cycle).
- Removed debug logging added during NaN investigation — kept `fillna(1)` safety net.

### Docs
- Updated README with `frg→root` naming convention.
- Added formal determinism and correctness proof.
- Added space and time complexity analysis.
- Added Forge vs manual comparison (FHIR claims benchmark).
- Added BigQuery cost analysis and querying comparison.
- Added incremental loading section and logging setup to README.
- Removed Redshift beta and PostgreSQL from supported warehouses.

### CI
- Added ruff lint job and fixed all lint errors.
- Added `merge_group` trigger for merge queue support.

---

## [0.1.2] — 2026-04-22

### Fixed
- **Critical:** SQL template files (`adapters/templates/**/*.sql`) were missing from
  the published wheel. Added `adapters/templates/**/*` to `[tool.setuptools.package-data]`.
  All installs from v0.1.0 and v0.1.1 will hit `Template not found` errors at runtime —
  upgrade to v0.1.2.

---

## [0.1.1] — 2026-04-22

### Fixed
- README: replaced stale `forge-core[bigquery]` references in Supported Warehouses
  table with the correct full package name `foxtrotcommunications-forge-core[bigquery]`

### Added
- Test suite (109 tests) covering `build_context`, `schema`, `json_schema`,
  `schema_writer`, `profiles`, and adapter factory/ABC
- GitHub Actions CI workflow (`ci.yml`) — runs pytest on Python 3.9 + 3.11
- Coverage config in `pyproject.toml` — 60% threshold on pure-logic modules (77% actual)
- Fixed `pyproject.toml` `[all]` extra that incorrectly referenced `forge-core`

---

## [0.1.0] — 2026-04-21

### Initial release

**Core engine**
- Deterministic BFS (breadth-first search) decomposition of nested JSON columns into normalized dbt models
- One dbt model generated per nested object or array, with a stable naming convention encoding the nesting path
- Rollup view (`frg__rollup`) that reassembles all normalized tables back into nested STRUCT/ARRAY form

**Warehouse adapters**
- BigQuery adapter (Application Default Credentials + service account)
- Snowflake adapter (keypair auth via environment variables)
- Databricks adapter (OAuth M2M via environment variables)
- Redshift adapter (psycopg2, beta)

**Artifacts generated per run**
- dbt SQL models (one per discovered nested structure)
- `schema.yml` — structural column inventory
- `schema.json` — standard JSON Schema draft-07
- `schema.mmd` — Mermaid ER diagram
- dbt docs (`index.html`)

**Packaging**
- `pyproject.toml` with optional warehouse extras: `[bigquery]`, `[snowflake]`, `[databricks]`, `[redshift]`
- CLI entry point: `forge-core build`
- Python API: `from forge_core import build_core`
- Supports Airflow, containers, and local execution

**Auth**
- Application Default Credentials (ADC) for BigQuery
- Standard environment variables for all warehouses (`SNOWFLAKE_*`, `DATABRICKS_*`, `REDSHIFT_*`)
- No hardcoded keys or proprietary credential paths

**Legal**
- Apache 2.0 license
- NOTICE file
- Full IP audit — zero proprietary SaaS references

---

## Versioning Policy

- **Patch** (`0.1.x`) — bug fixes, no breaking changes
- **Minor** (`0.x.0`) — new adapters, new artifact types, backward-compatible features  
- **Major** (`x.0.0`) — breaking changes to `build_core()` API or output schema format
