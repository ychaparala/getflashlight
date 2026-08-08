# Design: Efficiency / waste view (the second data plane)

> Status: **Phase 1a implemented** (branch `efficiency-waste`). Evidence base: [`../research/databricks-cost-waste.md`](https://getflashlight.app/research/databricks-cost-waste/index.md). Adds an **efficiency-waste** capability alongside the existing FOCUS/TCO product. Built: the `EfficiencyRecord` contract, the `metrics/` plane, `Connector.fetch_efficiency` (Databricks), `050_gold_waste.sql` classification, the `efficiency` GOLD group, and the dashboard page. **Validated end-to-end against a live warehouse (2026-06-28):** column names, joins, and classification confirmed on real data; interactive moved to cluster grain and `placement` restricted to all-purpose as a result. Open: Phase 1b Model Serving. See **Build order**.

## The one decision this design turns on

Efficiency waste = **billed − justified-by-use**, and that gap is **invisible to FOCUS**. `system.billing.usage` has no utilization and no dollars. So measuring waste needs a **second data plane** (utilization telemetry) that does **not** fit `FocusRecord`.

Therefore: keep the FOCUS plane untouched, add a **parallel metrics plane** that **reuses the existing lake machinery** (partition-replace BRONZE writes, in-memory DuckDB, atomic GOLD publish) rather than a second framework.

> **Scope reversal — must be acknowledged.** This reverses CLAUDE.md's *"does not produce optimization recommendations in v1 … Don't reintroduce analyzers/ recommendations."* This view is **measurement of recoverable spend**, not automated remediation — but CLAUDE.md must be updated when this is built. See §7.

______________________________________________________________________

## Implementation plan (at a glance)

**Approach:** mirror FOCUS, for waste. One standardized `EfficiencyRecord`, aggregated at source, written through the existing medallion machinery, classified by **one** GOLD view, rendered by **one** faceted dashboard. New platforms/compute-classes become *rows*, not code.

```
        DATABRICKS (SQL warehouse)
              │  ONE connector, TWO pulls (reuse _execute, _resolve_account_prices)
   fetch() ───┴─── fetch_efficiency() [NEW]
   FocusRecord            EfficiencyRecord  (GROUP BY entity,month AT SOURCE)
        │                        │
 bronze.write_window   lake.metrics.write_efficiency [NEW: copies purge+COPY kernel]
        ▼                        ▼
   bronze/x_source_connector=…   metrics/provider_name=…/charge_month=…
        └──────────┬─────────────┘
                   ▼  transform.build_gold()  (in-memory DuckDB)
        register_bronze → raw.focus_record
        register_metrics → metrics.efficiency_record   [NEW]
        sql/010,020,030 (FOCUS, unchanged)   +   sql/050_gold_waste.sql [NEW: classify→recoverable]
                   ▼  COPY per group → atomic publish
   gold/<provider>/*                       gold/efficiency/{waste_record,waste_summary_month} [NEW]
                   ▼
   dashboard/views/efficiency_waste.py  — ONE leaderboard: recoverable_$ × category × owner × provider
   FUTURE: BigQuery/Snowflake connector → fetch_efficiency() → same record → same gold → same dashboard
```

**New files (7):** `efficiency/model.py` · `lake/metrics_schema.py` · `lake/metrics.py` · `ingest/connectors/sql/databricks_efficiency.sql` · `transform/sql/050_gold_waste.sql` · `dashboard/views/efficiency_waste.py` · `tests/test_waste_classification.py`

**Touched (8):** `lake/paths.py` (`metrics_dir`) · `lake/duck.py` (`register_metrics`) · `ingest/base.py` (`fetch_efficiency`=None default) · `ingest/connectors/databricks.py` (implement it) · `ingest/runner.py` (efficiency pull→write before `build_gold`) · `transform/runner.py` (`register_metrics` + efficiency COPY loop) · `transform/catalog.py` (`EFFICIENCY_GROUP` + 2 ViewSpecs) · `dashboard/app.py` + `CLAUDE.md` (register page; scope note).

Detail per section below; phase sequence in **Build order** at the end.

> Phase 0 ships the `placement` category from FOCUS-only data (zero metrics plane) so the leaderboard is live before the telemetry pull exists. `# ponytail: efficiency pull is best-effort — it warns+skips on failure, never aborts the canonical cost ingest.`

______________________________________________________________________

## Architecture

```
FOCUS plane (existing, untouched)         Metrics plane (new)
  bronze/  (FocusRecord, Hive-part)         metrics/  (system-table aggregates)
     │                                          │
     ├─ silver.focus_normalized  ◄──────────────┤  (join spine: cluster_id, month)
     │                                          │
     └─ gold/<provider>/*  gold/shared/*       silver.waste_*  →  gold/efficiency/*
```

### Extensibility model — the existing pattern *is* the extension mechanism

The medallion + `sql/*.sql` glob + `ViewSpec` catalog is already the extensible architecture. The metrics plane **mirrors** it; nothing new is invented. Three extension points, ordered by how often they actually change:

1. **New waste signal (frequent)** → add a view to `050_gold_waste.sql` + one `ViewSpec` in `EFFICIENCY_BASE_VIEWS`. No plumbing — `build_gold` already globs `sql/*.sql` and the catalog is data-driven. This is the only axis that changes often, and it's already free.
1. **New telemetry dataset (rare)** → one typed Arrow schema + one fetch method + register it. Days apart, not hours.
1. **New provider (someday)** → `efficiency` is a *fixed Databricks group* in v1. Make it provider-keyed only when a 2nd provider (EKS/BigQuery rightsizing) needs it.

**Rejected as over-engineering:** a generic metric DSL/registry, an EAV metric table, and a shared `DataPlane` abstraction over FOCUS+metrics. The two planes share a 15-line COPY kernel, not a contract — see the writer note below.

### New on-disk layout — `lake/paths.py`

Add a sibling root to `bronze_dir()`:

```
def metrics_dir() -> Path:
    """Utilization-telemetry root, Hive-partitioned by dataset + month.
    metrics/<dataset>/charge_month=YYYY-MM-01/  (dataset ∈ cluster_util_daily,
    cluster_config, job_runs, query_stats)."""
    return home() / "metrics"
```

Add `metrics_dir()` to `ensure_layout()`. A new fixed GOLD group `efficiency/` joins `gold_signature()`'s glob (`gold/*/*.parquet`) for free — no path change needed.

### Grain — match the customer's lever, per compute class

Grain is **not** uniform, and **not** per-execution. A customer tunes a *job*, moves a *workload*, bills a *user*, or right-sizes an *endpoint* — never "a cluster", and never "run #37 of 60". The principle:

> **Grain = the smallest entity the customer can act on, × month, carrying averages + a count for confidence. Aggregated AT SOURCE — store the distribution stats, not the rows.**

A job runs 30–60×/month, so `job_id × month` is a **distribution, not a sum**: run_count, CPU-weighted avg util, and `pct_runs_underutilized` (consistency = confidence — 58/60 underutilized is a sure tune; 2/60 is noise). There is **no value in storing the 60 run rows** — the warehouse query computes these stats during the pull; drill-to-run is the Databricks Jobs UI's job, not ours.

Output splits into two categories:

- **Shadow waste** (efficiency) — billed-but-not-used; fixed by tuning the workload.
- **Opportunity** (placement/right-size) — real work on the wrong (~3–4×) or idle compute; fixed by moving/scaling it, billed to the owner meanwhile.

| Compute class              | Detect via                                              | Telemetry grain *available*                                                   | **Action grain (GOLD)**                                                                                | Lens                                                                         |
| -------------------------- | ------------------------------------------------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| Jobs / DLT                 | `billing_origin_product = JOBS`                         | per **job_run** (node util ∩ run window)                                      | **`job_id` × month** (run_count, avg util; drill → run)                                                | shadow waste: underutil, failed/retry                                        |
| All-purpose (classic)      | `billing_origin_product = ALL_PURPOSE`                  | cluster-level                                                                 | **`cluster_id` × month** (cluster util; owner = `single_user_name` if single-user, else heaviest user) | underutilized/idle cluster + move-to-jobs opportunity                        |
| Notebooks (serverless)     | `billing_origin_product IN ('INTERACTIVE','NOTEBOOKS')` | per-notebook (`usage_metadata.notebook_id`)                                   | **`notebook_id` × month** (real per-notebook cost, owner = `run_as` directly)                          | move-to-jobs opportunity (candidate)                                         |
| SQL warehouse              | `billing_origin_product = SQL`                          | warehouse + `query.history`                                                   | **`warehouse_id` × month**                                                                             | attribution; query-pattern health; serverless-vs-classic pricing (candidate) |
| SQL warehouse, per user    | `system.query.history.executed_by`                      | per-query duration                                                            | **`warehouse_id:user` × month** (cost allocated by duration share — estimate)                          | user concentration + high-frequency serverless cadence (candidate)           |
| **Model Serving**          | `billing_origin_product = MODEL_SERVING`                | endpoint request-rate vs provisioned (serving system tables — *verify names*) | **`endpoint` × month**                                                                                 | idle provisioned endpoint / no scale-to-zero                                 |
| **Photon** (cross-cutting) | `photon = true` on any of the above                     | inherits the host workload's grain                                            | the host workload                                                                                      | premium $ on no-gain workloads (**candidate**; real A/B needs with/without)  |

**Honest limitation that forces the grain:** `node_timeline` is per node, keyed by `cluster_id` — so **cluster** utilization is honest, but **per-user** CPU% on a shared cluster is not. Hence the entity is the **cluster** for all-purpose (cluster util → underutilized/idle is honest; `owner_user` is a best-effort heaviest-user hint, or the hard `single_user_name` fact when the cluster is single-user), and the **job** for jobs. SQL warehouses expose no per-entity utilization (`utilization_pct` = NULL) → never flagged underutilized; their waste is query-pattern health, per-user cost concentration, cadence, and serverless-vs-classic pricing signals instead. Model Serving waste is *idle-provisioned*, not utilization-based. *(Validated against a live warehouse 2026-06-28 — see Build order.)*

**Classic vs. serverless notebooks — not a shared-vs-single-user distinction.** Whether notebook-level cost is *measurable* depends on classic vs. serverless billing, not access mode: classic all-purpose clusters (shared or single-user) meter cost at the **cluster** level — `system.billing.usage` carries no notebook identity for them at all, so notebook-level attribution there would need a new telemetry source (`system.access.audit` notebook attach/command events, to time-allocate cluster cost) — an estimate, not a measurement. That stays **BLOCKED** (`classic_notebook_attribution` in `waste_rules.py`), not built. Serverless notebooks get `usage_metadata.notebook_id`/ `notebook_path` populated directly (already proven for FOCUS resource naming in `databricks_focus_1_3.sql`), so their per-notebook `billed_cost` is a real sum today.

### The one standardized schema — `EfficiencyRecord` (the FOCUS-analog for waste)

The goal is **not** the "right structure" — it's the **least standardized data** that lets a customer see *waste + cause + owner across platforms*. FOCUS did this for cost; do the same for efficiency: **one flat record every platform maps into.** The grain table above describes how each Databricks compute class *maps into* this one record — it is not five tables.

**One typed Arrow table, aggregated AT SOURCE, ~11 columns + a JSON.** One row per (entity × month × …). This is a typed fact table with a controlled `entity_type` — NOT EAV; `cause_detail` mirrors `FocusRecord.tags` (standardize the spine, carry specifics in JSON):

```
provider_name      Databricks | BigQuery | Snowflake | …
charge_month
entity_type        job | interactive | notebook | sql_warehouse | sql_warehouse_user |
                   endpoint | storage | table   (platform-agnostic roles)
entity_id
entity_name
owner_user         run_as          (nullable)
owner_project      tag             (nullable)
billed_cost        $  (reconciles to FOCUS)
native_quantity    DBUs | slot-hrs | credits
utilization_pct    0–100, NULL if not measurable     ← the billed-vs-used signal
activity_count     runs | queries | requests          ← idle + confidence
cause_detail       JSON  (run_count, pct_runs_underutilized, failed_cost, photon, …)
```

Partition-replaced by `(provider_name, charge_month)`. `photon` and all category-specific inputs live in `cause_detail`, not as top-level columns — cross-cutting and platform- specific extras stay out of the standardized spine.

`billed_cost` is sourced from `billing.usage ⋈ list_prices` via the connector's existing `_resolve_account_prices` (same rate table as FOCUS), so rates match. The **FOCUS plane stays the canonical headline $**; a reconciliation check sums `EfficiencyRecord.billed_cost` against FOCUS per provider/month and surfaces any gap (attribution honesty).

> ponytail: one record, aggregated at source. The connector emits raw standardized telemetry; the *waste interpretation* (category + recoverable $) is derived in GOLD SQL below, where the metrics contract already lives and the heuristics are tunable.

### New connector — extend `ingest/connectors/databricks.py`

The existing connector already owns the warehouse-execution machinery (`_execute`, chunked `EXTERNAL_LINKS`, polling). **Reuse it.** Add a sibling fetch path whose warehouse SQL does the `GROUP BY entity, month` (unioning job / interactive / endpoint aggregates) and emits **`EfficiencyRecord` rows** — one standardized dataset, not `FocusRecord`.

**Writer — copy the kernel, don't generalize.** `bronze.write_window` is FocusRecord- bound (`build_table`, `collapse_duplicates` via `dedupe_key`). The shared part is only the ~15-line purge-window + `COPY … PARTITION_BY` kernel. Put a small `lake/metrics.py::write_efficiency(window, arrow_table)` that copies it (partition on `(provider_name, charge_month)`). Source-aggregated rows are unique per grain → **no dedupe**. Duplicating 15 boring lines beats a `DataPlane` abstraction both planes share.

> ponytail: one connector, two outputs (FOCUS rows + EfficiencyRecord rows). Split into a separate `databricks_usage` connector only if the auth/warehouse lifecycles diverge.

______________________________________________________________________

## The waste contract — one GOLD classification view

The connector emits raw standardized telemetry (`EfficiencyRecord`); the **waste interpretation is derived in GOLD SQL**, where heuristics are tunable. One SQL file:

- `050_gold_waste.sql` — `gold.waste_record` (classify + recoverable) + `gold.waste_summary_month`.

SILVER is a near-passthrough — `EfficiencyRecord` is already at grain — plus the FOCUS reconciliation check. No per-signal silver views.

### `gold.waste_record` — the consumer table (one row per entity × month × category)

```
-- Classify each EfficiencyRecord into a waste_category + recoverable_cost.
-- One UNPIVOT-style pass: an entity can emit multiple category rows (additive,
-- like FOCUS line items). jobs_rate / multiplier come from list_prices.
WITH e AS (SELECT * FROM metrics.efficiency_record)
-- underutilized (measurable utilization only — never claimed for shared compute)
SELECT provider_name, charge_month, entity_type, entity_id, entity_name,
       owner_user, owner_project, billed_cost,
       'underutilized'                                           AS waste_category,
       round(billed_cost * (1 - utilization_pct/100.0), 2)       AS recoverable_cost,
       CASE WHEN (cause_detail->>'pct_runs_underutilized')::float >= 0.8
            THEN 'high' ELSE 'candidate' END                     AS confidence
FROM e WHERE utilization_pct IS NOT NULL AND utilization_pct <= 20
UNION ALL
-- idle (billed but zero activity)
SELECT provider_name, charge_month, entity_type, entity_id, entity_name,
       owner_user, owner_project, billed_cost,
       'idle', round(billed_cost, 2), 'high'
FROM e WHERE activity_count = 0 AND billed_cost > 0
UNION ALL
-- placement (interactive/sql that could be jobs compute) → OPPORTUNITY
SELECT provider_name, charge_month, entity_type, entity_id, entity_name,
       owner_user, owner_project, billed_cost,
       'placement', round(billed_cost * (1 - :jobs_ratio), 2), 'candidate'
FROM e WHERE entity_type IN ('interactive','sql_warehouse')
UNION ALL
-- failed-run spend
SELECT provider_name, charge_month, entity_type, entity_id, entity_name,
       owner_user, owner_project, billed_cost,
       'failed', (cause_detail->>'failed_cost')::decimal, 'high'
FROM e WHERE (cause_detail->>'failed_cost')::decimal > 0
UNION ALL
-- photon-no-gain (candidate; real A/B needs with/without)
SELECT provider_name, charge_month, entity_type, entity_id, entity_name,
       owner_user, owner_project, billed_cost,
       'photon_no_gain', round(billed_cost * (1 - 1/2.9), 2), 'candidate'
FROM e WHERE (cause_detail->>'photon')::boolean AND utilization_pct <= 20;
```

`gold.waste_summary_month` = `waste_record` rolled to `(charge_month, waste_category)` → total recoverable_cost per category, drives the KPI bar.

**Honesty rules baked in:** failed-run cost is reported, never auto-termination "savings" (research refutation); `photon_no_gain` and `placement` are `candidate` confidence; `underutilized` requires non-NULL `utilization_pct` (jobs + all-purpose clusters; SQL warehouses are NULL → never flagged); `placement` is all-purpose only; `idle` fires only on a **measured** zero activity, never on NULL; WASTE and OPPORTUNITY are separate lenses and are never summed into one headline.

______________________________________________________________________

## Catalog & runner changes

`transform/catalog.py`:

- Add a fixed group constant `EFFICIENCY_GROUP = "efficiency"` next to `SHARED_GROUP`.
- Add `EFFICIENCY_BASE_VIEWS` = two `ViewSpec`s: `waste_record` + `waste_summary_month`.
- `build_catalog` emits the efficiency group like it does `shared`.

`transform/runner.py`:

- After `register_bronze`, add `register_metrics(con)` (registers `metrics.efficiency_record` over `metrics/**/*.parquet`) so `050_gold_waste.sql` can read it.
- Materialize `EFFICIENCY_BASE_VIEWS` into `gold/efficiency/` (a second unfiltered COPY loop mirroring the `shared` loop).

> ponytail: `efficiency` is a *fixed* group, not provider-keyed — `waste_record` already carries `provider_name` as a column, so one group holds every platform. No per-provider fan-out needed (unlike the FOCUS plane).

______________________________________________________________________

## Dashboard — `dashboard/views/efficiency_waste.py`

New page registered next to `tco_overview`. Reads only `gold/efficiency/*` via `gold/reader.py` (GOLD-only invariant preserved). **One table, faceted** — the whole point of the single record is the dashboard doesn't need bespoke panels per signal:

- **KPI bar:** total recoverable $ this month, split high vs candidate confidence (`waste_summary_month`).
- **Leaderboard:** `waste_record` ranked by `recoverable_cost`, columns: entity, type, owner, billed $, `waste_category`, recoverable $, confidence. *The category IS the cause.*
- **Facets/filters:** `waste_category` (underutilized / idle / placement / failed / photon), `provider_name`, `owner_user`/`owner_project`. One `filterable_table`, no tabs.

Reuse `theme.py` (`kpi_cards`, `filterable_table`, `compact_money`, `panel`) — same components the TCO page uses.

______________________________________________________________________

## Honesty invariants (carried from the FOCUS plane)

1. **Candidates vs confirmed** — Phase-0 FOCUS-only signals are labelled *candidates*; only utilization-backed views claim *confirmed* waste.
1. **Idle ≠ eliminated by auto-termination** — show the billed idle window.
1. **No vendor waste %** — never display a headline "X % of spend is waste"; only per-resource recoverable dollars computed from the customer's own data.
1. **Region-scoped telemetry** — flag clusters in regions the metrics pull didn't cover (under-coverage surfaced, not hidden), exactly like the unattributed-AWS bucket.

______________________________________________________________________

## What this is NOT (scope guard)

- Not automated remediation / rightsizing actions — measurement only.
- Not query-rewriting or code analysis — `query_stats` flags spill/slow queries; it does not fix them.
- Not real-time — same batch cadence as ingest (system tables refresh ~24 h anyway).

______________________________________________________________________

## Known limitations — Databricks compute-class coverage

Investigated 2026-07-11 (live `DESCRIBE`/`SELECT` against a production warehouse), when extending `sql_warehouse_disk_spill` — recorded here rather than silently left as a gap.

1. **No spill/shuffle signal for classic JOBS / ALL_PURPOSE Spark execution.** Confirmed via `SHOW TABLES IN system.compute` / `system.lakeflow`: no system table exposes Spark stage-level metrics for arbitrary DataFrame/RDD code. `system.compute.node_timeline` has only CPU/memory/network/disk-free columns; `system.lakeflow.job_run_timeline` has only duration/status/trigger metadata. `system.query.history` (the source of the `sql_warehouse` spill/shuffle signal) only covers statements executed via the SQL/ Spark-Connect path — `compute.type` is never `JOBS`/`ALL_PURPOSE` on that table. The two real paths outside system tables are both deliberately out of scope for now:
1. Databricks' native "Compute metrics" UI *does* show shuffle read/write for jobs/ all-purpose compute, but it's UI-only — no documented REST API or system table backs it. Automating it means reverse-engineering an undocumented internal API, which breaks this project's vendored-and-validated-query convention.
1. Spark event log replay: cluster log delivery is off by default (an org-wide cluster-policy change to turn on), logs land as raw JSON files in DBFS/S3, and the only documented access is a UI notebook replay — not SQL, not an API. A real pull would mean a new JSON-parsing ingestion pipeline, not a `databricks_efficiency.sql` extension.
1. A self-hosted `spark-metrics` → Prometheus Pushgateway stack (community pattern) would get real per-cluster shuffle/spill, but needs cluster init-script changes org-wide, new metrics infrastructure, and a new non-SQL connector architecture (pull from Prometheus's HTTP API instead of a Databricks warehouse) — a distinct initiative, not an extension of this connector.

**Partial mitigation added 2026-07-12**: `possible_memory_pressure` and `possible_heavy_shuffle` in `waste_rules.py` approximate spill/shuffle from `system.compute.node_timeline` columns already joined for CPU/mem utilization — `cpu_wait_percent` (I/O wait is spill's textbook symptom), `mem_swap_percent` (memory exhaustion), `disk_free_bytes_per_mount_point` at `/local_disk0` (spill's actual write target — not the OS root partition, which is always small regardless of spill activity), and `network_sent_bytes`/`network_received_bytes` (shuffle is inter-executor network I/O). These are proxies, not measurements — always `'candidate'` confidence, `recoverable_cost_sql="0"` (a flag for review, no $ claim), gated by `avg_run_seconds >= 300` for job entities (a materiality filter: a short job with a scary-looking proxy reading isn't worth optimizing) that fails closed on untracked runs rather than guessing. Thresholds were grounded in a 30-day percentile scan of this account's job/all-purpose node_timeline rows (see inline comments in `databricks_efficiency.sql`'s `util`/`runs` CTEs for the exact numbers) — not picked blind. This narrows the gap; it does not close it — a real spill event with low CPU wait, or shuffle that doesn't saturate the network, would still go undetected. 2. **DLT/Lakeflow declarative pipeline (serverless) compute is entirely unattributed in the efficiency/waste plane** — not just missing spill/shuffle, missing as an entity at all. Confirmed live: this usage bills under `billing_origin_product IN ('SQL', 'DATABASE')` with `usage_metadata.dlt_pipeline_id` populated but `warehouse_id`/ `cluster_id`/`job_id` all NULL, so none of the 5 `UNION ALL` branches in `databricks_efficiency.sql` pick it up (each requires one of those four IDs). The FOCUS/TCO cost pull is unaffected — `databricks_focus_1_3.sql` sums `billing.usage` unconditionally, so this spend is billed correctly; it's specifically invisible to waste classification. Closing this needs a new `EntityType` + a 6th branch keyed on `dlt_pipeline_id`, with its own `billed_cost` attribution — a new-entity-type feature, not a SQL tweak. Deferred: sampled 30 days of this billing line and found zero spill, zero shuffle — the signal doesn't currently justify the lift. Revisit if DLT pipeline spend grows or a spill/shuffle complaint actually shows up on this line.

______________________________________________________________________

## §7 — CLAUDE.md changes required at build time

When Phase 0/1 lands, update CLAUDE.md:

- Amend *"does not produce optimization recommendations in v1"* to scope the efficiency-waste view as **recoverable-spend measurement** (not remediation).
- Add the **metrics plane** to the architecture section (`lake/metrics_*`, `metrics/` on-disk root, the `efficiency` GOLD group).
- Add the four honesty invariants above to "Key invariants."

______________________________________________________________________

## Build order

1. **Phase 0** — coarse `placement` rows from FOCUS only (all-purpose/SQL SKU spend at provider/SKU grain) → `gold.waste_record` + `efficiency` group + the dashboard leaderboard. FOCUS-only, smallest diff; the metrics plane later upgrades grain + adds utilization categories.
1. **Phase 1a-i** — `metrics/` plane plumbing: `paths.metrics_dir`, `metrics_schema.py` (the `EfficiencyRecord` Arrow schema), `lake/metrics.py` writer, `register_metrics`, and the connector's warehouse SQL emitting `EfficiencyRecord` for job + interactive.
1. **Phase 1a-ii** — `050_gold_waste.sql` (`waste_record` + `waste_summary_month`) wired into the leaderboard. Photon rides along (it's in `cause_detail`).
1. **Phase 1b** — Model Serving: extend the connector's emit to `entity_type='endpoint'` (verify serving system-table names); `idle` category already classifies it — no new view, just new rows.
1. **Done** — `sql_warehouse` disk spill (`system.query.history.spilled_local_bytes` → `cause_detail.spill_query_count`/`spilled_bytes` → `sql_warehouse_disk_spill` in `waste_rules.py`, validated live 2026-07-11: 78/66,709 queries spilled, worst case ~21 TB); autoscale/oversize (`waste_rules.py`).
1. **Later** — serverless placement; BigQuery/Snowflake mappers (each = a connector emitting the same `EfficiencyRecord`, zero GOLD change).

The payoff of the single record: **after Phase 1a, new compute classes and new platforms add rows, not views.** Model Serving, Vector Search, BigQuery slots — all land in `waste_record` via the connector, and the one dashboard renders them unchanged.
