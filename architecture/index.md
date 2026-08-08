# Architecture

```
sources ──▶ flashlight ingest ──▶ Parquet lake (FLASHLIGHT_HOME) ──▶ readers
 AWS FOCUS export    (writer)     bronze/  partitioned, source of truth   flashlight mcp serve
 Databricks tables                gold/    *.parquet ◀── the only surface  flashlight dashboard serve
 AWS Cost Explorer                         consumers read                 (each: own in-mem DuckDB)
```

Three independent processes: `ingest` is the sole writer; `mcp serve` and `dashboard serve` are read-only. Concurrency is "many readers over immutable Parquet, publish by atomic per-file rename" — no locks, no server. There is no REST API, no database, and no migrations: Parquet is self-describing and the `FocusRecord` Pydantic model is the schema.

The dashboard can *launch* the other two rather than doing their work: its Connections page shells out to `flashlight ingest` and its MCP server page starts/stops `flashlight mcp serve`. Both go through the same CLI entrypoint a terminal user would run, so there's one implementation of each — the dashboard is a control surface, never a second writer and never a second server.

User configuration lives in `<home>/config/`: `connections.yml` (sources), `policies.yml` (policy thresholds), `assistant.yml` (the BYOK model choice). Env vars override each file; secrets are in the OS keychain (or the env vars the config names), never in these files.

## The medallion

- **BRONZE** `bronze/` — canonical FOCUS records, Hive-partitioned by connector + charge month; partition-replace makes re-ingest idempotent and self-purging.
- **SILVER** (in-memory only) — the cleaned, normalized view every GOLD metric is derived from: one canonical `cost` column (EffectiveCost), charge-period grain.
- **GOLD** `gold/<group>/*.parquet` — the metrics contract the dashboard and MCP both read, so a chart and an agent never disagree. Built by `transform` via DuckDB `COPY`, split per provider (`aws.monthly_bill`, `databricks.monthly_bill`) plus the fixed cross-provider groups: `efficiency` (waste), `driver_health` (Databricks client-driver fleet health — a compliance signal, no cost metric), `policy` (governance guardrails, also no cost metric), `ai_usage` (AI serving tokens), and `storage` (the cloud storage bill behind a data platform — AWS S3 cost labelled by Unity Catalog's bucket map; cross-provider by construction, since each row names both the biller and the platform. It is **never** added to Databricks spend — see [Backing storage](https://getflashlight.app/design/backing-storage/index.md)).

### Why the billing period stops at BRONZE

FOCUS defines two time grains: the **charge period** (when usage happened) and the **billing period** (which invoice it lands on). `BillingPeriodStart`/`End` are ingested and stored on every BRONZE row — `FocusRecord` is the schema of record, so nothing is thrown away — but they are deliberately **not** projected into `silver.focus_normalized`, and no GOLD view or dashboard control exposes them.

Three reasons, in order of weight:

1. **Only the charge period is additive.** Aggregating on the billing period double-counts usage that was re-invoiced and hides usage not yet invoiced. Keeping the columns out of SILVER means no downstream view *can* accidentally group by them — the invariant is enforced by the schema, not by everyone remembering it.
1. **They carry no information this data doesn't already have.** Measured across both lakes: `billing_period_start` equals `date_trunc('month', charge_period_start)` and `billing_period_end` equals the following month for **940,790 of 940,791** rows of a real AWS + Databricks lake and **1,023 of 1,024** rows of the demo lake, across all three mapping paths (`connectors/_focus_map.py`, `connectors/aws_focus.py`, `focus/sql_mapping.py`). The sole exception is one synthetic Oracle row in the FinOps FOCUS sample. A billing-period dimension would be a relabelled `charge_month`.
1. **A second month key is a trap.** Two plausible month columns in SILVER invites a future view to pick the wrong one, and the failure is silent — the numbers still add up, they're just answering a different question.

If a future connector reports a genuinely different billing period (a non-calendar billing cycle, or invoicing that lags the charge month), this stops being true. That's why the claim is a test, not a comment: `tests/test_billing_period_invariant.py` fails the moment the committed demo lake violates it. Invoice-level reconciliation is served instead by `invoice_reconciliation_month`, which groups by the real `InvoiceId`.

## The efficiency plane

Utilization telemetry doesn't fit `FocusRecord`, so efficiency is a second, parallel medallion: connectors emit `EfficiencyRecord` rows (best-effort — a failed pull never blocks the cost ingest) into the `metrics/` Parquet root, and the GOLD waste view classifies them into waste categories with recoverable cost. Details: [Efficiency / waste](https://getflashlight.app/design/efficiency-waste/index.md).

## Package layout

```
src/flashlight/
  focus/      canonical FOCUS model + enums
  ingest/     connectors (aws_focus, databricks, redshift) + runner
  lake/       the Parquet layer: paths, schema, bronze writes, DuckDB, publish
  transform/  SILVER/GOLD SQL + runner (builds gold/*.parquet) + metric catalog
  gold/       reader.py — the shared GOLD read surface (MCP + dashboard)
  mcp/        MCP server over the GOLD views (the agent consumer surface)
  dashboard/  NiceGUI app over the GOLD views (the human consumer surface)
  cli.py      the unified `flashlight` command
```
