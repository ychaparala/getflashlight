# Source connectors & FOCUS mappings

| Connector                | Source                                                                                         | How it maps to FOCUS                                                                                              | Efficiency signal?                                    |
| ------------------------ | ---------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| `aws_focus`              | AWS Data Exports (FOCUS 1.2 Parquet in S3), or Cost Explorer via `cost_source="cost_explorer"` | S3 export: already FOCUS, light coercion. Cost Explorer: coarser account-level `SERVICE` totals, mapped in Python | S3 Intelligent-Tiering candidates                     |
| `databricks`             | Databricks system tables                                                                       | **vendored Databricks → FOCUS 1.3 SQL** (below)                                                                   | jobs/clusters/warehouses/tables + driver fleet health |
| `redshift`               | Redshift Data API + Cost Explorer (efficiency/waste only)                                      | no cost mapping — `fetch()` is a deliberate no-op; see below                                                      | query patterns, WLM, spill, tables                    |
| `bigquery` / `snowflake` | —                                                                                              | stubs (planned)                                                                                                   | —                                                     |

Connectors are configured in `connections.yml` (scaffolded by `flashlight init`); credentials come from `.env` via the `*_env` fields. Each connection also takes an optional `name` — falls back to its `type` for the common single-connection case, but needed (and enforced unique) once you have more than one connection of the same type, e.g. several Redshift clusters.

## AWS FOCUS setup

This is the primary AWS path — a native **AWS Data Export** in FOCUS 1.2 format, delivered straight to your own S3 bucket. Set `AwsFocusConfig.cost_source: cost_explorer` instead to skip the export entirely and query Cost Explorer directly — coarser (account-level `SERVICE` totals, no per-charge detail, no cost-subcategory classification) and needs `ce:GetCostAndUsage`, but no export to provision. Pick one explicitly; there's no automatic fallback between them.

**1. Create the export.**

```
flashlight aws create-export --bucket my-focus-bucket --prefix billing/focus
```

Walks you through bucket/prefix/region with resolved defaults, then provisions a `flashlight-focus` Data Export at FOCUS 1.2, `DAILY` granularity, `OVERWRITE_REPORT` delivery (each refresh replaces that billing period's files in place, rather than accumulating per-execution copies). `--dry-run` prints the request without creating anything.

**2. Grant the bucket policy AWS Data Exports needs to write there.**

```
flashlight aws bucket-policy --bucket my-focus-bucket --dry-run   # inspect first
flashlight aws bucket-policy --bucket my-focus-bucket
```

This is a separate, required step — creating the export does not attach the policy that lets the Data Exports service write to your bucket.

**3. Wait for the first delivery, then confirm it landed.**

```
flashlight aws describe-export --bucket my-focus-bucket --prefix billing/focus
```

AWS backfills history and then delivers on the configured cadence; first delivery can take a while. `describe-export` reports which billing periods, how many files, and how much data has actually arrived — check this before wiring up `flashlight ingest` so a config mistake doesn't look like an AWS delay.

**4. Point Flashlight at it**, in `connections.yml`:

```
connectors:
  - type: aws_focus
    name: Prod cost
    s3_bucket: my-focus-bucket
    s3_prefix: billing/focus
    region: us-east-1
    include_services: []   # [] = whole account; default is Redshift + S3
```

`include_services` is an allow-list of FOCUS `ServiceName` values — AWS Data Exports is account-wide and can't be scoped per service at the source, so Flashlight narrows here. Set `[]` to ingest the whole account, or list the specific services you want.

It defaults to Redshift's own service names **plus `Amazon Simple Storage Service`**. S3 is in the default because Databricks' own bill covers DBU compute only — the storage behind a Unity Catalog external location is billed by AWS, so without S3 here the Databricks → **Backing storage** tab has nothing to show (see [Backing storage](https://getflashlight.app/design/backing-storage/index.md)). Narrowing it back to just Redshift's names opts out, and the AWS group's totals shrink accordingly.

**5. Ingest.**

```
flashlight ingest
```

**How the read works.** `aws_focus` doesn't run queries against an AWS API — it's a manifest-driven, DuckDB-pushdown Parquet scan straight over S3. Each billing period's `metadata/BILLING_PERIOD=YYYY-MM/…Manifest.json` names the *current* version of that period's data files, so the connector reads exactly those files (never a `*.parquet` glob) — that's what stops a re-run from double-counting a period under `CREATE_NEW_REPORT` delivery, where AWS also leaves the previous execution's copy sitting in the same folder. DuckDB then scans the manifest-listed Parquet directly from S3 with the `include_services` allow-list and the ingest window pushed down as a `WHERE` predicate — column pruning and predicate pushdown mean a narrow, service-scoped pull reads a fraction of the account's bytes, not the whole export.

**Efficiency signal: S3 Intelligent-Tiering candidates.** `aws_focus` also implements `fetch_efficiency()` — a local read of its own just-written BRONZE rows (no second S3/API call), grouped by `(bucket, month)`. A bucket is flagged as an intelligent-tiering candidate unless any of its line items already mention Intelligent-Tiering in `ChargeDescription`/`SkuId`. This is a text-match heuristic against the billing line, not a real storage-class field read from S3 — `candidate` confidence, not `high`.

## Cost Explorer fallback (no export needed)

```
connectors:
  - type: aws_focus
    name: Prod cost
    cost_source: cost_explorer
    region: us-east-1
    include_services: []   # [] = whole account; default is Redshift + S3
```

Groups Cost Explorer's `get_cost_and_usage` by the `SERVICE` dimension and day, filtered by `include_services` — no S3 export, but only account-level totals per service, no per-charge detail, and no cost-subcategory classification (the Redshift $ breakdown other views rely on needs the FOCUS export's `ChargeDescription`/`SkuId`). Needs `ce:GetCostAndUsage` in addition to whatever IAM the S3 path would have needed.

This path also **cannot feed the backing-storage view**: Cost Explorer returns account-level service totals with no `ResourceId`, so no S3 charge can be attributed to a bucket. Those rows land as `mapping='no_resource_id'`.

There's no per-resource/tag scoping here (an earlier `aws_infra` connector did this for Databricks classic-compute AWS-infra attribution via a cluster tag; it was folded into this Cost Explorer path and that attribution capability was not carried over).

## Databricks mapping

The `databricks` connector does **not** hand-roll the billing math. It runs the authoritative **Databricks System Tables → FOCUS 1.3** query, vendored verbatim at [`src/flashlight/ingest/connectors/sql/databricks_focus_1_3.sql`](https://github.com/ychaparala/flashlight/blob/main/src/flashlight/ingest/connectors/sql/databricks_focus_1_3.sql) from the Databricks solution accelerator [`databricks-solutions/cloud-infra-costs`](https://github.com/databricks-solutions/cloud-infra-costs/blob/main/focus/focus_query.sql). The connector executes it on a SQL warehouse, then feeds the FOCUS-columned output through the same shared mapper used by the file/S3 connectors. The only field we add is `x_compute_class` (classic vs serverless), derived from the SKU — FOCUS doesn't carry it, and it's how you tell all-in serverless billing from classic compute that also shows up as separate cloud infra lines.

**This SQL is repurposable** — that's a feature, not a one-off:

- **Run it standalone.** Paste it into Databricks SQL / a notebook (set the `:account_prices` parameter) to materialize a FOCUS table, export it to Parquet/Delta, and ingest via `aws_focus`'s S3 FOCUS export path — no live API needed.
- **Template for other warehouses.** It's the reference pattern for *source-side* FOCUS mapping; the planned `snowflake`/`bigquery` connectors follow the same shape (run a warehouse-native FOCUS query, then map the rows).
- **Fork & extend.** The upstream mapping is explicitly "best-effort"; edit the vendored copy to add columns or refine the `billing_origin_product` taxonomy. To refresh it, re-pull the upstream file and re-apply the header.

Note

FOCUS™ is a trademark of the FinOps Foundation; the FOCUS spec is licensed CC-BY 4.0. The vendored query retains its source attribution in its header.

## Databricks efficiency, table inventory & driver-health telemetry

Same connector, same warehouse connection, three more pulls beyond the FOCUS cost mapping above — all via `fetch_efficiency()`/`fetch_driver_health()`, run after every connector's cost `fetch()` completes. Each is its own vendored SQL file under `ingest/connectors/sql/`, aggregated **at source** (one row per entity × month, not one row per query/run).

| Signal                                                                                                                    | Grain                                                                               | Source                                                                                                                                   |
| ------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Job run health — run/failure counts, node utilization overlapped with run windows                                         | `job_id` × month                                                                    | `databricks_efficiency.sql`: `system.lakeflow.job_run_timeline`, `system.lakeflow.jobs`, `system.compute.node_timeline`                  |
| All-purpose cluster utilization — CPU/mem avg + peak, autoscale sizing, owner                                             | `cluster_id` × month                                                                | `system.compute.node_timeline`, `system.compute.clusters`, `system.compute.node_types`                                                   |
| SQL warehouse disk spill / shuffle                                                                                        | `warehouse_id` × month                                                              | `system.query.history` (`spilled_local_bytes`, `shuffle_read_bytes`)                                                                     |
| Billed-cost attribution for every row above                                                                               | entity × month                                                                      | `system.billing.usage` ⋈ `system.billing.account_prices` — the same rate resolution as the FOCUS pull, so `billed_cost` reconciles to it |
| Delta table inventory — size, file count, compression codec                                                               | top 20 tables by size, snapshotted "as of today" (Delta has no historical-size API) | `system.information_schema.tables` (candidates, top 200 by `last_altered`) then a concurrent `DESCRIBE DETAIL` per table                 |
| Client-driver fleet health — which driver/app/user is hitting the warehouse, and how often (a compliance signal, no cost) | `client_driver` × `client_application` × `executed_by` × month                      | `databricks_driver_health.sql`: `system.query.history` (`client_driver`, `client_application`, `executed_by`)                            |

`billing_origin_product` (`JOBS` / `ALL_PURPOSE` / `SQL` / `MODEL_SERVING`) is what routes a row to the right branch — the same field the FOCUS mapping already reads, so a workload's cost and its efficiency signal always agree on what kind of compute it is.

**Known gaps** (investigated, not silently missing): classic JOBS/ALL_PURPOSE Spark execution has no system-table spill/shuffle signal at all (mitigated by CPU-wait/memory-swap/disk-headroom proxy signals, always `candidate` confidence); DLT/Lakeflow serverless pipeline compute is entirely unattributed in this plane, though its cost is still captured correctly by the FOCUS pull. Full detection table and the reasoning behind both gaps: [Efficiency / waste](https://getflashlight.app/design/efficiency-waste/index.md).

## Redshift efficiency/waste telemetry

Unlike every other connector, `redshift` does not map cost — Redshift's own SKUs already arrive via `aws_focus` (AWS Data Exports FOCUS carries `ServiceName="Amazon Redshift"`/`"Amazon Redshift Spectrum"`), so `RedshiftConnector.fetch()` is a deliberate no-op. All of its telemetry work is `fetch_efficiency()`, via the Redshift Data API (IAM-based; falls back to a direct SQL connection over an SSH bastion tunnel for a locked-down VPC cluster — see `RedshiftConfig.bastion`). Its $ breakdown is read from `aws_focus`'s already-ingested BRONZE rows for the same window (`x_cost_subcategory`, stamped by `aws_focus._classify_redshift_cost_category`) rather than a second Cost Explorer call — `ingest/runner.py` runs every connector's cost `fetch()` to completion before any connector's `fetch_efficiency()` runs, so those rows are guaranteed on disk by the time this reads them, **if** `aws_focus` is enabled and has ingested this window. **`aws_focus` is recommended but not required**: without it, the cluster's $ figures are just absent/zero — every other signal (query patterns, per-user activity, WLM health, table inventory/usage) still runs fully, since none of it depends on cost data.

Auth: `access_key_env`/`secret_key_env` name environment variables to read static AWS keys from (same pattern as every AWS connector); leave both unset to fall through to boto3's own default chain (IAM role, `~/.aws/credentials` default profile). Set `aws_profile` to authenticate as a specific named profile (including AWS SSO profiles) instead — takes priority over the env-var fields when set. This governs the AWS API calls (Data API, `describe_clusters`, `describe_reserved_nodes`) — it's separate from the SQL-query auth below.

**SQL-query auth is two independent choices**: which connection path reaches the cluster, and how that connection authenticates.

Connection path, checked in this order:

1. **`bastion_host` set** — a direct SQL connection over an SSH tunnel, for a cluster locked down in a private VPC that even a direct connection can't reach without a jump host. All `bastion_*` fields (`bastion_host`, `bastion_port`, `bastion_user`, `bastion_private_key_path`, `bastion_private_key_passphrase_env`) describe the SSH jump host itself, not the Redshift cluster — flattened directly onto `RedshiftConfig` (no nested sub-object) specifically so they can never collide by name with the Redshift-side `db_*` fields.
1. **`bastion_host` unset** — a direct SQL connection straight to the cluster's real endpoint, no tunnel. For a cluster that's reachable directly (public, or same network).

Either path needs `db_user` and `cluster_identifier` (provisioned clusters only). If neither is reachable over SQL at all, leave both unset — see mode 3 below.

Authentication for that connection: by default a short-lived IAM-authenticated credential (`redshift:GetClusterCredentials` for `db_user`, no password anywhere). Set **`db_password_env`** to connect with a plain native DB user/password instead — e.g. before `GetClusterCredentials` IAM access is set up, or against a local/dev cluster. Same env var, whether the connection above is tunneled or direct.

1. **Neither `bastion_host` nor a SQL connection configured (the default)** — the Data API: IAM (`db_user`) or Secrets Manager (`secret_arn`); no persistent connection at all. `db_password_env` has no effect here.

Whichever of modes 1/2 is active, one connection is opened for the whole `fetch_efficiency()` pull and reused across all 5 queries — not reconnected per query. As with every other credential in this file, only the *name* of the environment variable goes in config, never the password itself.

**Redshift cluster endpoint**: modes 1 and 2 both need the cluster's real host/port — auto-discovered via `describe_clusters` by default. Set `db_host`/`db_port` to skip that AWS API call entirely (e.g. you don't want to grant even a read-only Describe\* permission). `_reserved_node_coverage()`'s own node-count lookup still calls `describe_clusters` regardless — that's a separate, unrelated signal.

| Signal                                                                                                                                                            | Grain                                                                                                                                                                | Source                                                                                                                          |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Cluster $ breakdown (compute/concurrency-scaling/storage/Spectrum), reserved-node coverage, WLM queue wait (p95/p99, wait-to-exec ratio), overall disk-spill rate | one row per cluster/workgroup × month                                                                                                                                | `aws_focus` BRONZE rows + `describe_reserved_nodes` + `redshift_efficiency.sql` (STL_WLM_QUERY, SVCS_CONCURRENCY_SCALING_USAGE) |
| Query-pattern runtime/spill/skew — which repeated query shape (hashed, not stored verbatim) is slow, spilling, or skewed                                          | `query_pattern` × month, top-200 by runtime                                                                                                                          | `redshift_query_pattern_metrics.sql` (STL_QUERY, STL_WLM_QUERY, SVL_QUERY_REPORT)                                               |
| Per-user CPU/scan/spill pressure and cost-concentration share                                                                                                     | `sql_warehouse_user` × month, top-50 by exec time                                                                                                                    | `redshift_user_activity.sql` (SVL_QUERY_METRICS_SUMMARY, SVL_QUERY_REPORT)                                                      |
| Table inventory (size/encoding/maintenance-staleness) + usage (query count, last access, days since last access)                                                  | `table` × month, generous cap (5000 — a pathological-catalog safety valve, not a curation cut, since the waste rules and dashboard pagination do the real narrowing) | SVV_TABLE_INFO + STL_SCAN                                                                                                       |

**Retention caveat:** STL\_*/SVL\_* system tables typically retain only ~2–5 days of history unless the cluster has STL log export to S3/CloudWatch configured — so the query-pattern, per-user, and table-usage signals degrade if `flashlight ingest` doesn't run at least every 1–2 days. This is a property of Redshift's system tables, not something the connector can fix.

**Not yet validated against a live cluster** — column names follow AWS's published system-table docs; each vendored SQL file carries its own "spot-check `cause_detail` before trusting this" note, same discipline as `databricks_efficiency.sql`.
