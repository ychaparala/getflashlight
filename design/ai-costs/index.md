# AI costs — tokens, who used them, and what they cost

> Status: **implemented** (Phases 1–3 below). Cost is measured from the FOCUS bill and is complete today with no re-ingest. Token/user attribution needs `system.serving`, a Databricks Public Preview schema that an account has to enable — the pull degrades in three rungs rather than failing. **Column names in `databricks_ai_usage.sql` were validated against a live warehouse on 2026-08-05** (see §7); the published docs' list does not match the live schema.

## The one decision this design turns on

**Model serving bills two entirely different ways, and only one of them makes a $/token figure honest.**

- **Pay-per-token** (Foundation Model APIs): tokens *are* the meter. Splitting an endpoint's charge by a requester's token share is a proportional split of a per-token charge.
- **Provisioned throughput / provisioned compute**: billed per provisioned *hour*. An idle provisioned endpoint bills real money with **zero tokens**. Splitting its charge by token share would hand its idle capacity's cost to whoever happened to send traffic.
- **External models**: Databricks bills the gateway hop; the model vendor bills the tokens, on a bill this lake never sees. The tokens are real; the Databricks dollars are not their cost.

**Therefore:** every row that carries a dollar figure also carries a **`cost_allocation_basis`**, and `allocated_cost` / `cost_per_million_tokens` are **NULL for three of the four bases**. NULL means *not allocatable by token*, never *$0* — do not coalesce it. Token counts are honest for every basis; only the dollars are conditional.

The precedent is `EntityType.SQL_WAREHOUSE_USER`: cost allocated by duration share, always `candidate` confidence, "never claimed as exact" (`efficiency/model.py`).

## Implementation plan (at a glance)

```
                    ┌─ system.billing.usage ──────────────┐
Databricks ─────────┤  (vendored FOCUS 1.3 query)         │→ BRONZE ─┐
                    └─────────────────────────────────────┘          │
                                                          silver.focus_normalized
                                                                     │
                    ┌─ system.serving.endpoint_usage ─────┐          │  gold.ai_spend_month
                    │  ⋈ system.serving.served_entities   │          │  (provider-scoped)
                    └─────────────────────────────────────┘          │
                              │ AiUsageRecord                        │
                              ↓                                      ↓
                       ai_usage/ (Parquet)  ──→ metrics.ai_usage ──→ ai_usage.* (4 views)
                              │                                      ↑ the cost↔token join
                              │ (same rows, re-aggregated)           │ happens ONCE, here
                              ↓
                       EfficiencyRecord(entity_type='endpoint')
                              ↓
                       metrics.efficiency_record → efficiency.waste_record
                              (idle / failed fire with NO new rule)
```

**New files (7):** `lake/ai_usage_schema.py` · `lake/ai_usage.py` · `ingest/connectors/sql/databricks_ai_usage.sql` · `transform/sql/037_gold_ai_spend.sql` · `transform/sql/080_gold_ai_usage.sql` · `dashboard/views/ai_costs.py` · `docs/design/ai-costs.md`

**Touched (14):** `lake/paths.py` · `lake/duck.py` · `ingest/base.py` · `ingest/connectors/databricks.py` · `ingest/runner.py` · `transform/runner.py` · `transform/catalog.py` · `transform/sql/055_gold_utilization.sql` · `efficiency/waste_rules.py` · `efficiency/policy_rules.py` · `efficiency/policy_config.py` · `dashboard/router.py` · `dashboard/views/efficiency_waste.py` · `scripts/build_demo_lake.py`

## Phase 1 — AI spend, from the existing lake

The dollars were already in BRONZE: the vendored FOCUS query stamps `service_category = 'AI and Machine Learning'` on eight products and routes each one's `usage_metadata.<x>_id` into `resource_id`. They were just unreachable as a *category*.

`gold.ai_spend_month` (provider-scoped) selects them by **two routes**, deliberately:

1. `service_category = 'AI and Machine Learning'` — the FOCUS-native, provider-authored fact, so a product is included the day the provider categorizes it, including another provider's (AWS Bedrock stamps the same category).
1. An explicit `service_name` list for AI products the vendored query files **elsewhere** — **AI/BI Genie** above all, which bills as warehouse-shaped usage and lands under Databases. Anyone asking "what is AI costing me?" means Genie too, and editing the vendored query to recategorize it isn't an option (it's re-pulled upstream).

> Guessing an *enum value* here is safe in a way guessing a *column name* is not: a wrong value in an `IN` list matches nothing, while a wrong column name breaks the query. So all candidate spellings for an unconfirmed product are listed and at most one exists.

`gold.ai_product_family` is a **new macro**, not more `WHEN`s on `gold.compute_family` — folding Vector Search into that macro's `'endpoint'` bucket would silently change what every existing consumer of `spend_by_sku_month`/`resource_month` means by `'endpoint'`, and a family is about *how compute is bought*, not *what product it belongs to*.

`service_subcategory` is **not usable**: the vendored query computes it but it is not in `lake/schema.py`'s BRONZE schema, so it never reaches SILVER.

The window total also surfaces as a **KPI card on the provider page** (`ai_costs.kpi_card`, passed to `provider_focus.render` as `extra_kpis`), so "how much of this bill is AI?" is answerable without opening the tab. It is a **slice of** the `net` card beside it, not an addition — same bill, same dollars — so its sub-line reads `part of Databricks net` and it keeps the default hue, deliberately the inverse of the backing-storage card next to it (other provider's bill, outside net, own hue). Unlike this tab's own KPIs, which report the latest month, the card follows the page's date range, because the row it joins is windowed. Omitted rather than `$0` when the view is unpublished or the window has no AI rows.

## Phase 2 — the token plane

### Why a third plane rather than more `EfficiencyRecord` rows

`EfficiencyRecord` has exactly one `native_quantity`/`native_unit` pair, and for an endpoint that pair must be DBUs (it's what reconciles to FOCUS). Input and output tokens are two quantities at two prices; neither can displace DBUs, and both in `cause_detail` makes them JSON strings — un-aggregatable as GOLD measures and outside `MEASURE_UNITS` entirely. Its grain is also wrong: (entity × month) versus (endpoint × model × requester × project × month). And 40M tokens is not waste — `waste_record` only holds rows a rule *fired* on.

This is the same test `DriverHealthRecord` was created by: same *pattern*, different table.

**The record carries no cost column.** The endpoint is a FOCUS `ResourceId`, so its spend is already canonical in the FOCUS plane; a second `list_prices`-derived figure on the record would be a second source of truth. The join happens once, in GOLD.

### `serving_mode`, and the one cross-check

Derived in the connector SQL from `served_entities` (order matters — a provisioned-throughput endpoint is also a `FOUNDATION_MODEL`):

```
min_provisioned_throughput > 0                        → provisioned_throughput
entity_type = 'EXTERNAL_MODEL'                        → external
entity_type = 'FOUNDATION_MODEL'                      → pay_per_token
entity_type IN ('CUSTOM_MODEL','FEATURE_SPEC')
  AND workload_size IS NOT NULL                       → provisioned_compute
else                                                  → unknown
```

GOLD then cross-checks against the FOCUS SKU shape, with **exactly one** genuine conflict: `pay_per_token` billed on an hourly `ALL_PURPOSE` SKU → `unknown`. Provisioned throughput pairs with *either* shape legitimately — normally a `MODEL_SERVING`/`INFERENCE` SKU, but the vendored query documents it running on a classic cluster and billing as `ENTERPRISE_ALL_PURPOSE_COMPUTE`. Treating that pairing as a conflict pushed every ordinary provisioned endpoint to `unknown` and hid it from the unallocated bucket this design exists to surface (caught by `test_provisioned_endpoint_cost_is_never_split_by_tokens`).

### The GOLD contract — group `ai_usage`, 4 views

| View              | Answers                                                                                                                                            |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `endpoint_month`  | cost beside tokens per endpoint, `cost_allocation_basis`, `token_coverage_status`, `unallocated_cost`, `error_rate_pct`, `cost_per_million_tokens` |
| `model_month`     | $/1M tokens per served model — the "could a cheaper model do this?" input                                                                          |
| `project_month`   | **tokens and allocatable cost per project**; `project_key` never NULL                                                                              |
| `requester_month` | **tokens and allocatable cost per user**; `requester_kind` separates service principals                                                            |

Honesty rules baked in:

- **The join is `FULL OUTER`.** An endpoint with cost but no token rows must still appear — that's either never-measured or a genuinely silent provisioned endpoint, and dropping it would make *unmeasured* indistinguishable from *efficient*. `token_coverage_status` splits them.
- **`provider_name` is in the join key**, not a `WHERE` clause, so one provider's tokens can structurally never pick up another's dollars. This is a Databricks-internal join, **not** the cross-provider cost join CLAUDE.md forbids — that one summed Databricks DBUs with the AWS infra underneath them.
- **`project_key`/`requester_key` are never NULL** (`'(unattributed)'`) — the `waste_by_owner_month.owner_key` invariant. On real data that row is the largest bucket and *is* the finding.
- **`unallocated_cost` is the named complement of `allocated_cost`** and never overlaps it, so the two must never be summed into a "total AI cost by project".
- Request-level `usage_context` wins over the endpoint tag (it's the finer fact); `project_source` says which answered, so 26%-from-tags is distinguishable from 26%-from-instrumentation.

### Degradation — three rungs, not all-or-nothing

| Probe result | Behaviour                                                                                                                                                                                                                                       |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `full`       | both tables — tokens, model identity, serving mode                                                                                                                                                                                              |
| `usage_only` | `endpoint_usage` only — the `served_entities` CTE is replaced by a typed empty one, so the LEFT JOIN matches nothing, `serving_mode` falls through to `'unknown'`, and GOLD withholds every $/token claim automatically. **Tokens still land.** |
| `none`       | pull skipped with a warning naming what to enable. Cost ingest untouched; the tab keeps its cost panels and each token panel becomes a *named* empty panel.                                                                                     |

A probe failure counts as absent, never fatal: the probe shares the auth path with the cost query, so a broken connection should be reported by the cost pull, not by this one.

## Phase 3 — optimization

**Two of the three headline findings needed no new rule.** `idle` (`activity_count = 0 AND billed_cost > 0`) and `failed` (`coalesce(failed_cost, 0) > 0`) are entity-type-agnostic, so an endpoint `EfficiencyRecord` fires them as-is and inherits `waste_by_owner_month`, `waste_resolution_month` and the Efficiency & Waste tab for free. That *is* the CLAUDE.md invariant "new platforms add **rows**, not views", realized literally.

| Finding                                           | Where                                 | Confidence  | Priced                                                                         |
| ------------------------------------------------- | ------------------------------------- | ----------- | ------------------------------------------------------------------------------ |
| Idle provisioned endpoint (measured-zero traffic) | existing `idle`                       | high        | ✓ full `billed_cost`                                                           |
| Failed-request token spend                        | existing `failed`                     | high        | ✓ where tokens are the meter; **key absent** otherwise, so the rule can't fire |
| Scale-to-zero off on low traffic                  | new `endpoint_scale_to_zero_disabled` | candidate   | ✗ unpriced                                                                     |
| GPU class on low traffic                          | new `endpoint_oversized_workload`     | candidate   | ✗ unpriced                                                                     |
| Provisioned-vs-pay-per-token crossover            | **not shipped**                       | —           | —                                                                              |
| Model $/1M-token spread                           | AI tab panel only, never a rule       | descriptive | ✓ measured rate                                                                |

Why the two new rules are **unpriced** (`recoverable_cost_sql="0"`): the recoverable amount is the idle fraction of provisioned wall-clock, and nothing we pull carries it — `endpoint_usage` has request timestamps, not uptime. And no `workload_size → SKU` mapping exists in any system table we read, so there is no real rate to re-price a step-down against. Real-but-unpriced beats fabricated — the `snappy_to_zstd_compression` precedent. Contrast `placement`, which *is* priced precisely because `jobs_priced_cost` re-prices at a real rate.

The **crossover** rule is deliberately not shipped at all: the only honest priced version needs the pay-per-token SKU↔model rate, and it is unverified that it resolves in `list_prices`. The tab shows the *measured* $/1M tokens and says to compare it against Databricks' published rate yourself. See §7's probe 6 to decide whether it can be built.

The **model spread** is descriptive on purpose: a cheaper model is not necessarily a substitutable one, and Flashlight has no basis for judging capability. Same stance as Client Driver Health — humans read the leaderboard and judge.

`'endpoint'` is added to `055_gold_utilization.sql`'s `not_applicable` list: an endpoint has no CPU% at all, so leaving it in the `ELSE` would report every endpoint as "could be measured but no telemetry arrived" and inflate the coverage caption's unmeasured count with rows that can never carry a reading.

### The tagging policy

`endpoint_tagging` mirrors `cluster_tagging` exactly (`not_applicable` when `tag_count IS NULL`, `non_compliant` at 0), so the Policy Compliance tab reports a real denominator — "N% of serving endpoints are tagged". That is the lever that makes per-project token attribution work at all, which is why the AI tab's Untagged AI Spend KPI flags the gap and Policy Compliance runs the check itself.

One honest difference: cluster/warehouse `tag_count` reads `size(tags)` off the resource's own config row in `system.compute.clusters`/`.warehouses`. Serving has no counterpart here, so an endpoint's count comes from `size(custom_tags)` on its billing rows — the billing-propagated tag set, which can include account-level default tags. A "tagged" endpoint is therefore not proof of a *useful* tag; it is still the honest answer to "has this been tagged at all".

## What this is NOT (scope guard)

- Not a remediation surface — no endpoint is stopped, resized or re-routed.
- Not a model recommender. It ranks unit economics; it does not claim model B can replace A.
- Not a cross-provider AI total. `provider_name` stays the top-level split.
- Not a second waste surface. `efficiency.waste_record` remains the one verdict surface; the AI tab's last panel summarizes its endpoint slice and links out.

## §7 — Verification against a live warehouse

Validated 2026-08-05 against a live production warehouse. Live schema deltas from the published docs (baked into `databricks_ai_usage.sql`):

- `endpoint_usage` has **no** `endpoint_name` or `execution_duration_ms` — join via `served_entity_id`; `total_duration_ms` is always 0.
- `served_entities` nests config in structs (`foundation_model_config.min_provisioned_throughput`, `custom_model_config.compute_type`, …). Top-level `workload_size` / `scale_to_zero_enabled` are **absent** — scale-to-zero rules stay unmeasured; `workload_type` is the compute_type.

Re-check on another account with:

```
-- 1. Does the preview exist at all?
SHOW SCHEMAS IN system;                       -- expect a `serving` schema
SHOW TABLES IN system.serving;                -- expect endpoint_usage, served_entities

-- 2. Exact column names — the biggest risk if Databricks changes the preview.
DESCRIBE TABLE system.serving.endpoint_usage;
--   need: request_time, served_entity_id, input_token_count, output_token_count,
--         status_code, requester, usage_context (MAP)
--   NOT present live: endpoint_name, execution_duration_ms
DESCRIBE TABLE system.serving.served_entities;
--   need: served_entity_id, endpoint_id, endpoint_name, entity_type, entity_name,
--         entity_version, foundation_model_config, custom_model_config,
--         feature_spec_config, external_model_config, change_time
--   NOT present live: workload_size, workload_type, scale_to_zero_enabled,
--         min/max_provisioned_throughput (those live inside the structs)

-- 3. Is it populated, and is usage_context ever set?
SELECT count(*) AS rows, count(input_token_count) AS with_tokens,
       count(DISTINCT requester) AS requesters,
       count(*) FILTER (WHERE size(usage_context) > 0) AS with_usage_context
FROM system.serving.endpoint_usage WHERE request_time >= current_date - INTERVAL 30 DAYS;
-- If with_usage_context is 0, that is the expected answer and the endpoint tag is doing all
-- the project attribution. Record the number here.
SELECT DISTINCT map_keys(usage_context) FROM system.serving.endpoint_usage
WHERE size(usage_context) > 0 LIMIT 20;       -- confirm 'project' is the right key

-- 4. Which serving shapes does this account actually run?
SELECT entity_type,
       count(foundation_model_config) AS with_fm,
       count(custom_model_config) AS with_cm,
       count(external_model_config) AS with_ext,
       count(feature_spec_config) AS with_fs,
       count(*) AS n
FROM (
  SELECT * FROM system.serving.served_entities
  QUALIFY ROW_NUMBER() OVER (PARTITION BY served_entity_id ORDER BY change_time DESC) = 1
) GROUP BY 1 ORDER BY n DESC;

-- 5. Genie's real billing_origin_product — the Phase 1 extra-product list guesses spellings.
SELECT DISTINCT billing_origin_product FROM system.billing.usage ORDER BY 1;
-- Add the real value to gold.ai_product_family AND the WHERE list in 037_gold_ai_spend.sql
-- if it differs from AI_BI_GENIE / GENIE.

-- 6. The BLOCKED crossover rule's gate: is there a per-token SKU rate at all?
SELECT DISTINCT sku_name, usage_unit FROM system.billing.usage
WHERE billing_origin_product = 'MODEL_SERVING';
```

Then, after `flashlight ingest`:

- **Reconcile against the bill.** `sum(ai_usage.endpoint_month.net_cost)` must equal `databricks.ai_spend_month` net cost for `ai_product_family = 'model_serving'` that month. (Pinned locally by `test_endpoint_net_cost_reconciles_to_the_endpoint_shaped_focus_bill`.)
- **Reconcile the two dollar sources.** `ai_usage.endpoint_month.net_cost` (FOCUS-derived) vs `efficiency.waste_record.billed_cost` for `entity_type='endpoint'` (`list_prices`-derived, via the query's `endpoint_cost` CTE). They come from different joins by design; a material gap means that CTE's product filter is wrong.
- **Spot-check the honesty branches.** One provisioned endpoint: `allocated_cost IS NULL`, `unallocated_cost = net_cost`, `cost_per_million_tokens IS NULL`. One pay-per-token endpoint: the rate is plausible against Databricks' published price.
- **Prove degradation.** Point the connector at an account without `system.serving` and confirm the cost ingest completes, the other five efficiency branches still emit, the `ai_usage` views publish as 0-row files, and the tab renders cost plus named empty token panels — not an exception and not a silent blank.

### Locally

```
uv run ruff check src tests scripts && uv run mypy src tests scripts && uv run pytest
uv run python scripts/build_demo_lake.py --out demo/lake   # regenerates + self-verifies
uv run flashlight transform                                 # Phase 1 needs no re-pull
uv run flashlight dashboard serve                           # /databricks → AI Costs
```

`scripts/build_demo_lake.py` covers **both** cost-allocation bases, a service-principal requester, a Genie row with a non-AI `service_category`, and all three endpoint findings — and asserts each, so a future change that empties the tab fails there rather than in a browser.
