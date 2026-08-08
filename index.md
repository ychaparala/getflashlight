# Flashlight

*Finds what's burning money in the dark.*

FOCUS-based, multi-cloud cloud-spend visualization.

Flashlight ingests cloud billing in the [FinOps FOCUS](https://focus.finops.org/focus-specification/) format, standardizes it into a layered data model, and serves a bundled NiceGUI dashboard plus an MCP server for agents. It answers what you're actually spending — across every cloud and data platform on one FOCUS-normalized bill, plus how much of it is recoverable waste.

## Choose a path

- :material-rocket-launch: **Try it in five minutes**

______________________________________________________________________

Install Flashlight, load the FOCUS sample, and open the dashboard.

[Start the quickstart](https://getflashlight.app/quickstart/index.md)

- :material-cloud-download: **Connect production billing**

______________________________________________________________________

Configure AWS, Databricks, or Redshift with least-privilege credentials.

[Connect a source](https://getflashlight.app/getting-started/real-source/index.md)

- :material-robot-outline: **Use an agent**

______________________________________________________________________

Give an MCP-compatible agent read-only access to the same metrics as the dashboard.

[Set up MCP](https://getflashlight.app/guides/mcp/index.md)

- :material-book-open-page-variant: **Understand the numbers**

______________________________________________________________________

Learn the FOCUS model, the lake layers, and the integrity rules behind every total.

[Read the concepts](https://getflashlight.app/concepts/focus-and-integrity/index.md)

## Why use Flashlight?

- **One binary, no infra.** `pip install getflashlight`. No Docker, no database server. State is Parquet under `FLASHLIGHT_HOME`, queried by a throwaway in-memory DuckDB in each process.
- **FOCUS-native.** Every connector maps its source into one canonical `FocusRecord`. One cost metric (`EffectiveCost`) is summed everywhere; nothing gets to invent its own column.
- **Recoverable spend, not just spend.** A parallel efficiency/waste plane measures the billed-but-not-used gap — idle, underutilized, wrong compute placement — as dollars, never as an auto-remediation action.
- **Humans and agents read the same numbers.** The dashboard and the MCP server are both read-only consumers of the same published GOLD Parquet, so a chart and an agent can't disagree.
- **Multi-cloud by construction.** A new provider is a connector that maps into the existing `FocusRecord`/`EfficiencyRecord` contracts — not a new dashboard or a new schema.

## Install

```
pip install getflashlight
flashlight sample               # download the FinOps FOCUS sample + seed it
flashlight dashboard serve      # dashboard → http://127.0.0.1:8501
```

Connecting real billing — AWS, Databricks, Redshift — is a `connections.yml` away; see [Quick start](https://getflashlight.app/quickstart/index.md) and [Connectors](https://getflashlight.app/connectors/overview/index.md).

## Data integrity

The SILVER/GOLD layer enforces the rules that make FOCUS data safe to sum:

- One cost metric per view (`EffectiveCost`)
- Charge-period grain only, partial current period flagged
- Credit/refund signs preserved
- Single currency asserted at ingest
- AWS spend that can't be attributed to a cluster is shown as an explicit unattributed bucket, never hidden

## Documentation

- [Get started](https://getflashlight.app/getting-started/overview/index.md) — installation, a short trial, and production setup
- [User guide](https://getflashlight.app/guides/configure/index.md) — configure, ingest, explore, deploy, and use agents
- [Concepts](https://getflashlight.app/concepts/focus-and-integrity/index.md) — FOCUS, data contracts, integrity, and metric definitions
- [Connectors](https://getflashlight.app/connectors/overview/index.md) — supported sources, prerequisites, and mapping details
- [Reference](https://getflashlight.app/reference/cli/index.md) — commands, configuration, data contracts, and MCP tools
- [Troubleshooting](https://getflashlight.app/troubleshooting/index.md) — solve setup, credentials, data, and serving issues
- [llms.txt](https://getflashlight.app/llms.txt) — an index of these docs for LLM tooling; the MCP server (`flashlight mcp serve`) reads the same GOLD views the dashboard does
