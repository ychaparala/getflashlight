# Quick start

## 1. Install

```
pip install getflashlight        # or: uv sync (from the repo)
```

**Cross-platform.** The lake home defaults to the OS user-data dir (`platformdirs`) — `~/Library/Application Support/flashlight` on macOS, `%LOCALAPPDATA%\flashlight\flashlight` on Windows, `~/.local/share/flashlight` on Linux — or set `FLASHLIGHT_HOME` to override. Secrets load from a `.env` in the working directory (real shell env wins).

## 2. Try it with sample data (no configuration)

```
flashlight sample               # download the FinOps FOCUS sample + seed it
flashlight dashboard serve      # dashboard → http://127.0.0.1:8501
```

`flashlight sample [--rows 1000|10000]` loads the FinOps FOCUS sample CSV straight into Parquet via a vectorized DuckDB projection — the zero-config way to see the dashboard with real data.

## 3. Connect your own sources

```
flashlight init                 # scaffold the lake home + config/*.yml
# edit <FLASHLIGHT_HOME>/config/connections.yml; put credentials in .env
flashlight ingest               # pull configured connectors → BRONZE, rebuild GOLD
```

For connector-specific prerequisites and least-privilege guidance, see [Connect a real source](https://getflashlight.app/getting-started/real-source/index.md).

## 4. Serve the results

| Surface             | Command                      | Where                                   |
| ------------------- | ---------------------------- | --------------------------------------- |
| Dashboard (humans)  | `flashlight dashboard serve` | http://127.0.0.1:8501                   |
| MCP server (agents) | `flashlight mcp serve`       | http://localhost:8002 (streamable-http) |

Both are independent read-only processes over the published GOLD Parquet — `ingest` is the only writer.

You can also start, stop and watch the MCP server from the dashboard's **MCP server** page, which shows its status, the endpoint to paste into a client, and the tools it exposes. A server started there is a child of the dashboard, so it exits with it — use the CLI (or a service manager) for one that outlives it. Either way the port has **no authentication** and serves ad-hoc read-only SQL over your lake, so keep `FLASHLIGHT_MCP_HOST` on `127.0.0.1` unless you mean to expose it.

## Next steps

Your config lives in `<home>/config/`, all of it optional and all scaffolded with comments by `flashlight init`:

| File              | What it holds                                                   |
| ----------------- | --------------------------------------------------------------- |
| `connections.yml` | the billing sources to ingest                                   |
| `policies.yml`    | cost-policy threshold overrides                                 |
| `assistant.yml`   | which LLM the BYOK assistant uses (provider / model / base URL) |

None of them ever holds a secret — credentials go to your OS keychain, or to the env vars the config names.

Environment overrides, defaults shown:

```
FLASHLIGHT_HOME=                          # lake root; default: platform user-data dir
FLASHLIGHT_BASE_CURRENCY=USD              # ingest asserts all rows match this
FLASHLIGHT_CONNECTIONS_PATH=              # default: <home>/config/connections.yml
FLASHLIGHT_PARQUET_COMPRESSION=zstd
FLASHLIGHT_PARQUET_COMPRESSION_LEVEL=3    # zstd 1-22
FLASHLIGHT_MCP_HOST=0.0.0.0
FLASHLIGHT_MCP_PORT=8002
FLASHLIGHT_DASHBOARD_HOST=127.0.0.1
FLASHLIGHT_DASHBOARD_PORT=8501
FLASHLIGHT_ASSISTANT_PROVIDER=            # overrides config/assistant.yml
FLASHLIGHT_ASSISTANT_MODEL=
FLASHLIGHT_ASSISTANT_BASE_URL=
FLASHLIGHT_ASSISTANT_API_KEY=             # only if no OS keychain is reachable
```

- [Ingest and manage data](https://getflashlight.app/guides/ingest/index.md) covers refreshes, backfills, and cleanup.
- [Explore the dashboard](https://getflashlight.app/guides/dashboard/index.md) explains the product surfaces and metric semantics.
- [FOCUS and cost integrity](https://getflashlight.app/concepts/focus-and-integrity/index.md) explains how to reconcile totals.
