# CLI reference

Run `flashlight --help` for the installed version's authoritative options. The commands below describe the stable operator interface.

## Core commands

| Command                                            | Description                                     |
| -------------------------------------------------- | ----------------------------------------------- |
| `flashlight init [--force]`                        | Create lake layout and starter configuration    |
| `flashlight sample [--rows 1000\|10000] [--force]` | Download and seed the FOCUS sample              |
| `flashlight sample --clean`                        | Remove only sample-seeded data and rebuild GOLD |
| `flashlight ingest`                                | Pull enabled connectors and rebuild GOLD        |
| `flashlight transform`                             | Rebuild GOLD from existing BRONZE data          |
| `flashlight cleanup`                               | Remove all lake data after confirmation         |
| `flashlight dashboard serve [--dev]`               | Start the local dashboard                       |
| `flashlight mcp serve`                             | Start the read-only MCP server                  |

## `flashlight ingest`

```
flashlight ingest [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                  [--connections PATH] [--connector NAME]
                  [--no-transform] [--full-refresh] [--run-id ID]
```

`--start` and `--end` define an inclusive window. `--connector` accepts a configured connection name or its type when unnamed. `--full-refresh` removes all historical BRONZE data for the selected connector before ingesting the requested window; use it with an explicit broad range. `--run-id` is useful for external scheduler correlation.

## `flashlight cleanup`

```
flashlight cleanup [--connector NAME] [--dry-run] [--yes]
```

Without `--connector`, this removes all BRONZE/GOLD data and run logs while preserving config. With a connector, it removes that connector's BRONZE data then rebuilds GOLD from what remains. Use `--dry-run` before destructive work.

## AWS export commands

| Command                          | Description                                            |
| -------------------------------- | ------------------------------------------------------ |
| `flashlight aws create-export`   | Provision an AWS FOCUS Data Export                     |
| `flashlight aws update-export`   | Update an existing export destination/options          |
| `flashlight aws describe-export` | Report delivered data periods, files, and size         |
| `flashlight aws bucket-policy`   | Print or merge-apply required S3 delivery policy       |
| `flashlight aws delete-export`   | Delete the AWS export; leaves delivered S3 data intact |

Use `--dry-run` on create/update/policy/delete where offered. AWS commands can read defaults from `connections.yml`; use `--yes` only in non-interactive automation after reviewing the resolved target.
