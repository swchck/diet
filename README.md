# diet — Directus Import Export Tool

[![ci](https://github.com/swchck/diet/actions/workflows/ci.yml/badge.svg)](https://github.com/swchck/diet/actions/workflows/ci.yml)

Full-fidelity export and import of Directus collections with all metadata, validation rules, interface options, display settings, field sort order, and relations. API-only — no database connection required.

## Features

- **Full metadata preservation** — field notes, validation rules, interface options, O2M table layout, display settings, sort order
- **System entities** — export/import flows, operations, dashboards, panels, roles, presets, translations, webhooks
- **Bulk schema apply** — uses Directus `/schema/diff` + `/schema/apply` for ~30× faster schema phase, with automatic fallback to per-field POSTs
- **Parallel data import** — concurrent chunk POSTs inside each collection while preserving FK-safe topological order
- **Optional audit-log skip** — `--strip-accountability` cuts data import time roughly in half by setting `meta.accountability=null` on collections
- **Interactive TUI** — collection picker with search, tabs (Collections | System), per-item selection; per-import options screen
- **Profile wizard** — `diet` (no args) launches a profile picker; servers persist to `~/.config/diet/config.yml`
- **Diff** — side-by-side compare of two instances (collections, fields, relations, system counts)
- **Clean command** — delete collections and system entities with confirmation
- **Smart ordering** — topological sort for FK dependencies, multi-pass insert with retry
- **Two archive formats** — tar+zstd (best compression, default) or zip

## Installation

```bash
go install github.com/swchck/diet/cmd/diet@latest
```

Or build from source:

```bash
go build -o bin/diet ./cmd/diet
```

## Usage

### Wizard mode

Running `diet` with no subcommand launches an interactive wizard: pick the operation, pick a saved server profile (or create one), then jump into the relevant TUI. New profiles are saved to `~/.config/diet/config.yml` (mode `0600`).

```bash
diet                            # wizard → operation → profile → action
```

### Export

```bash
# Interactive TUI — select collections and system entities
diet export --url=https://directus.example.com --token=YOUR_TOKEN

# Export all collections without TUI
diet export --url=https://directus.example.com --token=YOUR_TOKEN --all

# Custom output path and format
diet export --url=... --token=... -o backup.zip --format=zip
```

### Import

```bash
# Import archive into target Directus
diet import -i backup.tar.zst --target-url=http://localhost:8055 --target-token=TOKEN

# With token refresh (for long imports)
diet import -i backup.tar.zst --target-url=... --target-token=... \
  --email=admin@example.com --password=admin

# Skip data, import schema only
diet import -i backup.tar.zst --target-url=... --target-token=... --data=false

# Fastest path: skip audit log + crank concurrency
diet --concurrency=24 --batch-size=200 import -i backup.tar.zst \
  --target-url=... --target-token=... --strip-accountability
```

### Clean

```bash
# Interactive TUI — select what to delete
diet clean --url=https://directus.example.com --token=YOUR_TOKEN

# Delete all collections and system entities (with confirmation)
diet clean --url=... --token=... --all --system
```

### Diff

```bash
# Compare two instances — schema differences and item counts.
diet diff --url=https://src --token=SRC_TOKEN \
          --target-url=https://dst --target-token=DST_TOKEN

# With no flags: pick source and target from saved profiles in the TUI.
diet diff
```

### Common Flags

| Flag | Description |
|------|-------------|
| `--url` | Directus instance URL |
| `--token` | Static access token |
| `--plain` | Plain text output for scripting (no TUI) |
| `--concurrency` | Parallel HTTP workers (default 6) |
| `--timeout` | HTTP timeout in seconds (default 60) |
| `--batch-size` | Items per batch POST during data import (default 100) |
| `--retry-passes` | Max retry passes for FK-blocked rows (default 5) |

`import`-specific flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--bulk-schema` | `true` | Use Directus `/schema/apply` for the schema phase (~30× faster). On any error — payload too large, version mismatch — falls back to the per-field path automatically. |
| `--strip-accountability` | `false` | Set `meta.accountability=null` on every imported collection. Skips `directus_activity` (audit log) and `directus_revisions` writes during data import — roughly halves the data-phase time on Directus's side. Reversible after import via the admin UI (collection settings → Activity & Revisions Tracking). |
| `--data` | `true` | Also import item data. Set `--data=false` for schema-only. |

The same tuning fields are persisted per-profile in `~/.config/diet/config.yml` via the wizard:

| Field | Default | Effect |
|-------|---------|--------|
| `concurrency` | 6 | Parallel workers for data pull/insert |
| `timeout` | 60 | HTTP timeout in seconds |
| `batch_size` | 100 | Items per batch POST during import |
| `retry_passes` | 5 | Max retry passes for FK-blocked rows |
| `format` | `zstd` | Archive format (`zstd` or `zip`) |

## Performance

On a real-world archive (119 collections, 858 fields, 158k items, 1.5MB compressed) against a single-node Dockerized Directus:

| Settings | Time |
|----------|-----:|
| `--bulk-schema=false` (legacy per-field path) | ~2m 30s |
| Default (`--bulk-schema=true`) | ~1m 30s |
| `--concurrency=24 --batch-size=200` | ~1m 25s |
| `+ --strip-accountability` | ~35s |

The biggest single lever is `--strip-accountability`. The remaining floor is Directus itself — its Node process saturates one CPU core handling validation, ACL, and schema cache lookups per row. Past `--concurrency=12` returns flatten because Directus is the bottleneck, not the diet client.

### Target Directus requirements

`--bulk-schema=true` posts the entire snapshot as a single request. The default Directus `MAX_PAYLOAD_SIZE=1mb` rejects real-world archives — set it to `10mb` or higher on the target:

```yaml
# docker-compose.yml on the target
environment:
  MAX_PAYLOAD_SIZE: "10mb"
```

If that's not feasible, pass `--bulk-schema=false` and diet falls back to per-field POSTs (slower, but no payload-size requirement).

## Archive Format

```
diet-export/
  manifest.json                 # version info, source URL, collection list
  schema/
    collections.json            # collection definitions (folders + tables)
    fields.json                 # all fields with full metadata
    relations.json              # FK and M2M relations
  data/
    <collection>.json           # item data per collection
  system/
    flows.json                  # Directus system entities
    operations.json
    dashboards.json
    ...
```

## Local Development

Requires Docker for a local Directus instance:

```bash
docker compose up -d            # Start Postgres, Redis, Directus
docker compose down -v          # Clean reset
```

Local Directus runs at `http://localhost:8055` with credentials `admin@example.com` / `admin` and static token `e2e-test-token`.

The bundled `docker-compose.yml` is tuned for fast local imports — not a production recipe. Notable env vars set on the Directus service:

- `MAX_PAYLOAD_SIZE=10mb` — required by `--bulk-schema`
- `LOG_LEVEL=error` — keeps real failures visible, drops the per-request info noise
- `RATE_LIMITER_ENABLED=false`, `PRESSURE_LIMITER_ENABLED=false` — the pressure limiter would otherwise 503 us when Directus saturates CPU under bulk import
- `WEBSOCKETS_ENABLED=false`, `OPENAPI_ENABLED=false`, `GRAPHQL_INTROSPECTION=false`, `TELEMETRY=false` — drop unused surface
- `ACCEPT_TERMS=true` — pre-acks the BSL banner in the admin UI

```bash
# Build and smoke against local instance
go build -o bin/diet ./cmd/diet
./bin/diet export --url=http://localhost:8055 --token=e2e-test-token --all --plain

# Run tests (CI runs the same with -race)
go test -race -count=1 ./...
```

## Known Limitations

- **Sort field** — The `meta.sort_field` property on collections (used for drag-and-drop sorting in Directus UI) is not recreated on import. This field is managed by Directus internally and requires manual setup after import.
- **Users/permissions** — Not included in system entity export to avoid sensitive data and cross-instance reference issues.
