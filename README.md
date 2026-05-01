# diet — Directus Import Export Tool

Full-fidelity export and import of Directus collections with all metadata, validation rules, interface options, display settings, field sort order, and relations. API-only — no database connection required.

## Features

- **Full metadata preservation** — field notes, validation rules, interface options, O2M table layout, display settings, sort order
- **System entities** — export/import flows, operations, dashboards, panels, roles, presets, translations, webhooks
- **Interactive TUI** — collection picker with search, tabs (Collections | System), per-item selection
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

Per-profile tuning (set in `~/.config/diet/config.yml` via the wizard):

| Field | Default | Effect |
|-------|---------|--------|
| `concurrency` | 6 | Parallel workers for data pull/insert |
| `timeout` | 60 | HTTP timeout in seconds |
| `batch_size` | 100 | Items per batch POST during import |
| `retry_passes` | 5 | Max retry passes for FK-blocked rows |
| `format` | `zstd` | Archive format (`zstd` or `zip`) |

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

```bash
# Build and test against local instance
go build -o bin/diet ./cmd/diet
./bin/diet export --url=http://localhost:8055 --token=e2e-test-token --all --plain

# Run tests
go test ./cmd/diet/ -v
```

## Known Limitations

- **Sort field** — The `meta.sort_field` property on collections (used for drag-and-drop sorting in Directus UI) is not recreated on import. This field is managed by Directus internally and requires manual setup after import.
- **Users/permissions** — Not included in system entity export to avoid sensitive data and cross-instance reference issues.
