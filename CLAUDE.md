# target-mssql

Singer target for Microsoft SQL Server, built with the Meltano Singer SDK.

## Setup

```bash
uv sync
```

## Running tests

Tests require a live MSSQL instance. Start one with Docker:

```bash
docker compose up -d sink
```

Run the full test suite:

```bash
uv run pytest --capture=no
```

Run a single test:

```bash
uv run pytest target_mssql/tests/test_core.py::test_simple_continents --capture=no
```

## Linting

```bash
uvx tox -e lint      # check
uvx tox -e format    # auto-fix
```

## Performance

See [PERFORMANCE.md](PERFORMANCE.md) for benchmarks, timing breakdown, and next steps.

Key files:
- `target_mssql/sinks.py` — `process_batch`, `bulk_insert_records`, `merge_upsert_from_table`
- `target_mssql/connector.py` — connection and schema management

## Architecture

- **`target_mssql/target.py`** — entry point, config schema
- **`target_mssql/connector.py`** — `mssqlConnector`: SQLAlchemy connection, DDL helpers
- **`target_mssql/sinks.py`** — `mssqlSink`: batch processing, upsert via temp table + MERGE

The upsert path: records → deduplicate in memory → bulk insert into `#temp` table → `MERGE INTO` main table.
