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

## Performance benchmarking

### Setup

Start MSSQL and create a config file:

```bash
docker compose up -d sink
cat > benchmark_config.json <<'EOF'
{"schema":"dbo","username":"sa","password":"P@55w0rd","host":"localhost","port":"1433","database":"master","table_prefix":"bench_"}
EOF
```

### Generate synthetic data and time a run

```bash
uv run python target_mssql/tests/generate_benchmark_data.py 100000 \
  | { time uv run target-mssql --config benchmark_config.json; } 2>&1 \
  | grep -E "METRIC|cpu"
```

### Baseline (2026-05-11, local Docker MSSQL 2022, upsert path)

| Records | Wall time | Throughput |
|---------|-----------|------------|
| 1,000   | 1.63s     | ~610 rec/s |
| 10,000  | 2.77s     | ~3,610 rec/s |
| 100,000 | 23.4s     | ~4,270 rec/s |

Fixed startup + first-batch overhead is ~1.3s. At steady state each 10,000-record batch
takes ~2s through the temp-table + MERGE path.

### Key bottleneck

Each batch goes: deduplicate in memory → `INSERT` into `#temp` table → `MERGE INTO` main.
The MERGE is the dominant cost. SDK default batch size is 10,000 rows (`MAX_SIZE_DEFAULT`).

Key files for performance work:
- `target_mssql/sinks.py` — `process_batch`, `bulk_insert_records`, `merge_upsert_from_table`
- `target_mssql/connector.py` — connection and schema management

## Architecture

- **`target_mssql/target.py`** — entry point, config schema
- **`target_mssql/connector.py`** — `mssqlConnector`: SQLAlchemy connection, DDL helpers
- **`target_mssql/sinks.py`** — `mssqlSink`: batch processing, upsert via temp table + MERGE

The upsert path: records → deduplicate in memory → bulk insert into `#temp` table → `MERGE INTO` main table.
