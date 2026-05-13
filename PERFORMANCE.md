# Performance notes

## How to benchmark

Start MSSQL and create a config file:

```bash
docker compose up -d sink
cat > benchmark_config.json <<'EOF'
{"schema":"dbo","username":"sa","password":"P@55w0rd","host":"localhost","port":"1433","database":"master","table_prefix":"bench_"}
EOF
```

Generate synthetic data and time a run:

```bash
uv run python target_mssql/tests/generate_benchmark_data.py 100000 \
  | { time uv run target-mssql --config benchmark_config.json; } 2>&1 \
  | grep -E "METRIC|cpu"
```

## Benchmarks (local Docker MSSQL 2022, upsert path, 8-column schema)

| SDK | Driver | `bulk_insert_records` | 10k batch time | 100k wall time | Throughput |
|-----|--------|-----------------------|----------------|----------------|------------|
| 0.48.1 | pymssql | executemany (original) | 1.97s | 23.4s | ~4,270 rec/s |
| 0.48.1 | pymssql | multi-row INSERT + TABLOCK | 1.87s | 23.3s | ~4,290 rec/s |
| 0.53.7 | pymssql | multi-row INSERT + TABLOCK | 2.17s | 21.2s | ~4,720 rec/s |
| 0.53.7 | **pyodbc** | multi-row INSERT + TABLOCK | **1.48s** | **9.8s** | **~10,240 rec/s** |
| 0.54.0 | pymssql | multi-row INSERT + TABLOCK | — | 22.0s | ~4,550 rec/s |
| 0.54.0 | pyodbc | multi-row INSERT + TABLOCK | — | 9.5s | ~10,530 rec/s |

pyodbc (ODBC Driver 18 for SQL Server) is **2.2× faster** than pymssql for this workload.
The ODBC Driver 18 has a more efficient TDS implementation than pymssql/FreeTDS, particularly
for parameterised multi-row inserts.

SDK 0.54.0 shows no significant change vs 0.53.7 (within measurement noise).
SDK 0.53.x delivered ~10% throughput improvement over 0.48.1 with pymssql.
Our `bulk_insert_records` rewrite did not change raw throughput (the bottleneck is SQL
Server tempdb I/O), but it correctly reports row counts, uses TABLOCK for minimal logging,
and works correctly with both pymssql (`%s`) and pyodbc (`?`) parameter styles.

Fixed startup + first-batch overhead is ~1.3s. At steady state each 10,000-record batch
takes ~2s through the temp-table + MERGE path.

## Batch timing breakdown (10k records, upsert path)

| Step | Time | % |
|------|------|---|
| dedupe in memory | 0.12s | 6% |
| `prepare_table` (DDL check) | 0.10s | 5% |
| `create_temp_table` (DROP+SELECT INTO) | 0.02s | 1% |
| **`bulk_insert` into `#temp`** | **1.55s** | **83%** |
| `MERGE INTO` main table | 0.07s | 4% |

## Key finding

The bottleneck is **SQL Server's INSERT throughput into tempdb** (~6,000–7,000 rows/sec).
Confirmed by trying: multi-row INSERTs (same speed), `WITH (TABLOCK)` (marginal),
`SET NOCOUNT ON` (no effect), inline-VALUES MERGE (slower — 2.1s vs 1.7s), and measuring
the no-primary-key path (same ~1.7s for 10k rows directly into the main table).
The wall is SQL Server I/O in Docker, not Python or network overhead.

## What was changed

`bulk_insert_records` now uses explicit multi-row `INSERT … VALUES (…),(…),…` statements
chunked to SQL Server's 2100-parameter limit, with `WITH (TABLOCK)` on temp table inserts.
This is cleaner than SQLAlchemy `executemany` (which also returned a wrong `rowcount`)
and enables minimal logging on the heap temp table.

## Remaining levers to explore

- **Larger `batch_size_rows`** — the 0.1s `prepare_table` + 0.02s temp-table DDL overhead
  is paid once per batch; fewer bigger batches would amortize it.
  Set via `{"batch_size_rows": 50000}` in config.
- **Skip `prepare_table` when schema is unchanged** — saves ~0.1s per batch after the first.
- **SQL Server bulk-copy (BCP)** — would bypass tempdb logging entirely but requires
  file-system access not available through pymssql.
