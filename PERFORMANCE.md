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

## Batch timing breakdown (10k records, upsert path, after tempdb bypass)

| Step | Time | % |
|------|------|---|
| dedupe in memory | 0.12s | ~7% |
| `prepare_table` (DDL check) | 0.10s | ~6% |
| **`merge_upsert_records` (chunked MERGE)** | **~1.5s** | **~87%** |

## Key findings

### Local Docker (pymssql/pyodbc)

The bottleneck is **SQL Server's MERGE throughput into the main table** (~6,000–7,000 rows/sec).
The wall is SQL Server I/O, not Python or network overhead.

### Azure SQL staging (the SIT timeout investigation)

Root cause: **tempdb page-latch contention**. Azure SQL's tempdb is a shared resource
across all tenants. Two parallel streams both inserting into tempdb simultaneously compete
for the same PFS/GAM/SGAM allocation pages — which are shared server-wide, not per-session.
On a low-tier staging SKU this caused ~1 rec/sec throughput (439 records took 431 seconds).

The production environment (higher-tier Azure SQL) was not affected: 438 records in ~32s.

**Fix:** replace the temp-table upsert path (`create_temp → INSERT → MERGE`)
with `merge_upsert_records`, which uses chunked `MERGE … USING (VALUES …) AS source(…)`
directly into the target table. This bypasses tempdb entirely.

## What was changed

### `bulk_insert_records`
Uses explicit multi-row `INSERT … VALUES (…),(…),…` statements chunked to SQL Server's
2100-parameter limit. No-key-properties (append-only) path only.

Removed: `WITH (TABLOCK)` on temp table inserts. On Azure SQL (FULL recovery model)
TABLOCK does not enable minimal logging; it only acquires an exclusive table lock.
On local SQL Server with tempdb (SIMPLE recovery), it was a minor optimisation that is
now moot since we no longer write to tempdb.

### `merge_upsert_records` (new)
Used for the key-properties (upsert) path. Generates:
```sql
MERGE INTO target AS target
USING (VALUES (?, ?, …), …) AS source([col1], [col2], …)
ON (target.[key] = source.[key])
WHEN MATCHED THEN UPDATE SET …
WHEN NOT MATCHED BY TARGET THEN INSERT … VALUES …;
```
Chunked to the 2100-parameter limit. All chunks share one transaction.
No temp table, no tempdb I/O, no page-latch contention.

## Remaining levers to explore

- **Larger `batch_size_rows`** — the 0.1s `prepare_table` DDL overhead is paid once per
  batch; fewer bigger batches amortise it. Set via `{"batch_size_rows": 50000}` in config.
- **Skip `prepare_table` when schema is unchanged** — saves ~0.1s per batch after the first.
- **SQL Server bulk-copy (BCP)** — bypasses logging entirely but requires file-system access
  not available through pymssql or pyodbc.
