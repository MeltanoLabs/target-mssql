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

### Upsert path: temp table + MERGE (old, pre-#15)

| SDK | Driver | Upsert method | 10k batch time | 100k wall time | Throughput |
|-----|--------|---------------|----------------|----------------|------------|
| 0.48.1 | pymssql | executemany (original) | 1.97s | 23.4s | ~4,270 rec/s |
| 0.48.1 | pymssql | multi-row INSERT #temp + MERGE | 1.87s | 23.3s | ~4,290 rec/s |
| 0.53.7 | pymssql | multi-row INSERT #temp + MERGE | 2.17s | 21.2s | ~4,720 rec/s |
| 0.53.7 | **pyodbc** | multi-row INSERT #temp + MERGE | **1.48s** | **9.8s** | **~10,240 rec/s** |
| 0.54.0 | pymssql | multi-row INSERT #temp + MERGE | — | 22.0s | ~4,550 rec/s |
| 0.54.0 | pyodbc | multi-row INSERT #temp + MERGE | — | 9.5s | ~10,530 rec/s |

These rows used `INSERT INTO #temp … WITH (TABLOCK)` + single `MERGE #temp → target`.
TABLOCK enables minimal logging on local SQL Server (SIMPLE recovery), making #temp inserts
fast. This path was abandoned in #15 because Azure SQL's shared tempdb causes page-latch
contention under parallel streams.

### Upsert path: chunked MERGE VALUES (current, post-#15)

| SDK | Driver | Upsert method | 100k wall time | Throughput |
|-----|--------|---------------|----------------|------------|
| 0.54.2 | pymssql | chunked MERGE … USING (VALUES …) | 37.7s | ~2,650 rec/s |
| 0.54.2 | **pyodbc** | chunked MERGE … USING (VALUES …) | **17.6s** | **~5,680 rec/s** |
| 0.54.2 | pymssql | staging table (INSERT heap → MERGE) | 30.3s | ~3,300 rec/s |
| 0.54.2 | **pyodbc** | staging table (INSERT heap → MERGE) | **14.3s** | **~7,000 rec/s** |

pyodbc (ODBC Driver 18 for SQL Server) is **~2× faster** than pymssql for this workload.
The ODBC Driver 18 has a more efficient TDS implementation than pymssql/FreeTDS, particularly
for parameterised multi-row inserts.

The chunked MERGE VALUES path is ~40–45% slower than the old temp-table path locally.
This is expected: TABLOCK + minimal logging on #temp is inherently faster than direct
MERGE into a fully-logged target table. The trade-off is deliberate — no tempdb contention
on Azure SQL.

**Staging table approach** is **~19–20% faster** than direct chunked MERGE VALUES locally
(same SDK). It trades N chunked MERGE statements for N simpler INSERT chunks + 1 efficient
table-to-table MERGE, which reduces per-statement work even though statement count is similar.
On a high-latency connection (Azure SQL) the single expensive MERGE may offer a larger
advantage. The permanent staging table (not `#temp`) avoids the tempdb page-latch contention
that originally motivated the switch to direct MERGE VALUES.

Fixed startup + first-batch overhead is ~1.3s. At steady state each 10,000-record batch
takes ~2s through the temp-table + MERGE path.

### Upsert path: Azure Blob Storage stage (Azure SQL staging, 8-column schema)

Measured against `meltano-staging.database.windows.net` (Azure SQL), 50k-row batches.

| Driver | Upsert method | Batch 1 | Batch 2 | 100k wall time | Throughput |
|--------|---------------|---------|---------|----------------|------------|
| pyodbc | staging table (INSERT heap → MERGE) | 133.7s | 115.1s | 4:22 | ~402 rec/s |
| **pyodbc** | **blob stage (upload → OPENROWSET MERGE)** | **42.3s** | **38.6s** | **1:44** | **~961 rec/s** |
| pymssql | staging table (INSERT heap → MERGE) | 214.8s | 198.3s | 7:06 | ~235 rec/s |
| **pymssql** | **blob stage (upload → OPENROWSET MERGE)** | **41.6s** | **47.3s** | **1:41** | **~985 rec/s** |

Key observations:

- **Blob stage is ~2.4× faster for pyodbc, ~4.2× faster for pymssql** compared to the
  staging-table path. The staging-table path pays Azure SQL round-trip latency on every
  INSERT chunk (~24 round-trips per 50k batch); the blob path pays one upload + one
  in-Azure MERGE, which is far cheaper over a high-latency WAN connection.
- **The blob path eliminates the pyodbc/pymssql gap entirely.** With the staging-table
  path pyodbc is ~1.7× faster than pymssql (driver matters for parameterised inserts).
  With the blob path both drivers run at ~960-985 rec/s — the bottleneck shifts to the
  server-side OPENROWSET MERGE, where the driver is not involved.
- **Blob batch times are consistent** (pyodbc: 42.3/38.6s; pymssql: 41.6/47.3s)
  compared to the staging-table path (pyodbc: 133.7/115.1s; pymssql: 214.8/198.3s),
  confirming that variability in the old path comes from parameterised-insert overhead.
- **Why it doesn't win as dramatically on local Docker:** there the round-trip latency is
  <1 ms and the bottleneck is SQL Server CPU / I/O, so the OPENROWSET overhead matters
  more relative to the INSERT savings.

## Batch timing breakdown (10k records, upsert path, chunked MERGE VALUES)

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
