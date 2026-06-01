# Azure Blob Storage stage

The blob-stage path replaces the row-by-row INSERT / chunked MERGE path with a three-step bulk load:

1. Each batch is serialised to a JSON file on disk.
2. The file is uploaded to Azure Blob Storage.
3. SQL Server reads it directly via `OPENROWSET(BULK …, DATA_SOURCE = …)` and executes either an `INSERT` (append-only streams) or a `MERGE` (streams with key properties).

The blob is deleted after a successful load.

## Prerequisites

### Python package

```bash
pip install 'target-mssql[azure]'
# or with uv:
uv sync --extra azure
```

### Azure Blob Storage

1. Create a Storage Account (general-purpose v2, LRS is fine for staging).
2. Create a **container** inside it (e.g. `mssql-stage`).
3. Generate a **Shared Access Signature (SAS) token** scoped to the container with at minimum:
   - **Allowed services:** Blob
   - **Allowed resource types:** Object
   - **Allowed permissions:** Read, Write, Create, Delete
   - **Expiry:** set to suit your rotation policy

   **Via the Azure Portal:** Storage Account → **Shared access signature** → configure and click **Generate SAS and connection string**. Copy the **SAS token** value (starts with `sv=`; omit any leading `?`).

   **Via the `az` CLI** (recommended — no 7-day limit when using an account key):

   ```bash
   # Create the container (skip if it already exists)
   az storage container create \
     --account-name <storage_account> \
     --name mssql-stage \
     --auth-mode login

   # Generate a long-lived SAS token using the account key
   KEY=$(az storage account keys list \
     --account-name <storage_account> \
     --query "[0].value" \
     --output tsv)

   SAS=$(az storage container generate-sas \
     --account-name <storage_account> \
     --name mssql-stage \
     --permissions rwdc \
     --expiry 2027-01-01T00:00:00Z \
     --https-only \
     --account-key "$KEY" \
     --output tsv)

   echo "$SAS"
   ```

   Minimum permissions: **r**ead (OPENROWSET load), **w**rite + **c**reate (upload), **d**elete (post-load cleanup).

### SQL Server permissions

#### Default setup (target manages the credential)

The database user running the target must have:

| Permission | Level | Purpose |
|---|---|---|
| `ALTER ANY DATABASE SCOPED CREDENTIAL` | Database | Create/alter the `DATABASE SCOPED CREDENTIAL` |
| `ALTER ANY EXTERNAL DATA SOURCE` | Database | Create the `EXTERNAL DATA SOURCE` |
| `ADMINISTER BULK OPERATIONS` | Server | Execute `OPENROWSET(BULK …)` |

```sql
-- Database-level (run in the target database)
GRANT ALTER ANY DATABASE SCOPED CREDENTIAL TO [<db-user>];
GRANT ALTER ANY EXTERNAL DATA SOURCE TO [<db-user>];

-- Server-level (run as sysadmin in master)
GRANT ADMINISTER BULK OPERATIONS TO [<login-name>];
```

> A `DATABASE MASTER KEY` must also exist before any scoped credential can be created.
> This is a one-time prerequisite that requires `CONTROL DATABASE` and must be done by a DBA:
> ```sql
> IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
>     CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<strong-password>';
> ```

#### Restricted setup (`skip_credential_setup: true`)

If the database user lacks `CONTROL DATABASE`, a DBA can pre-create the objects once (see [Pre-creating the credential manually](#pre-creating-the-credential-manually)) and the target can skip that step. The user then only needs:

| Permission | Purpose |
|---|---|
| `ALTER ANY EXTERNAL DATA SOURCE` | Create the `EXTERNAL DATA SOURCE` on first run |
| `ADMINISTER BULK OPERATIONS` | Execute `OPENROWSET(BULK …)` |

> **On-premises SQL Server 2017+** also requires Ad Hoc Distributed Queries to be enabled:
> ```sql
> EXEC sp_configure 'show advanced options', 1; RECONFIGURE;
> EXEC sp_configure 'Ad Hoc Distributed Queries', 1; RECONFIGURE;
> ```
> Azure SQL Database has this enabled by default.

## Configuration

Add the `azure_blob_storage` block to your target-mssql config:

```json
{
  "host": "myserver.database.windows.net",
  "database": "mydb",
  "username": "myuser",
  "password": "...",
  "azure_blob_storage": {
    "account_name": "mystorageaccount",
    "sas_token": "sv=2023-11-03&ss=b&srt=o&sp=rwdlacupiytfx&...",
    "container": "mssql-stage",
    "path_prefix": "target-mssql"
  }
}
```

| Option | Required | Default | Description |
|---|---|---|---|
| `account_name` | Yes | — | Azure Storage account name |
| `sas_token` | Yes | — | SAS token (without a leading `?`) |
| `container` | Yes | — | Blob container used as the staging area |
| `path_prefix` | No | `target-mssql` | Virtual directory prefix inside the container |
| `skip_credential_setup` | No | `false` | Skip automatic `CREATE`/`ALTER DATABASE SCOPED CREDENTIAL`. Use when the DB user lacks `CONTROL DATABASE`. See [Pre-creating the credential manually](#pre-creating-the-credential-manually). |

## What the target creates in SQL Server

On the first batch the target creates the following objects (idempotently — safe to run repeatedly):

```sql
-- Required by SQL Server before any DATABASE SCOPED CREDENTIAL can be created.
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '…';

-- Stores the SAS token; updated automatically on every run so token rotation
-- takes effect without manual intervention.
CREATE DATABASE SCOPED CREDENTIAL [target_mssql_credential]
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = '<sas_token>';

-- Points SQL Server at the blob container.
CREATE EXTERNAL DATA SOURCE [target_mssql_stage]
    WITH (
        TYPE        = BLOB_STORAGE,
        LOCATION    = 'https://<account>.blob.core.windows.net/<container>',
        CREDENTIAL  = [target_mssql_credential]
    );
```

These objects persist across runs. If you need to drop them manually:

```sql
DROP EXTERNAL DATA SOURCE [target_mssql_stage];
DROP DATABASE SCOPED CREDENTIAL [target_mssql_credential];
```

## Pre-creating the credential manually

If the database user running the target lacks `CONTROL DATABASE` permission, a DBA must create the credential and external data source once before the first run.  After that, set `skip_credential_setup: true` in the target config so the target never attempts to `CREATE` or `ALTER` the credential.

**1. Create the master key** (required once per database; skip if one already exists):

```sql
IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<choose-a-strong-password>';
```

**2. Create the scoped credential** with the SAS token from your blob container:

```sql
CREATE DATABASE SCOPED CREDENTIAL [target_mssql_credential]
    WITH IDENTITY = N'SHARED ACCESS SIGNATURE',
    SECRET = N'<sas-token-without-leading-?>';
```

> When you rotate the SAS token, run `ALTER DATABASE SCOPED CREDENTIAL` manually (or temporarily remove `skip_credential_setup` to let the target update it):
> ```sql
> ALTER DATABASE SCOPED CREDENTIAL [target_mssql_credential]
>     WITH IDENTITY = N'SHARED ACCESS SIGNATURE',
>     SECRET = N'<new-sas-token>';
> ```

**3. Create the external data source** pointing at your container:

```sql
CREATE EXTERNAL DATA SOURCE [target_mssql_stage]
    WITH (
        TYPE       = BLOB_STORAGE,
        LOCATION   = N'https://<account_name>.blob.core.windows.net/<container>',
        CREDENTIAL = [target_mssql_credential]
    );
```

**4. Grant the target user the remaining permissions:**

```sql
GRANT ALTER ANY EXTERNAL DATA SOURCE TO [<db-user>];
GRANT ADMINISTER BULK OPERATIONS TO [<db-user>];  -- server-level; run as sysadmin
```

**5. Set `skip_credential_setup: true` in your target config:**

```json
{
  "azure_blob_storage": {
    "account_name": "mystorageaccount",
    "sas_token": "sv=…",
    "container": "mssql-stage",
    "skip_credential_setup": true
  }
}
```

## How the load SQL works

**Append-only streams** (no `key_properties`):

```sql
INSERT INTO [dbo].[my_table] ([col1], [col2], …)
SELECT [col1], [col2], …
FROM OPENROWSET(
    BULK  N'target-mssql/<uuid>/data.json',
    DATA_SOURCE = N'target_mssql_stage',
    SINGLE_CLOB
) AS _blob
CROSS APPLY OPENJSON(_blob.BulkColumn)
WITH (
    [col1] NVARCHAR(MAX) '$.col1',
    [col2] BIGINT        '$.col2',
    …
) AS _src;
```

**Upsert streams** (with `key_properties`):

```sql
MERGE INTO [dbo].[my_table] AS _target
USING (
    SELECT * FROM OPENROWSET(…) AS _blob
    CROSS APPLY OPENJSON(_blob.BulkColumn) WITH (…) AS _src
) AS _src
ON (_target.[id] = _src.[id])
WHEN MATCHED THEN UPDATE SET …
WHEN NOT MATCHED BY TARGET THEN INSERT (…) VALUES (…);
```

## Notes and limitations

- **NULL vs empty string:** JSON `null` is preserved as SQL `NULL`. Intentionally empty strings in source data are preserved correctly.
- **Booleans:** Singer boolean values are stored as SQL `BIT` in the staged file, then inserted into the target's `VARCHAR(1)` column as `0`/`1`.
- **Complex types (objects, arrays):** already serialised to JSON strings by the target before staging, so they arrive in the target table as `NVARCHAR(MAX)` text — consistent with the non-staged path.
- **Batch size:** combine with a larger `batch_size_rows` to amortise the per-batch round-trip to blob storage (e.g. `"batch_size_rows": 50000`).
- **SAS token rotation:** update the token in config and restart the target. The credential is altered automatically on the next run.
- **Minimum SQL Server version:** 2017 (14.x) for `FORMAT='CSV'` and `OPENROWSET` with `DATA_SOURCE`. Azure SQL Database and Azure SQL Managed Instance are fully supported.
