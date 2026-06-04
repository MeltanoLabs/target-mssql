# Azure Blob Storage stage

Replaces the row-by-row INSERT / chunked MERGE path with a three-step bulk load:

1. Each batch is serialised to a JSON file on disk.
2. The file is uploaded to Azure Blob Storage.
3. SQL Server reads it via `OPENROWSET(BULK …, DATA_SOURCE = …)` and executes either an `INSERT` (append-only streams) or a `MERGE` (streams with key properties).

The blob is deleted after a successful load.

## Prerequisites

### Python package

```bash
pip install 'target-mssql[azure]'
# or with uv:
uv sync --extra azure
```

### Azure Blob Storage

Create a Storage Account (general-purpose v2), a container (e.g. `mssql-stage`), and a SAS token scoped to that container with at minimum: **Read**, **Write**, **Create**, **Delete** on **Object** resource type.

Via the `az` CLI:

```bash
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
```

### SQL Server — one-time DBA setup

The target does **not** create the credential or external data source. A DBA must create them once before the first run.

**1. Database master key** (required once per database):

```sql
IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<strong-password>';
```

**2. Database scoped credential** — choose one option:

#### Option A: SAS token

Simpler to set up; the token expires and must be rotated periodically.

```sql
/*
-- To recreate the object, first run:
DROP EXTERNAL DATA SOURCE target_mssql_stage;
DROP DATABASE SCOPED CREDENTIAL [target_mssql_credential];
*/

CREATE DATABASE SCOPED CREDENTIAL [target_mssql_credential]
    WITH IDENTITY = N'SHARED ACCESS SIGNATURE',
         SECRET   = N'<sas-token-without-leading-?>';
```

To rotate the token later:

```sql
ALTER DATABASE SCOPED CREDENTIAL [target_mssql_credential]
    WITH IDENTITY = N'SHARED ACCESS SIGNATURE',
         SECRET   = N'<new-sas-token>';
```

#### Option B: Managed Identity

No secrets to rotate. Requires Azure SQL Server (not on-premises) with a system-assigned managed identity enabled.

**Prerequisites (Azure portal or CLI):**

1. Enable the system-assigned managed identity on the Azure SQL Server:
   - Portal: **Azure SQL Server → Security → Identity → System assigned managed identity → On**
   - CLI: `az sql server update --name <server> --resource-group <rg> --assign-identity`

2. Grant the SQL Server's managed identity **Storage Blob Data Owner** (or **Storage Blob Data Reader**) on the container:

   - Portal: **Storage account → Access Control (IAM) → + Add → Add role Assigment** and add the "Storage Blob Data Owner" role to the SQL Server managed identity

   - CLI:

     ```bash
     PRINCIPAL_ID=$(az sql server show \
       --name <server> \
       --resource-group <rg> \
       --query "identity.principalId" \
       --output tsv)

     az role assignment create \
       --assignee "$PRINCIPAL_ID" \
       --role "Storage Blob Data Owner" \
       --scope "/subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<account_name>/blobServices/default/containers/<container>"
     ```

**SQL:**

```sql
/*
-- To recreate the object, first run:
DROP EXTERNAL DATA SOURCE target_mssql_stage;
DROP DATABASE SCOPED CREDENTIAL [target_mssql_credential];
*/

CREATE DATABASE SCOPED CREDENTIAL [target_mssql_credential]
    WITH IDENTITY = N'MANAGED IDENTITY';
```

**3. External data source** pointing at the container:

```sql
CREATE EXTERNAL DATA SOURCE [target_mssql_stage]
    WITH (
        TYPE       = BLOB_STORAGE,
        LOCATION   = N'https://<account_name>.blob.core.windows.net/<container>',
        CREDENTIAL = [target_mssql_credential]
    );
```

**4. Grant the target user the required permissions:**

```sql
-- Database-level (run in the target database)
ALTER ROLE db_datareader ADD MEMBER [<db-user>];      -- SELECT on all tables
ALTER ROLE db_datawriter ADD MEMBER [<db-user>];      -- INSERT, UPDATE, DELETE on all tables
GRANT CREATE TABLE TO [<db-user>];                    -- create target tables for new streams
GRANT ALTER ANY EXTERNAL DATA SOURCE TO [<db-user>];  -- create the EXTERNAL DATA SOURCE on first run

-- Server-level (run as sysadmin in master)
GRANT ADMINISTER BULK OPERATIONS TO [<login-name>];   -- execute OPENROWSET(BULK …)
```

> **On-premises SQL Server 2017+** also requires Ad Hoc Distributed Queries:
> ```sql
> EXEC sp_configure 'show advanced options', 1; RECONFIGURE;
> EXEC sp_configure 'Ad Hoc Distributed Queries', 1; RECONFIGURE;
> ```
> Azure SQL Database has this enabled by default.

To drop the objects if needed:

```sql
DROP EXTERNAL DATA SOURCE [target_mssql_stage];
DROP DATABASE SCOPED CREDENTIAL [target_mssql_credential];
```

## Configuration

```json
{
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

## Notes

- **Minimum SQL Server version:** 2017 (14.x). Azure SQL Database and Azure SQL Managed Instance are fully supported.
- **Batch size:** use a larger `batch_size_rows` to amortise the per-batch round-trip to blob storage (e.g. `"batch_size_rows": 50000`).
- **Booleans:** stored as `BIT` in the staged file, inserted into the target's `VARCHAR(1)` column as `0`/`1`.
- **Complex types (objects, arrays):** serialised to JSON strings, stored as `NVARCHAR(MAX)` — consistent with the non-staged path.

## Further reading

- [CREATE DATABASE SCOPED CREDENTIAL (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-database-scoped-credential-transact-sql)
- [Import bulk data by using BULK INSERT or OPENROWSET(BULK…)](https://learn.microsoft.com/en-us/sql/relational-databases/import-export/import-bulk-data-by-using-bulk-insert-or-openrowset-bulk-sql-server)
- [Examples of bulk access to data in Azure Blob Storage](https://learn.microsoft.com/en-us/sql/relational-databases/import-export/examples-of-bulk-access-to-data-in-azure-blob-storage)
