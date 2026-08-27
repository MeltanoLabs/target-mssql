"""Native Arrow ingestion for MSSQL via mssql-python's `Cursor.bulkcopy_arrow`.

Bulk-copies Arrow data directly into SQL Server over TDS, without converting rows to
Python dicts first. Used for Singer `BATCH` messages with `encoding: {"format": "arrow"}`.

This intentionally uses a connection separate from the target's main SQLAlchemy engine
(which may be configured with `driver: pymssql` or `driver: pyodbc`): mssql-python is the
only driver that exposes `bulkcopy_arrow`, so Arrow batches always go through it regardless
of what the rest of the target is configured to use.
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy.engine.url

if TYPE_CHECKING:
    from collections.abc import Callable

    import mssql_python

    from target_mssql.connector import MSSQLConnector


def connect_kwargs_from_url(url: str, *, trust_server_certificate: bool = False) -> dict[str, Any]:
    """Derive `mssql_python.connect()` kwargs from the target's resolved SQLAlchemy URL.

    Works regardless of whether the user configured discrete fields (host/port/database/
    username/password) or a raw `sqlalchemy_url`, since `MSSQLConnector.sqlalchemy_url` is
    always a fully resolved URL string by the time a sink is created.
    """
    parsed = sqlalchemy.engine.url.make_url(url)
    kwargs: dict[str, Any] = {
        "Server": f"{parsed.host},{parsed.port}" if parsed.port else parsed.host,
        "Database": parsed.database,
    }
    if parsed.username:
        kwargs["UID"] = parsed.username
    if parsed.password:
        kwargs["PWD"] = parsed.password
    if trust_server_certificate:
        kwargs["TrustServerCertificate"] = "yes"
    return kwargs


def _conform_column(
    column: pa.ChunkedArray,
    property_jsonschema: dict,
    connector: MSSQLConnector,
) -> pa.ChunkedArray | pa.Array:
    """Coerce one Arrow column to match the SQL type already decided for it.

    No new type inference happens here: object/array columns become JSON strings
    (matching `MSSQLSink.preprocess_record`'s `json.dumps`), booleans become "1"/"0"
    strings (matching `MSSQLConnector.to_sql_type`'s VARCHAR(1) mapping), and NUL bytes
    are stripped from plain strings (matching `preprocess_record`'s NUL-stripping).
    """
    if connector._jsonschema_type_check(property_jsonschema, ("object",)) or connector._jsonschema_type_check(
        property_jsonschema, ("array",)
    ):
        values = [None if v is None else json.dumps(v, default=str) for v in column.to_pylist()]
        return pa.array(values, type=pa.string())

    if connector._jsonschema_type_check(property_jsonschema, ("boolean",)):
        bool_array = column if pa.types.is_boolean(column.type) else column.cast(pa.bool_())
        return pc.if_else(bool_array, pa.scalar("1"), pa.scalar("0"))

    if connector._jsonschema_type_check(property_jsonschema, ("number",)) and not connector.config.get(
        "prefer_float_over_numeric", False
    ):
        # `to_sql_type` maps "number" to NUMERIC(38, 16) unless prefer_float_over_numeric is
        # set. mssql-python's Arrow writer needs a matching Arrow decimal128 type here -- it
        # doesn't do float64->NUMERIC conversion the way row-by-row bulkcopy() would.
        if not pa.types.is_decimal(column.type):
            return column.cast(pa.decimal128(38, 16))
        return column

    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
        return pc.replace_substring(column, pattern="\x00", replacement="")

    return column


def conform_arrow_table(
    table: pa.Table,
    schema: dict,
    conform_name: Callable[[str, str], str],
    connector: MSSQLConnector,
) -> pa.Table:
    """Rename Arrow fields to conformed column names, drop unknown columns, and coerce values.

    Only columns present in both the Arrow table and the stream's (conformed) JSON schema
    are kept, mirroring how the dict-based path silently ignores unknown record keys.
    """
    table = table.rename_columns([conform_name(name, "column") for name in table.column_names])

    properties = schema["properties"]
    keep = [name for name in table.column_names if name in properties]
    table = table.select(keep)

    for i, col_name in enumerate(table.column_names):
        conformed = _conform_column(table.column(i), properties[col_name], connector)
        table = table.set_column(i, col_name, conformed)

    return table


def new_temp_table_name() -> str:
    """A unique global temp table name for staging one Arrow batch.

    `bulkcopy_arrow` runs on its own internal connection under the hood, distinct from
    the DBAPI cursor's connection -- a local (session-scoped) `#temp` table created via
    `cursor.execute(...)` is therefore invisible to it. A *global* `##temp` table is
    visible across connections and gets cleaned up explicitly after the MERGE.
    """
    return f"##arrow_batch_{uuid.uuid4().hex}"


def bulk_copy_append(cursor: mssql_python.Cursor, full_table_name: str, table: pa.Table) -> int:
    """Bulk-copy an Arrow table directly into an existing destination table."""
    result = cursor.bulkcopy_arrow(full_table_name, table, column_mappings=table.column_names)
    return result["rows_copied"]


def bulk_copy_upsert(
    cursor: mssql_python.Cursor,
    temp_table_name: str,
    source_table_name: str,
    table: pa.Table,
    merge_sql: str,
) -> int:
    """Stage an Arrow table into a fresh global temp table, then MERGE it into the target.

    Uses a global (`##`) temp table rather than a local one because `bulkcopy_arrow`
    opens its own internal connection, separate from `cursor`'s -- a local `#temp` table
    would be invisible to it (and, since that internal connection commits independently,
    `cursor`'s connection runs in autocommit mode so the staging DDL doesn't hold a lock
    that would block it). The `MERGE` statement itself is atomic; the temp table is
    dropped in a `finally` regardless of whether the bulk-copy or merge step raises.
    """
    cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
    cursor.execute(f"SELECT TOP 0 * INTO {temp_table_name} FROM {source_table_name}")  # noqa: S608
    try:
        cursor.bulkcopy_arrow(
            temp_table_name,
            table,
            column_mappings=table.column_names,
            use_internal_transaction=False,
        )
        cursor.execute(merge_sql)
        return cursor.rowcount
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
