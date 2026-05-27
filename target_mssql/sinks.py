"""mssql target sink class, which handles writing streams."""

from __future__ import annotations

import json
import re
import sys
from typing import TYPE_CHECKING, Any

from singer_sdk import metrics
from singer_sdk.helpers._conformers import replace_leading_digit
from singer_sdk.sql import SQLSink
from sqlalchemy import Column

from target_mssql.connector import MSSQLConnector

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.sql.connector import FullyQualifiedName
    from sqlalchemy.engine import Connection


_MAX_PARAM_LIMIT = 2099  # SQL Server rejects exactly 2100 parameters; keep strictly under.


class MSSQLSink(SQLSink[MSSQLConnector]):
    """mssql target sink class."""

    connector_class = MSSQLConnector

    # Copied purely to help with type hints
    @property
    def connector(self) -> MSSQLConnector:
        """The connector object.
        Returns:
            The connector object.
        """
        return self._connector

    @property
    def schema_name(self) -> str | None:
        """Return the schema name or `None` if using names with no schema part.

        Returns:
            The target schema name.
        """

        default_target_schema = self.config.get("default_target_schema", None)
        parts = self.stream_name.split("-")

        if default_target_schema:
            return default_target_schema

        if len(parts) in {2, 3}:
            # Stream name is a two-part or three-part identifier.
            # Use the second-to-last part as the schema name.
            stream_schema = self.conform_name(parts[-2], "schema")

            if stream_schema == "public":
                return "dbo"
            else:
                return stream_schema

        # Schema name not detected.
        return None

    @override
    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Process incoming record and return a modified result.
        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        Returns:
            A new, processed record.
        """
        for key, value in record.items():
            if type(value) in [list, dict]:
                record[key] = json.dumps(value, default=str)
            elif type(value) is str:
                record[key] = value.replace("\x00", "")

        return record

    def bulk_insert_records(
        self,
        connection: Connection,
        full_table_name: str | FullyQualifiedName,
        schema: dict,
        records: Iterable[dict[str, Any]],
    ) -> int | None:
        """Bulk insert records to an existing destination table.

        Uses multi-row INSERT statements chunked to stay within SQL Server's
        2100-parameter-per-statement limit.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.
        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        columns = self.column_representation(schema)
        col_names = [col.name for col in columns]

        insert_records = [{col: record.get(col) for col in col_names} for record in records]

        if not insert_records:
            return 0

        dialect = self.connector._dialect
        quote = dialect.identifier_preparer.quote
        quoted_cols = ", ".join(quote(c) for c in col_names)
        # pymssql uses %s; pyodbc (and most other DBAPIs) use ?
        param_marker = "?" if getattr(dialect.dbapi, "paramstyle", None) == "qmark" else "%s"
        row_placeholder = "(" + ", ".join([param_marker] * len(col_names)) + ")"

        rows_per_stmt = max(1, _MAX_PARAM_LIMIT // len(col_names))

        total = 0
        with connection.begin():
            for offset in range(0, len(insert_records), rows_per_stmt):
                chunk = insert_records[offset : offset + rows_per_stmt]
                placeholders = ", ".join([row_placeholder] * len(chunk))
                sql = f"INSERT INTO {full_table_name} ({quoted_cols}) VALUES {placeholders}"  # noqa: S608
                params = tuple(row[col] for row in chunk for col in col_names)
                connection.exec_driver_sql(sql, params)
                total += len(chunk)

        with metrics.record_counter(str(full_table_name)) as record_counter:
            record_counter.increment(total)

    def column_representation(
        self,
        schema: dict,
    ) -> list[Column]:
        """Returns a sql alchemy table representation for the current schema."""
        columns: list[Column] = []
        conformed_properties = self.conform_schema(schema)["properties"]
        for property_name, property_jsonschema in conformed_properties.items():
            columns.append(
                Column(
                    property_name,
                    self.connector.to_sql_type(property_jsonschema),
                )
            )
        return columns

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.
        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.
        Args:
            context: Stream partition or context dictionary.
        """
        conformed_records = (self.conform_record(record) for record in context["records"])

        join_keys = [self.conform_name(key, "column") for key in self.key_properties]
        schema = self.conform_schema(self.schema)

        with self.connector._engine.connect() as connection:
            if self.key_properties:
                deduped_records = list(
                    {frozenset(record[k] for k in self.key_properties): record for record in conformed_records}.values()
                )

                self.logger.info(f"Preparing table {self.full_table_name}")
                self.connector.prepare_table(
                    full_table_name=self.full_table_name,
                    schema=schema,
                    primary_keys=join_keys,
                    as_temp_table=False,
                )
                self.logger.info(f"Upserting {len(deduped_records)} records into {self.full_table_name}")
                self.merge_upsert_records(
                    connection=connection,
                    full_table_name=self.full_table_name,
                    schema=schema,
                    records=deduped_records,
                    join_keys=join_keys,
                )

            else:
                self.bulk_insert_records(
                    connection=connection,
                    full_table_name=self.full_table_name,
                    schema=schema,
                    records=conformed_records,
                )

    def merge_upsert_records(
        self,
        connection: Connection,
        full_table_name: str | FullyQualifiedName,
        schema: dict,
        records: list[dict[str, Any]],
        join_keys: list[str],
    ) -> None:
        """Upsert records directly using chunked MERGE … USING (VALUES …) AS source(…).

        Avoids writing to tempdb entirely, eliminating page-latch contention that
        occurs under concurrent loads (e.g. Azure SQL with multiple parallel streams).
        Records are chunked to stay within SQL Server's 2100-parameter-per-statement
        limit. All chunks share one transaction.

        Args:
            connection: Active SQLAlchemy connection.
            full_table_name: The destination table name.
            schema: Singer JSON schema for the stream.
            records: Pre-deduplicated records to upsert.
            join_keys: Column names used in the ON clause.
        """
        if not records:
            return

        columns = self.column_representation(schema)
        col_names = [col.name for col in columns]

        dialect = self.connector._dialect
        quote = dialect.identifier_preparer.quote
        quoted = {col: quote(col) for col in col_names}

        param_marker = "?" if getattr(dialect.dbapi, "paramstyle", None) == "qmark" else "%s"
        row_placeholder = "(" + ", ".join([param_marker] * len(col_names)) + ")"
        rows_per_stmt = max(1, _MAX_PARAM_LIMIT // len(col_names))

        join_condition = " AND ".join(f"target.{quoted[k]} = source.{quoted[k]}" for k in join_keys)
        update_cols = [c for c in col_names if c not in join_keys]
        update_stmt = ", ".join(f"target.{quoted[c]} = source.{quoted[c]}" for c in update_cols)
        all_quoted = ", ".join(quoted[c] for c in col_names)
        source_vals = ", ".join(f"source.{quoted[c]}" for c in col_names)

        matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_stmt}" if update_stmt else ""

        total = 0
        with connection.begin():
            for offset in range(0, len(records), rows_per_stmt):
                chunk = records[offset : offset + rows_per_stmt]
                placeholders = ", ".join([row_placeholder] * len(chunk))
                params = tuple(row.get(col) for row in chunk for col in col_names)

                merge_sql = f"""
                    MERGE INTO {full_table_name} AS target
                    USING (VALUES {placeholders}) AS source({all_quoted})
                    ON ({join_condition})
                    {matched_clause}
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT ({all_quoted}) VALUES ({source_vals});
                """  # noqa: S608
                connection.exec_driver_sql(merge_sql, params)
                total += len(chunk)

        with metrics.record_counter(str(full_table_name)) as record_counter:
            record_counter.increment(total)

    def merge_upsert_from_table(
        self,
        connection: Connection,
        from_table_name: str | FullyQualifiedName,
        to_table_name: str | FullyQualifiedName,
        schema: dict,
        join_keys: list[str],
    ) -> int | None:
        """Merge upsert data from one table to another.
        Args:
            from_table_name: The source table name.
            to_table_name: The destination table name.
            join_keys: The merge upsert keys, or `None` to append.
            schema: Singer Schema message.
        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.
        """
        # TODO think about sql injeciton,
        # issue here https://github.com/MeltanoLabs/target-postgres/issues/22
        quoted_keys = {key: self.connector._dialect.identifier_preparer.quote(key) for key in schema["properties"]}

        join_condition = " and ".join([f"temp.{quoted_keys[key]} = target.{quoted_keys[key]}" for key in join_keys])

        update_stmt = ", ".join(
            [
                f"target.{quoted_key} = temp.{quoted_key}"
                for key, quoted_key in quoted_keys.items()
                if key not in join_keys
            ]
        )  # noqa

        matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_stmt}" if update_stmt else ""
        merge_sql = f"""
            MERGE INTO {to_table_name} AS target
            USING {from_table_name} AS temp
            ON {join_condition}
            {matched_clause}
            WHEN NOT MATCHED THEN
                INSERT ({", ".join(quoted_keys.values())})
                VALUES ({", ".join([f"temp.{quoted_key}" for quoted_key in quoted_keys.values()])});
        """  # noqa: S608

        with connection.begin():
            connection.exec_driver_sql(merge_sql)

    def parse_full_table_name(self, full_table_name: str) -> tuple[str | None, str | None, str]:
        """Parse a fully qualified table name into its parts.
        Developers may override this method if their platform does not support the
        traditional 3-part convention: `db_name.schema_name.table_name`
        Args:
            full_table_name: A table name or a fully qualified table name. Depending on
                SQL the platform, this could take the following forms:
                - `<db>.<schema>.<table>` (three part names)
                - `<db>.<table>` (platforms which do not use schema groupings)
                - `<schema>.<name>` (if DB name is already in context)
                - `<table>` (if DB name and schema name are already in context)
        Returns:
            A three part tuple (db_name, schema_name, table_name) with any unspecified
            or unused parts returned as None.
        """
        db_name: str | None = None
        schema_name: str | None = None

        parts = full_table_name.split(".")
        if len(parts) == 1:
            table_name = full_table_name
        if len(parts) == 2:
            schema_name, table_name = parts
        if len(parts) == 3:
            db_name, schema_name, table_name = parts

        return db_name, schema_name, table_name

    def snakecase(self, name):
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
        return name.lower()

    def conform_name(self, name: str, object_type: str | None = None) -> str:
        """Conform a stream property name to one suitable for the target system.
        Transforms names to snake case by default, applicable to most common DBMSs'.
        Developers may override this method to apply custom transformations
        to database/schema/table/column names.
        Args:
            name: Property name.
            object_type: One of ``database``, ``schema``, ``table`` or ``column``.
        Returns:
            The name transformed to snake case.
        """
        # strip non-alphanumeric characters, keeping - . _ and spaces
        name = re.sub(r"[^a-zA-Z0-9_\-\.\s]", "", name)
        # convert to snakecase
        name = self.snakecase(name)
        # replace leading digit
        return replace_leading_digit(name)
