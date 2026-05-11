"""mssql target sink class, which handles writing streams."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from singer_sdk import metrics
from singer_sdk.connectors.sql import SQLConnector
from singer_sdk.helpers._conformers import replace_leading_digit
from singer_sdk.sinks.sql import SQLSink
from sqlalchemy import Column

from target_mssql.connector import mssqlConnector

if TYPE_CHECKING:
    from singer_sdk.plugin_base import PluginBase


class mssqlSink(SQLSink):
    """mssql target sink class."""

    connector_class = mssqlConnector

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
        connector: SQLConnector | None = None,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        if self._config.get("table_prefix"):
            self.stream_name = self._config.get("table_prefix") + stream_name

    # Copied purely to help with type hints
    @property
    def connector(self) -> mssqlConnector:
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
        full_table_name: str,
        schema: dict,
        records: Iterable[dict[str, Any]],
        is_temp_table: bool = False,
    ) -> int | None:
        """Bulk insert records to an existing destination table.

        Uses multi-row INSERT statements chunked to stay within SQL Server's
        2100-parameter-per-statement limit. TABLOCK enables minimally-logged
        writes into the heap temp table, which is faster than row-by-row logging.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.
            is_temp_table: whether the table is a temp table.
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

        # SQL Server hard limit: 2100 parameters per statement.
        rows_per_stmt = max(1, 2100 // len(col_names))

        # TABLOCK enables minimally-logged bulk inserts into heap tables (temp tables
        # created via SELECT INTO have no clustered index, so they qualify).
        tablock = " WITH (TABLOCK)" if is_temp_table else ""

        total = 0
        with self.connection.begin():
            for offset in range(0, len(insert_records), rows_per_stmt):
                chunk = insert_records[offset : offset + rows_per_stmt]
                placeholders = ", ".join([row_placeholder] * len(chunk))
                sql = f"INSERT INTO {full_table_name}{tablock} ({quoted_cols}) VALUES {placeholders}"  # noqa: S608
                params = tuple(row[col] for row in chunk for col in col_names)
                self.connection.exec_driver_sql(sql, params)
                total += len(chunk)

        with metrics.record_counter(full_table_name) as record_counter:
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
        # First we need to be sure the main table is already created
        conformed_records = (self.conform_record(record) for record in context["records"])

        join_keys = [self.conform_name(key, "column") for key in self.key_properties]
        schema = self.conform_schema(self.schema)

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
            # Create a temp table (Creates from the table above)
            self.logger.info(f"Creating temp table {self.full_table_name}")
            self.connector.create_temp_table_from_table(from_table_name=self.full_table_name)

            db_name, schema_name, table_name = self.parse_full_table_name(self.full_table_name)
            tmp_table_name = f"{schema_name}.#{table_name}" if schema_name else f"#{table_name}"
            # Insert into temp table
            self.bulk_insert_records(
                full_table_name=tmp_table_name,
                schema=schema,
                records=deduped_records,
                is_temp_table=True,
            )
            # Merge data from Temp table to main table
            self.logger.info(f"Merging data from temp table to {self.full_table_name}")
            self.merge_upsert_from_table(
                from_table_name=tmp_table_name,
                to_table_name=self.full_table_name,
                schema=schema,
                join_keys=join_keys,
            )

        else:
            self.bulk_insert_records(
                full_table_name=self.full_table_name,
                schema=schema,
                records=conformed_records,
            )

    def merge_upsert_from_table(
        self,
        from_table_name: str,
        to_table_name: str,
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

        merge_sql = f"""
            MERGE INTO {to_table_name} AS target
            USING {from_table_name} AS temp
            ON {join_condition}
            WHEN MATCHED THEN
                UPDATE SET
                    {update_stmt}
            WHEN NOT MATCHED THEN
                INSERT ({", ".join(quoted_keys.values())})
                VALUES ({", ".join([f"temp.{quoted_key}" for quoted_key in quoted_keys.values()])});
        """  # noqa: S608

        with self.connection.begin():
            self.connection.exec_driver_sql(merge_sql)

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
