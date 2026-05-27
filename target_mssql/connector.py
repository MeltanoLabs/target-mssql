from __future__ import annotations

import sys
from typing import TYPE_CHECKING, cast

import sqlalchemy
from singer_sdk.helpers._typing import get_datelike_property_type
from singer_sdk.sql import SQLConnector
from sqlalchemy.dialects import mssql

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Sequence

    from singer_sdk.sql.connector import FullyQualifiedName
    from sqlalchemy.engine import Connection


class MSSQLConnector(SQLConnector):
    """The connector for mssql.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = True  # Whether altering column types is supported.
    allow_merge_upsert: bool = True  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    @override
    def create_schema(self, schema_name: str):
        with self._connect() as conn, conn.begin():
            conn.exec_driver_sql(f"CREATE SCHEMA {schema_name}")

    @override
    def create_empty_table(
        self,
        full_table_name: str | FullyQualifiedName,
        schema: dict,
        primary_keys: Sequence[str] | None = None,
        partition_keys: Sequence[str] | None = None,
        as_temp_table: bool = False,
    ) -> None:
        """Create an empty target table.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        if as_temp_table:
            msg = "Temporary tables are not supported"
            raise NotImplementedError(msg)

        _ = partition_keys  # Not supported in generic implementation.

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sqlalchemy.MetaData()
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError:
            msg = f"Schema for '{full_table_name}' does not define properties: {schema}"
            raise RuntimeError(msg) from None
        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys

            columntype = self.to_sql_type(property_jsonschema)

            # In MSSQL, Primary keys can not be more than 900 bytes. Setting at 255
            if isinstance(columntype, sqlalchemy.types.VARCHAR) and is_primary_key:
                columntype = sqlalchemy.types.VARCHAR(255)

            if is_primary_key:
                columns.append(sqlalchemy.Column(property_name, columntype, primary_key=True, autoincrement=False))
            else:
                columns.append(sqlalchemy.Column(property_name, columntype, primary_key=False))

        _ = sqlalchemy.Table(table_name, meta, *columns, schema=schema_name)
        meta.create_all(self._engine)

    def merge_sql_types(self, sql_types):
        current_type, target_type = sql_types

        if isinstance(current_type, sqlalchemy.DateTime) and isinstance(target_type, mssql.DATETIMEOFFSET):
            return target_type

        return super().merge_sql_types(sql_types)

    def _jsonschema_type_check(self, jsonschema_type: dict, type_check: tuple[str]) -> bool:
        """Return True if the jsonschema_type supports the provided type.
        Args:
            jsonschema_type: The type dict.
            type_check: A tuple of type strings to look for.
        Returns:
            True if the schema suports the type.
        """
        if "type" in jsonschema_type:
            if isinstance(jsonschema_type["type"], (list, tuple)):
                for t in jsonschema_type["type"]:
                    if t in type_check:
                        return True
            else:
                if jsonschema_type.get("type") in type_check:
                    return True

        return bool(any(t in type_check for t in jsonschema_type.get("anyOf", ())))

    def to_sql_type(self, jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:  # noqa
        """Convert JSON Schema type to a SQL type.
        Args:
            jsonschema_type: The JSON Schema object.
        Returns:
            The SQL type.
        """
        if self._jsonschema_type_check(jsonschema_type, ("string",)):
            datelike_type = get_datelike_property_type(jsonschema_type)
            if datelike_type:
                if datelike_type == "date-time":
                    return cast("sqlalchemy.types.TypeEngine", mssql.DATETIMEOFFSET())
                if datelike_type in "time":
                    return cast("sqlalchemy.types.TypeEngine", sqlalchemy.types.TIME())
                if datelike_type == "date":
                    return cast("sqlalchemy.types.TypeEngine", sqlalchemy.types.DATE())

            maxlength = jsonschema_type.get("maxLength")
            if maxlength is not None and maxlength > 8000:
                return cast("sqlalchemy.types.TypeEngine", sqlalchemy.types.TEXT())

            return cast("sqlalchemy.types.TypeEngine", sqlalchemy.types.VARCHAR(maxlength))

        if self._jsonschema_type_check(jsonschema_type, ("integer",)):
            return cast("sqlalchemy.types.TypeEngine", sqlalchemy.types.BIGINT())

        if self._jsonschema_type_check(jsonschema_type, ("number",)):
            if self.config.get("prefer_float_over_numeric", False):
                return cast("sqlalchemy.types.TypeEngine", sqlalchemy.types.FLOAT())
            return cast("sqlalchemy.types.TypeEngine", sqlalchemy.types.NUMERIC(38, 16))

        if self._jsonschema_type_check(jsonschema_type, ("boolean",)):
            return cast("sqlalchemy.types.TypeEngine", mssql.VARCHAR(1))

        if self._jsonschema_type_check(jsonschema_type, ("object",)):
            return cast("sqlalchemy.types.TypeEngine", sqlalchemy.types.VARCHAR())

        if self._jsonschema_type_check(jsonschema_type, ("array",)):
            return cast("sqlalchemy.types.TypeEngine", sqlalchemy.types.JSON())

        return cast("sqlalchemy.types.TypeEngine", sqlalchemy.types.VARCHAR())

    def create_temp_table_from_table(
        self,
        connection: Connection,
        from_table_name: str | FullyQualifiedName,
    ) -> None:
        """Temp table from another table.

        The temp table is created on the supplied connection so that subsequent
        INSERT / MERGE statements in the same `process_batch` call (which must
        share the connection because MSSQL `#temp` tables are session-scoped)
        can see it.
        """
        db_name, schema_name, table_name = self.parse_full_table_name(from_table_name)
        full_table_name = f"{schema_name}.{table_name}" if schema_name else f"{table_name}"
        tmp_full_table_name = f"{schema_name}.#{table_name}" if schema_name else f"#{table_name}"

        with connection.begin():
            droptable = f"DROP TABLE IF EXISTS {tmp_full_table_name}"
            connection.exec_driver_sql(droptable)

            ddl = f"""
                SELECT TOP 0 *
                into {tmp_full_table_name}
                FROM {full_table_name}
            """  # noqa: S608

            connection.exec_driver_sql(ddl)

    def get_column_add_ddl(self, table_name, column_name, column_type):
        column = sqlalchemy.Column(column_name, column_type)

        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData())
        table.append_column(column)

        create_column_clause = sqlalchemy.sql.ddl.CreateColumn(column)
        compiled = create_column_clause.compile(self._engine)

        # SELECT statement required to work around
        # "Statement not executed or executed statement has no resultset" error
        # from pymssql
        return sqlalchemy.DDL(
            """
            ALTER TABLE %(table_name)s ADD %(create_column_clause)s
            SELECT 1 AS ok
            """,
            {
                "table_name": table_name,
                "create_column_clause": compiled,
            },
        )

    def get_column_alter_ddl(self, table_name, column_name, column_type):
        # SELECT statement required to work around
        # "Statement not executed or executed statement has no resultset" error
        # from pymssql.
        # Strip collation: MSSQL rejects quoted collation names (e.g. COLLATE "...").
        self.remove_collation(column_type)
        return sqlalchemy.DDL(
            """
            ALTER TABLE %(table_name)s ALTER COLUMN %(column_name)s %(column_type)s
            SELECT 1 AS ok
            """,
            {
                "table_name": table_name,
                "column_name": self._dialect.identifier_preparer.quote(column_name),
                "column_type": column_type,
            },
        )
