"""mssql target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_mssql.sinks import mssqlSink


class Targetmssql(SQLTarget):
    """Singer target for mssql."""

    name = "target-mssql"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            description="SQLAlchemy connection string",
        ),
        th.Property(
            "username",
            th.StringType,
            description="SQL Server username",
        ),
        th.Property(
            "password",
            th.StringType,
            description="SQL Server password",
        ),
        th.Property(
            "host",
            th.StringType,
            description="SQL Server host",
        ),
        th.Property(
            "port",
            th.StringType,
            default="1433",
            description="SQL Server port",
        ),
        th.Property(
            "database",
            th.StringType,
            description="SQL Server database",
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            description="Default target schema to write to",
        ),
        th.Property("table_prefix", th.StringType, description="Prefix to add to table name"),
        th.Property(
            "prefer_float_over_numeric",
            th.BooleanType,
            description="Use float data type for numbers (otherwise number type is used)",
            default=False,
        ),
        th.Property(
            "driver",
            th.StringType,
            description="The driver to use for the database connection (pymssql or pyodbc)",
            default="pymssql",
            allowed_values=["pymssql", "pyodbc"],
        ),
        th.Property(
            "odbc_driver",
            th.StringType,
            description="The ODBC driver to use when driver=pyodbc (e.g. 'ODBC Driver 18 for SQL Server')",
        ),
        th.Property(
            "trust_server_certificate",
            th.BooleanType,
            description="Trust the server certificate without validation (useful for self-signed certs)",
            default=False,
        ),
    ).to_dict()

    default_sink_class = mssqlSink


if __name__ == "__main__":
    Targetmssql.cli()
