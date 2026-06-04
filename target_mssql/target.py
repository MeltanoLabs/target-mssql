"""mssql target class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

import sqlalchemy.engine.url
from singer_sdk import Sink
from singer_sdk import typing as th
from singer_sdk.sql import SQLTarget

from target_mssql.connector import MSSQLConnector
from target_mssql.sinks import MSSQLSink

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


class TargetMSSQL(SQLTarget):
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
        th.Property(
            "table_prefix",
            th.StringType,
            description="Prefix to add to table name",
        ),
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
        th.Property(
            "azure_blob_storage",
            th.ObjectType(
                th.Property(
                    "account_name",
                    th.StringType,
                    required=True,
                    description="Azure Storage account name",
                ),
                th.Property(
                    "sas_token",
                    th.StringType,
                    required=True,
                    secret=True,
                    description=(
                        "Shared Access Signature token for the storage account "
                        "(without a leading '?'). Must grant read/write/delete on blobs."
                    ),
                ),
                th.Property(
                    "container",
                    th.StringType,
                    required=True,
                    description="Blob container used as the staging area",
                ),
                th.Property(
                    "path_prefix",
                    th.StringType,
                    default="target-mssql",
                    description="Path prefix (virtual directory) inside the container",
                ),
                th.Property(
                    "credential",
                    th.StringType,
                    required=True,
                ),
                th.Property(
                    "data_source",
                    th.StringType,
                    required=True,
                ),
            ),
            description=(
                "Optional Azure Blob Storage stage for high-performance bulk loading. "
                "When set, each batch is serialised to a JSON file, uploaded to blob "
                "storage, and loaded into SQL Server via OPENROWSET(BULK …). "
                "Requires azure-storage-blob: pip install 'target-mssql[azure]'."
            ),
        ),
    ).to_dict()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._url: str | None = None

    def get_url(self, config: Mapping) -> sqlalchemy.engine.url.URL:
        if sqlalchemy_url := config.get("sqlalchemy_url"):
            return sqlalchemy.engine.url.make_url(sqlalchemy_url)

        driver = config["driver"]
        query = {}
        if driver == "pyodbc":
            odbc_driver = config.get("odbc_driver", "ODBC Driver 17 for SQL Server")
            query["driver"] = odbc_driver
            if config.get("trust_server_certificate"):
                query["TrustServerCertificate"] = "yes"

        return sqlalchemy.engine.url.URL.create(
            drivername=f"mssql+{driver}",
            username=config["username"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"],
            query=query,
        )

    @property
    def url(self) -> str:
        """Generates a SQLAlchemy URL for mssql.

        Args:
            config: The configuration for the connector.
        """
        if self._url is None:
            url = self.get_url(self.config)
            self.logger.info("Using SQLAlchemy driver '%s'", url.drivername)

            self._url = url.render_as_string(hide_password=False)

        return self._url

    @override
    def add_sink(  # ty:ignore[override-of-final-method]
        self,
        stream_name: str,
        schema: dict,
        key_properties: Sequence[str] | None = None,
    ) -> Sink:
        """Create a sink and register it.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            stream_name: Name of the stream.
            schema: Schema of the stream.
            key_properties: Primary key of the stream.

        Returns:
            A new sink for the stream.
        """
        self.logger.debug("Initializing target sink '%s'...", self.name)
        sink = self.create_sink(
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
        )

        try:
            sink.setup()
        except Exception:  # pragma: no cover
            self.logger.error("Error initializing target sink '%s'", self.name)  # noqa: TRY400
            raise

        self._sinks_active[stream_name] = sink
        return sink

    @override
    def add_sqlsink(  # ty:ignore[override-of-final-method]
        self,
        stream_name: str,
        schema: dict,
        key_properties: Sequence[str] | None = None,
    ) -> Sink:
        """Create a sink and register it.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            stream_name: Name of the stream.
            schema: Schema of the stream.
            key_properties: Primary key of the stream.

        Returns:
            A new sink for the stream.
        """
        self.logger.debug("Initializing target sink '%s'...", self.name)
        sink = self.create_sink(
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
        )
        sink.setup()
        self._sinks_active[stream_name] = sink

        return sink

    # @override
    def create_sink(
        self,
        *,
        stream_name: str,
        schema: dict,
        key_properties: Sequence[str] | None = None,
    ) -> MSSQLSink:
        if prefix := self.config.get("table_prefix"):
            stream_name = f"{prefix}{stream_name}"

        return MSSQLSink(
            target=self,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
            connector=MSSQLConnector(self.config, sqlalchemy_url=self.url),
        )


if __name__ == "__main__":
    TargetMSSQL.cli()
