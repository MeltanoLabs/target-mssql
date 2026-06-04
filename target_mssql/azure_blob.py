"""Azure Blob Storage helpers for the target-mssql blob stage."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from azure.storage.blob import BlobClient


@dataclass
class _AzureBlobConfig:
    account_name: str
    container: str
    credential: str
    data_source: str
    sas_token: str = field(repr=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> _AzureBlobConfig:
        return cls(
            data["account_name"],
            data["container"],
            data["credential"],
            data["data_source"],
            data["sas_token"],
        )

    @property
    def account_url(self) -> str:
        return f"https://{self.account_name}.blob.core.windows.net"

    @property
    def blob_location(self) -> str:
        return f"{self.account_url}/{self.container}"


class AzureBlobManager:
    def __init__(self, *, config: _AzureBlobConfig, blob_name: str) -> None:
        self.config = config
        self.blob_name = blob_name
        self.__client: BlobClient | None = None

    @classmethod
    def from_config(cls, *, config: Mapping[str, Any], blob_name: str) -> AzureBlobManager:
        return AzureBlobManager(
            config=_AzureBlobConfig.from_dict(config),
            blob_name=blob_name,
        )

    @property
    def client(self) -> BlobClient:
        if self.__client is None:
            self.__client = self._get_service_client()
        return self.__client

    def _get_service_client(self) -> BlobClient:
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError:
            msg = "azure-storage-blob is required for blob staging. Install with: pip install 'target-mssql[azure]'"
            raise ImportError(msg) from None

        service = BlobServiceClient(account_url=self.config.account_url, credential=self.config.sas_token)
        return service.get_blob_client(container=self.config.container, blob=self.blob_name)

    def upload_file(self, file_path: str | Path) -> None:
        """Upload a local file to Azure Blob Storage, overwriting if it already exists."""
        with open(file_path, "rb") as f:
            self.client.upload_blob(f, overwrite=True)

    def delete_blob(self) -> None:
        """Delete a blob, including any snapshots."""
        self.client.delete_blob(delete_snapshots="include")
