"""Azure Blob Storage helpers for the target-mssql blob stage."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def _get_blob_client(account_name: str, sas_token: str, container: str, blob_name: str):
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        msg = "azure-storage-blob is required for blob staging. Install with: pip install 'target-mssql[azure]'"
        raise ImportError(msg) from None

    account_url = f"https://{account_name}.blob.core.windows.net"
    service = BlobServiceClient(account_url=account_url, credential=sas_token)
    return service.get_blob_client(container=container, blob=blob_name)


def upload_file(
    account_name: str,
    sas_token: str,
    container: str,
    blob_name: str,
    file_path: str | Path,
) -> None:
    """Upload a local file to Azure Blob Storage, overwriting if it already exists."""
    client = _get_blob_client(account_name, sas_token, container, blob_name)
    with open(file_path, "rb") as f:
        client.upload_blob(f, overwrite=True)


def delete_blob(
    account_name: str,
    sas_token: str,
    container: str,
    blob_name: str,
) -> None:
    """Delete a blob, including any snapshots."""
    client = _get_blob_client(account_name, sas_token, container, blob_name)
    client.delete_blob(delete_snapshots="include")
