"""Find the newest Iceberg metadata file for a Tableflow table in object storage.

Targets that register Iceberg tables from storage (for example Snowflake with
an object-store catalog integration) must point at a specific
``metadata/*.metadata.json`` file. The Tableflow API only returns the table's
root ``table_path``, so the current metadata file has to be found by listing
storage. Listing runs wherever the sync runs, so on private networking it goes
through the storage private endpoint / VPC endpoint, never the public internet.
"""

from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Tableflow writes "00042-<uuid>.metadata.json"; Hadoop-style catalogs write "v42.metadata.json".
_METADATA_VERSION = re.compile(r"^v?(\d+)[-.]")


@dataclass(frozen=True)
class StoragePath:
    """A Tableflow ``table_path`` split into its storage coordinates."""

    scheme: str  # "abfss" or "s3"
    account: str  # ADLS account name, or S3 bucket
    container: str  # ADLS container ("" for S3)
    prefix: str  # path inside the container/bucket, with a trailing "/"


def parse_table_path(table_path: str) -> StoragePath:
    """Parse ``abfss://<container>@<account>.dfs.core.windows.net/<path>`` or ``s3://<bucket>/<path>``."""
    parsed = urlparse(table_path)
    prefix = parsed.path.strip("/")
    prefix = f"{prefix}/" if prefix else ""
    if parsed.scheme in ("abfss", "abfs"):
        container, _, host = parsed.netloc.partition("@")
        if not container or not host:
            raise ValueError(f"Invalid ADLS table path: {table_path!r}")
        return StoragePath("abfss", host.split(".")[0], container, prefix)
    if parsed.scheme in ("s3", "s3a"):
        if not parsed.netloc:
            raise ValueError(f"Invalid S3 table path: {table_path!r}")
        return StoragePath("s3", parsed.netloc, "", prefix)
    raise ValueError(f"Unsupported storage scheme in table path: {table_path!r}")


def _newest(candidates: list[tuple[str, datetime | None]]) -> str | None:
    """Pick the newest metadata file: highest version number, then last modified."""

    def sort_key(item: tuple[str, datetime | None]):
        key, modified = item
        match = _METADATA_VERSION.match(key.rsplit("/", 1)[-1])
        version = int(match.group(1)) if match else -1
        return (version, modified or datetime.min, key)

    files = [c for c in candidates if c[0].endswith(".metadata.json")]
    return max(files, key=sort_key)[0] if files else None


class IcebergMetadataLocator(ABC):
    @abstractmethod
    def latest_metadata_file(self, table_path: str) -> str | None:
        """Return the object key (path inside the container/bucket) of the newest
        ``metadata/*.metadata.json`` under ``table_path``, or None if Tableflow
        has not committed a snapshot yet."""


class AdlsMetadataLocator(IcebergMetadataLocator):
    """Lists ADLS Gen2 through the Blob endpoint (works with HNS accounts).

    Uses ``DefaultAzureCredential`` unless a credential is supplied, so a VM or
    Function managed identity with Storage Blob Data Reader is enough.
    """

    def __init__(self, credential=None) -> None:
        self._credential = credential
        self._clients: dict[tuple[str, str], object] = {}

    def _container(self, account: str, container: str):
        if (account, container) not in self._clients:
            from azure.storage.blob import ContainerClient

            if self._credential is None:
                from azure.identity import DefaultAzureCredential

                self._credential = DefaultAzureCredential()
            self._clients[(account, container)] = ContainerClient(
                account_url=f"https://{account}.blob.core.windows.net",
                container_name=container,
                credential=self._credential,
            )
        return self._clients[(account, container)]

    def latest_metadata_file(self, table_path: str) -> str | None:
        path = parse_table_path(table_path)
        client = self._container(path.account, path.container)
        blobs = client.list_blobs(name_starts_with=f"{path.prefix}metadata/")
        return _newest([(b.name, b.last_modified) for b in blobs])


class S3MetadataLocator(IcebergMetadataLocator):
    """Lists S3 with the default boto3 credential chain (instance/Lambda role)."""

    def __init__(self, client=None) -> None:
        self._client = client

    def latest_metadata_file(self, table_path: str) -> str | None:
        path = parse_table_path(table_path)
        if self._client is None:
            import boto3

            self._client = boto3.client("s3")
        paginator = self._client.get_paginator("list_objects_v2")
        candidates: list[tuple[str, datetime | None]] = []
        for page in paginator.paginate(Bucket=path.account, Prefix=f"{path.prefix}metadata/"):
            candidates.extend((o["Key"], o.get("LastModified")) for o in page.get("Contents", []))
        return _newest(candidates)


class StorageMetadataLocator(IcebergMetadataLocator):
    """Dispatches to the ADLS or S3 locator based on the table path scheme."""

    def __init__(
        self,
        adls: IcebergMetadataLocator | None = None,
        s3: IcebergMetadataLocator | None = None,
    ) -> None:
        self._adls = adls
        self._s3 = s3

    def latest_metadata_file(self, table_path: str) -> str | None:
        scheme = parse_table_path(table_path).scheme
        if scheme == "abfss":
            self._adls = self._adls or AdlsMetadataLocator()
            return self._adls.latest_metadata_file(table_path)
        self._s3 = self._s3 or S3MetadataLocator()
        return self._s3.latest_metadata_file(table_path)
