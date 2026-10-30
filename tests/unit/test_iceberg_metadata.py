from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from catalog_sync.iceberg_metadata import (
    AdlsMetadataLocator,
    S3MetadataLocator,
    StorageMetadataLocator,
    StoragePath,
    _newest,
    parse_table_path,
)

ADLS_PATH = "abfss://tableflow@acct.dfs.core.windows.net/org/env/lkc-1/v1/abc"
S3_PATH = "s3://bucket/org/env/lkc-1/v1/abc/"


def _t(minute: int) -> datetime:
    return datetime(2026, 1, 1, 0, minute, tzinfo=timezone.utc)


def test_parse_adls_table_path():
    assert parse_table_path(ADLS_PATH) == StoragePath("abfss", "acct", "tableflow", "org/env/lkc-1/v1/abc/")


def test_parse_s3_table_path():
    assert parse_table_path(S3_PATH) == StoragePath("s3", "bucket", "", "org/env/lkc-1/v1/abc/")


@pytest.mark.parametrize("path", ["gs://bucket/x", "abfss://acct.dfs.core.windows.net/x", "s3:///x", "nope"])
def test_parse_rejects_unsupported_paths(path):
    with pytest.raises(ValueError):
        parse_table_path(path)


def test_newest_prefers_highest_version_over_mtime():
    candidates = [
        ("t/metadata/00010-a.metadata.json", _t(1)),
        ("t/metadata/00009-b.metadata.json", _t(5)),
        ("t/metadata/snap-1.avro", _t(9)),
    ]
    assert _newest(candidates) == "t/metadata/00010-a.metadata.json"


def test_newest_handles_hadoop_style_names():
    candidates = [("t/metadata/v2.metadata.json", _t(1)), ("t/metadata/v11.metadata.json", _t(1))]
    assert _newest(candidates) == "t/metadata/v11.metadata.json"


def test_newest_returns_none_without_metadata_files():
    assert _newest([("t/metadata/snap-1.avro", _t(1))]) is None
    assert _newest([]) is None


def test_adls_locator_lists_metadata_prefix():
    container = MagicMock()
    container.list_blobs.return_value = [
        SimpleNamespace(name="org/env/lkc-1/v1/abc/metadata/00001-x.metadata.json", last_modified=_t(1)),
        SimpleNamespace(name="org/env/lkc-1/v1/abc/metadata/00002-y.metadata.json", last_modified=_t(2)),
    ]
    locator = AdlsMetadataLocator(credential=object())
    locator._clients[("acct", "tableflow")] = container

    assert locator.latest_metadata_file(ADLS_PATH) == "org/env/lkc-1/v1/abc/metadata/00002-y.metadata.json"
    container.list_blobs.assert_called_once_with(name_starts_with="org/env/lkc-1/v1/abc/metadata/")


def test_s3_locator_paginates():
    client = MagicMock()
    client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "org/env/lkc-1/v1/abc/metadata/00001-x.metadata.json", "LastModified": _t(1)}]},
        {"Contents": [{"Key": "org/env/lkc-1/v1/abc/metadata/00003-z.metadata.json", "LastModified": _t(3)}]},
        {},
    ]
    locator = S3MetadataLocator(client=client)

    assert locator.latest_metadata_file(S3_PATH) == "org/env/lkc-1/v1/abc/metadata/00003-z.metadata.json"
    client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="bucket", Prefix="org/env/lkc-1/v1/abc/metadata/",
    )


def test_storage_locator_dispatches_by_scheme():
    adls, s3 = MagicMock(), MagicMock()
    adls.latest_metadata_file.return_value = "adls-key"
    s3.latest_metadata_file.return_value = "s3-key"
    locator = StorageMetadataLocator(adls=adls, s3=s3)

    assert locator.latest_metadata_file(ADLS_PATH) == "adls-key"
    assert locator.latest_metadata_file(S3_PATH) == "s3-key"
