from unittest.mock import patch, MagicMock
import pytest
from catalog_sync.sources.confluent_cloud import ConfluentCloudSource


def _source():
    return ConfluentCloudSource(
        api_key="key",
        api_secret="secret",
        cluster_id="lkc-abc123",
        environment_id="env-xyz",
    )


def _api_response(topics, next_link=None):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "data": topics,
        "metadata": {"next": next_link},
    }
    return resp


def _topic(name, location, table_formats=None):
    return {
        "spec": {
            "display_name": name,
            "table_formats": table_formats or ["DELTA"],
            "storage": {
                "table_path": location,
            },
        },
        "status": {
            "phase": "RUNNING",
        },
    }


@patch("catalog_sync.sources.confluent_cloud.requests.get")
def test_list_tables_returns_tableflow_topics(mock_get):
    mock_get.return_value = _api_response([
        _topic("orders", "s3://bucket/warehouse/orders"),
        _topic("customers", "s3://bucket/warehouse/customers"),
    ])

    tables = _source().list_tables()

    assert len(tables) == 2
    assert tables[0].name == "orders"
    assert tables[0].location == "s3://bucket/warehouse/orders"
    assert tables[0].table_format == "DELTA"
    assert tables[1].name == "customers"


@patch("catalog_sync.sources.confluent_cloud.requests.get")
def test_list_tables_iceberg_format(mock_get):
    mock_get.return_value = _api_response([
        _topic("events", "s3://bucket/warehouse/events", table_formats=["ICEBERG"]),
    ])

    tables = _source().list_tables()

    assert len(tables) == 1
    assert tables[0].table_format == "ICEBERG"


@patch("catalog_sync.sources.confluent_cloud.requests.get")
def test_list_tables_empty_response(mock_get):
    mock_get.return_value = _api_response([])

    tables = _source().list_tables()

    assert tables == []


@patch("catalog_sync.sources.confluent_cloud.requests.get")
def test_list_tables_skips_topics_without_storage_location(mock_get):
    topic_no_location = {
        "spec": {"display_name": "no_tableflow", "table_formats": ["DELTA"], "storage": {}},
        "status": {},
    }
    mock_get.return_value = _api_response([topic_no_location])

    tables = _source().list_tables()

    assert tables == []


@patch("catalog_sync.sources.confluent_cloud.requests.get")
def test_list_tables_handles_pagination(mock_get):
    page1 = _api_response(
        [_topic("orders", "s3://b/orders")],
        next_link="https://api.confluent.cloud/tableflow/v1/tableflow-topics?page_token=abc",
    )
    page2 = _api_response([_topic("customers", "s3://b/customers")])

    mock_get.side_effect = [page1, page2]

    tables = _source().list_tables()

    assert len(tables) == 2
    assert mock_get.call_count == 2


@patch("catalog_sync.sources.confluent_cloud.requests.get")
def test_list_tables_raises_on_http_error(mock_get):
    resp = MagicMock()
    resp.raise_for_status.side_effect = Exception("401 Unauthorized")
    mock_get.return_value = resp

    with pytest.raises(Exception, match="401"):
        _source().list_tables()


@patch("catalog_sync.sources.confluent_cloud.requests.get")
def test_list_tables_uses_custom_namespace(mock_get):
    mock_get.return_value = _api_response([
        _topic("orders", "s3://bucket/warehouse/orders"),
    ])

    source = ConfluentCloudSource(
        api_key="key",
        api_secret="secret",
        cluster_id="lkc-abc123",
        environment_id="env-xyz",
        namespace="lkc-5k3v92",
    )
    tables = source.list_tables()

    assert len(tables) == 1
    assert tables[0].namespace == "lkc-5k3v92"


@patch("catalog_sync.sources.confluent_cloud.requests.get")
def test_list_tables_skips_topics_not_running(mock_get):
    """Topics still materializing (phase != RUNNING) should be skipped to
    avoid Databricks writing _delta_log before Tableflow is ready."""
    provisioning_topic = {
        "spec": {
            "display_name": "new_topic",
            "table_formats": ["DELTA"],
            "storage": {"table_path": "s3://bucket/new_topic"},
        },
        "status": {"phase": "PROVISIONING"},
    }
    running_topic = _topic("orders", "s3://bucket/orders")
    mock_get.return_value = _api_response([provisioning_topic, running_topic])

    tables = _source().list_tables()

    assert len(tables) == 1
    assert tables[0].name == "orders"


@patch("catalog_sync.sources.confluent_cloud.requests.get")
def test_list_tables_passes_auth_and_params(mock_get):
    mock_get.return_value = _api_response([])

    _source().list_tables()

    call_args = mock_get.call_args
    assert call_args[1]["auth"] == ("key", "secret")
    assert "lkc-abc123" in call_args[0][0]
    assert "env-xyz" in call_args[0][0]
