"""Tests for governance tag sync — classification tags and business metadata
from Confluent Cloud Stream Catalog to Databricks Unity Catalog."""

from unittest.mock import patch, MagicMock
import pytest

from catalog_sync.models import (
    TableInfo, sanitize_tag_key,
)
from catalog_sync.sources.confluent_cloud import ConfluentCloudSource
from catalog_sync.targets.unity_catalog import UnityCatalogTarget
from catalog_sync.engine import SyncEngine, SyncResult
from catalog_sync.sources.base import CatalogSource
from catalog_sync.targets.base import CatalogTarget


# ── sanitize_tag_key ─────────────────────────────────────────


class TestSanitizeTagKey:
    def test_simple_key_unchanged(self):
        assert sanitize_tag_key("PII") == "PII"

    def test_dots_replaced(self):
        assert sanitize_tag_key("data.owner") == "data_owner"

    def test_dashes_replaced(self):
        assert sanitize_tag_key("data-owner") == "data_owner"

    def test_colons_replaced(self):
        assert sanitize_tag_key("ns:key") == "ns_key"

    def test_equals_replaced(self):
        assert sanitize_tag_key("key=val") == "key_val"

    def test_slashes_replaced(self):
        assert sanitize_tag_key("a/b") == "a_b"

    def test_commas_replaced(self):
        assert sanitize_tag_key("a,b") == "a_b"

    def test_whitespace_replaced(self):
        assert sanitize_tag_key("a b") == "a_b"

    def test_consecutive_invalid_chars_collapsed(self):
        assert sanitize_tag_key("a..b--c") == "a_b_c"

    def test_leading_trailing_stripped(self):
        assert sanitize_tag_key(".leading.") == "leading"

    def test_mixed_invalid_chars(self):
        assert sanitize_tag_key("DataOwnership_owner") == "DataOwnership_owner"

    def test_underscore_preserved(self):
        assert sanitize_tag_key("my_tag") == "my_tag"


# ── Tag fetching from Confluent Cloud ────────────────────────


def _source_with_tags():
    return ConfluentCloudSource(
        api_key="key",
        api_secret="secret",
        cluster_id="lkc-abc123",
        environment_id="env-xyz",
        schema_registry_url="https://psrc-test.us-east-1.aws.confluent.cloud",
        schema_registry_api_key="sr-key",
        schema_registry_api_secret="sr-secret",
        sync_tags=True,
    )


def _source_without_tags():
    return ConfluentCloudSource(
        api_key="key",
        api_secret="secret",
        cluster_id="lkc-abc123",
        environment_id="env-xyz",
        sync_tags=False,
    )


def _tableflow_response(topics, next_link=None):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "data": topics,
        "metadata": {"next": next_link},
    }
    return resp


def _topic(name, location):
    return {
        "spec": {
            "display_name": name,
            "table_formats": ["DELTA"],
            "storage": {"table_path": location},
        },
        "status": {"phase": "RUNNING"},
    }


def _graphql_response(topics):
    """Mock response for POST /catalog/graphql."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"data": {"kafka_topic": topics}}
    resp.raise_for_status = MagicMock()
    return resp


def _gql_topic(topic_name, cluster_id="lkc-abc123", tags=None,
               business_metadata=None, lsrc_id="lsrc-test1"):
    """Build a GraphQL kafka_topic result."""
    return {
        "qualifiedName": f"{lsrc_id}:{cluster_id}:{topic_name}",
        "tags": tags or [],
        "business_metadata": business_metadata or [],
    }


def _mock_requests(tableflow_resp, graphql_resp):
    """Return (get_side_effect, post_side_effect) to route:
    - requests.get → Tableflow API
    - requests.post → GraphQL API
    """
    graphql_responses = graphql_resp if isinstance(graphql_resp, list) else [graphql_resp]
    gql_idx = [0]

    def get_side_effect(url, **kwargs):
        return tableflow_resp

    def post_side_effect(url, **kwargs):
        idx = gql_idx[0]
        gql_idx[0] += 1
        return graphql_responses[idx]

    return get_side_effect, post_side_effect


class TestTagFetching:
    @patch("catalog_sync.sources.confluent_cloud.requests.post")
    @patch("catalog_sync.sources.confluent_cloud.requests.get")
    def test_fetches_classification_tags(self, mock_get, mock_post):
        get_se, post_se = _mock_requests(
            _tableflow_response([_topic("orders", "s3://b/orders")]),
            _graphql_response([_gql_topic("orders", tags=["PII"])]),
        )
        mock_get.side_effect = get_se
        mock_post.side_effect = post_se

        tables = _source_with_tags().list_tables()

        assert len(tables) == 1
        assert tables[0].tags == {"PII": "true"}

    @patch("catalog_sync.sources.confluent_cloud.requests.post")
    @patch("catalog_sync.sources.confluent_cloud.requests.get")
    def test_fetches_business_metadata(self, mock_get, mock_post):
        get_se, post_se = _mock_requests(
            _tableflow_response([_topic("orders", "s3://b/orders")]),
            _graphql_response([_gql_topic("orders", business_metadata=[
                {"name": "DataOwnership.owner", "value": "payments-team"},
                {"name": "DataOwnership.priority", "value": 1},
            ])]),
        )
        mock_get.side_effect = get_se
        mock_post.side_effect = post_se

        tables = _source_with_tags().list_tables()

        assert tables[0].tags == {
            "DataOwnership_owner": "payments-team",
            "DataOwnership_priority": "1",
        }

    @patch("catalog_sync.sources.confluent_cloud.requests.post")
    @patch("catalog_sync.sources.confluent_cloud.requests.get")
    def test_fetches_both_tags_and_bm(self, mock_get, mock_post):
        get_se, post_se = _mock_requests(
            _tableflow_response([_topic("customers", "s3://b/customers")]),
            _graphql_response([_gql_topic("customers",
                tags=["PII", "Sensitive"],
                business_metadata=[
                    {"name": "DataOwnership.owner", "value": "security-team"},
                ],
            )]),
        )
        mock_get.side_effect = get_se
        mock_post.side_effect = post_se

        tables = _source_with_tags().list_tables()

        assert tables[0].tags == {
            "PII": "true",
            "Sensitive": "true",
            "DataOwnership_owner": "security-team",
        }

    @patch("catalog_sync.sources.confluent_cloud.requests.post")
    @patch("catalog_sync.sources.confluent_cloud.requests.get")
    def test_skips_null_bm_values(self, mock_get, mock_post):
        get_se, post_se = _mock_requests(
            _tableflow_response([_topic("orders", "s3://b/orders")]),
            _graphql_response([_gql_topic("orders", business_metadata=[
                {"name": "DataOwnership.owner", "value": "team-a"},
                {"name": "DataOwnership.description", "value": None},
            ])]),
        )
        mock_get.side_effect = get_se
        mock_post.side_effect = post_se

        tables = _source_with_tags().list_tables()

        assert "DataOwnership_description" not in tables[0].tags
        assert tables[0].tags == {"DataOwnership_owner": "team-a"}

    @patch("catalog_sync.sources.confluent_cloud.requests.get")
    def test_no_tags_when_sync_disabled(self, mock_get):
        mock_get.return_value = _tableflow_response([
            _topic("orders", "s3://b/orders"),
        ])

        tables = _source_without_tags().list_tables()

        assert tables[0].tags == {}
        assert mock_get.call_count == 1

    @patch("catalog_sync.sources.confluent_cloud.requests.post")
    @patch("catalog_sync.sources.confluent_cloud.requests.get")
    def test_graphql_failure_does_not_block_table_discovery(self, mock_get, mock_post):
        import requests as req

        mock_get.return_value = _tableflow_response([_topic("orders", "s3://b/orders")])
        mock_post.side_effect = req.RequestException("503 Service Unavailable")

        tables = _source_with_tags().list_tables()

        assert len(tables) == 1
        assert tables[0].name == "orders"
        assert tables[0].tags == {}

    @patch("catalog_sync.sources.confluent_cloud.requests.post")
    @patch("catalog_sync.sources.confluent_cloud.requests.get")
    def test_sanitizes_tag_keys(self, mock_get, mock_post):
        get_se, post_se = _mock_requests(
            _tableflow_response([_topic("orders", "s3://b/orders")]),
            _graphql_response([_gql_topic("orders",
                tags=["data.classification"],
                business_metadata=[{"name": "data-quality.score/100", "value": "95"}],
            )]),
        )
        mock_get.side_effect = get_se
        mock_post.side_effect = post_se

        tables = _source_with_tags().list_tables()

        assert "data_classification" in tables[0].tags
        assert "data_quality_score_100" in tables[0].tags

    @patch("catalog_sync.sources.confluent_cloud.requests.post")
    @patch("catalog_sync.sources.confluent_cloud.requests.get")
    def test_multiple_bm_types(self, mock_get, mock_post):
        get_se, post_se = _mock_requests(
            _tableflow_response([_topic("orders", "s3://b/orders")]),
            _graphql_response([_gql_topic("orders", business_metadata=[
                {"name": "DataOwnership.owner", "value": "team-a"},
                {"name": "DataClassification.level", "value": "confidential"},
                {"name": "DataClassification.region", "value": "EU"},
            ])]),
        )
        mock_get.side_effect = get_se
        mock_post.side_effect = post_se

        tables = _source_with_tags().list_tables()

        assert tables[0].tags == {
            "DataOwnership_owner": "team-a",
            "DataClassification_level": "confidential",
            "DataClassification_region": "EU",
        }

    @patch("catalog_sync.sources.confluent_cloud.requests.post")
    @patch("catalog_sync.sources.confluent_cloud.requests.get")
    def test_bm_values_stringified(self, mock_get, mock_post):
        get_se, post_se = _mock_requests(
            _tableflow_response([_topic("orders", "s3://b/orders")]),
            _graphql_response([_gql_topic("orders", business_metadata=[
                {"name": "DataQuality.verified", "value": True},
                {"name": "DataQuality.score", "value": 98.5},
            ])]),
        )
        mock_get.side_effect = get_se
        mock_post.side_effect = post_se

        tables = _source_with_tags().list_tables()

        assert tables[0].tags["DataQuality_verified"] == "True"
        assert tables[0].tags["DataQuality_score"] == "98.5"

    @patch("catalog_sync.sources.confluent_cloud.requests.post")
    @patch("catalog_sync.sources.confluent_cloud.requests.get")
    def test_filters_by_cluster_id(self, mock_get, mock_post):
        get_se, post_se = _mock_requests(
            _tableflow_response([_topic("orders", "s3://b/orders")]),
            _graphql_response([
                _gql_topic("orders", cluster_id="lkc-abc123", tags=["PII"]),
                _gql_topic("orders", cluster_id="lkc-other", tags=["Sensitive"]),
            ]),
        )
        mock_get.side_effect = get_se
        mock_post.side_effect = post_se

        tables = _source_with_tags().list_tables()

        assert tables[0].tags == {"PII": "true"}

    @patch("catalog_sync.sources.confluent_cloud.requests.post")
    @patch("catalog_sync.sources.confluent_cloud.requests.get")
    def test_single_graphql_call_for_all_topics(self, mock_get, mock_post):
        get_se, post_se = _mock_requests(
            _tableflow_response([
                _topic("orders", "s3://b/orders"),
                _topic("customers", "s3://b/customers"),
                _topic("products", "s3://b/products"),
            ]),
            _graphql_response([
                _gql_topic("orders", tags=["PII"]),
                _gql_topic("customers", tags=["Sensitive"]),
            ]),
        )
        mock_get.side_effect = get_se
        mock_post.side_effect = post_se

        tables = _source_with_tags().list_tables()

        # 1 GraphQL POST for all tags (not N per-topic calls)
        assert mock_post.call_count == 1
        assert len(tables) == 3
        assert tables[0].tags == {"PII": "true"}
        assert tables[1].tags == {"Sensitive": "true"}
        assert tables[2].tags == {}

    @patch("catalog_sync.sources.confluent_cloud.requests.post")
    @patch("catalog_sync.sources.confluent_cloud.requests.get")
    def test_paginates_graphql_when_over_500(self, mock_get, mock_post):
        page1 = [_gql_topic(f"topic-{i}") for i in range(500)]
        page2 = [_gql_topic("orders", tags=["PII"])]

        get_se, _ = _mock_requests(
            _tableflow_response([_topic("orders", "s3://b/orders")]),
            _graphql_response([]),  # unused
        )
        mock_get.side_effect = get_se
        mock_post.side_effect = [
            _graphql_response(page1),
            _graphql_response(page2),
        ]

        tables = _source_with_tags().list_tables()

        assert mock_post.call_count == 2
        assert tables[0].tags == {"PII": "true"}


# ── UC Tag Sync (UnityCatalogTarget.sync_tags) ───────────────


def _make_target_with_tags(mock_ws_cls, show_tags_data=None):
    """Helper to create a UC target with configurable table_tags response."""
    mock_ws = MagicMock()
    mock_ws_cls.return_value = mock_ws

    # Default: init SQL calls succeed, table_tags returns provided data
    def execute_side_effect(**kwargs):
        sql = kwargs.get("statement", "")
        result = MagicMock()
        result.status.state.value = "SUCCEEDED"

        if "table_tags" in sql:
            result.result.data_array = show_tags_data or []
        elif "information_schema" in sql:
            result.result.data_array = []
        else:
            result.result = None

        return result

    mock_ws.statement_execution.execute_statement.side_effect = execute_side_effect

    target = UnityCatalogTarget(
        host="https://ws.databricks.com",
        token="dapi123",
        catalog_name="tf_catalog",
    )
    # Track calls after init
    mock_ws.statement_execution.execute_statement.reset_mock()
    mock_ws.statement_execution.execute_statement.side_effect = execute_side_effect

    return target, mock_ws


class TestUCTagSync:
    @patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
    def test_adds_new_tags(self, mock_ws_cls):
        target, mock_ws = _make_target_with_tags(mock_ws_cls, show_tags_data=[])

        table = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
            tags={"PII": "true", "Sensitive": "true"},
        )

        changes = target.sync_tags(table)

        assert changes == 2
        calls = mock_ws.statement_execution.execute_statement.call_args_list
        sqls = [c[1]["statement"] for c in calls]
        assert any("table_tags" in s for s in sqls)
        alter_sql = [s for s in sqls if "SET TAGS" in s]
        assert len(alter_sql) == 1
        assert "'PII' = 'true'" in alter_sql[0]
        assert "'Sensitive' = 'true'" in alter_sql[0]

    @patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
    def test_no_changes_when_tags_match(self, mock_ws_cls):
        existing = [
            ["PII", "true"],
        ]
        target, mock_ws = _make_target_with_tags(mock_ws_cls, show_tags_data=existing)

        table = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
            tags={"PII": "true"},
        )

        changes = target.sync_tags(table)

        assert changes == 0
        calls = mock_ws.statement_execution.execute_statement.call_args_list
        sqls = [c[1]["statement"] for c in calls]
        assert not any("SET TAGS" in s for s in sqls)
        assert not any("UNSET TAGS" in s for s in sqls)

    @patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
    def test_preserves_uc_native_tags(self, mock_ws_cls):
        existing = [
            ["team", "analytics"],
            ["PII", "true"],
        ]
        target, mock_ws = _make_target_with_tags(mock_ws_cls, show_tags_data=existing)

        table = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
            tags={"PII": "true"},
        )

        changes = target.sync_tags(table)

        assert changes == 0
        calls = mock_ws.statement_execution.execute_statement.call_args_list
        sqls = [c[1]["statement"] for c in calls]
        assert not any("SET TAGS" in s for s in sqls)
        assert not any("UNSET TAGS" in s for s in sqls)

    @patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
    def test_updates_changed_tag_value(self, mock_ws_cls):
        existing = [
            ["DataOwnership_owner", "old-team"],
        ]
        target, mock_ws = _make_target_with_tags(mock_ws_cls, show_tags_data=existing)

        table = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
            tags={"DataOwnership_owner": "new-team"},
        )

        changes = target.sync_tags(table)

        assert changes == 1
        calls = mock_ws.statement_execution.execute_statement.call_args_list
        alter_sqls = [c[1]["statement"] for c in calls if "ALTER" in c[1]["statement"]]
        assert "'DataOwnership_owner' = 'new-team'" in alter_sqls[0]

    @patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
    def test_same_key_as_uc_tag_overwrites(self, mock_ws_cls):
        existing = [
            ["PII", "false"],
        ]
        target, mock_ws = _make_target_with_tags(mock_ws_cls, show_tags_data=existing)

        table = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
            tags={"PII": "true"},
        )

        changes = target.sync_tags(table)

        assert changes == 1
        calls = mock_ws.statement_execution.execute_statement.call_args_list
        alter_sqls = [c[1]["statement"] for c in calls if "ALTER" in c[1]["statement"]]
        assert "'PII' = 'true'" in alter_sqls[0]

    @patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
    def test_empty_source_tags_no_changes(self, mock_ws_cls):
        existing = [
            ["PII", "true"],
        ]
        target, mock_ws = _make_target_with_tags(mock_ws_cls, show_tags_data=existing)

        table = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
            tags={},
        )

        changes = target.sync_tags(table)

        assert changes == 0

    @patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
    def test_skips_empty_tag_keys(self, mock_ws_cls):
        target, mock_ws = _make_target_with_tags(mock_ws_cls, show_tags_data=[])

        table = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
            tags={"": "true", "PII": "true"},
        )

        changes = target.sync_tags(table)

        assert changes == 1
        calls = mock_ws.statement_execution.execute_statement.call_args_list
        alter_sqls = [c[1]["statement"] for c in calls if "ALTER" in c[1]["statement"]]
        assert "'PII' = 'true'" in alter_sqls[0]
        assert "'' =" not in alter_sqls[0]

    @patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
    def test_show_tags_failure_still_applies_tags(self, mock_ws_cls):
        mock_ws = MagicMock()
        mock_ws_cls.return_value = mock_ws

        def execute_side_effect(**kwargs):
            sql = kwargs.get("statement", "")
            result = MagicMock()
            result.status.state.value = "SUCCEEDED"
            result.result = None

            if "table_tags" in sql:
                raise RuntimeError("SQL execution failed: table_tags not supported")

            return result

        mock_ws.statement_execution.execute_statement.side_effect = execute_side_effect

        target = UnityCatalogTarget(
            host="https://ws.databricks.com",
            token="dapi123",
            catalog_name="tf_catalog",
        )
        mock_ws.statement_execution.execute_statement.side_effect = execute_side_effect

        table = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
            tags={"PII": "true"},
        )

        changes = target.sync_tags(table)

        assert changes == 1


# ── Engine integration with tags ─────────────────────────────


class FakeSource(CatalogSource):
    def __init__(self, tables):
        self._tables = tables

    def list_tables(self):
        return self._tables


class FakeTarget(CatalogTarget):
    def __init__(self, tables=None):
        self._tables = {t.full_name: t for t in (tables or [])}
        self.registered = []
        self.updated = []
        self.removed = []
        self.tags_synced = []

    def list_tables(self):
        return list(self._tables.values())

    def register_table(self, table):
        self.registered.append(table)

    def update_table(self, table):
        self.updated.append(table)

    def remove_table(self, namespace, name):
        self.removed.append(f"{namespace}.{name}")

    def sync_tags(self, table):
        self.tags_synced.append(table)
        return len(table.tags)


class TestEngineTagSync:
    def test_engine_syncs_tags_for_new_tables(self):
        table = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
            tags={"PII": "true"},
        )
        source = FakeSource([table])
        target = FakeTarget()
        engine = SyncEngine(source, target, sync_tags=True)

        result = engine.sync()

        assert result.added == 1
        assert result.tags_synced == 1
        assert len(target.tags_synced) == 1
        assert target.tags_synced[0].tags == {"PII": "true"}

    def test_engine_syncs_tags_for_existing_tables(self):
        existing = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
        )
        updated = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
            tags={"PII": "true"},
        )
        source = FakeSource([updated])
        target = FakeTarget([existing])
        engine = SyncEngine(source, target, sync_tags=True)

        result = engine.sync()

        assert result.updated == 0  # Location didn't change
        assert result.tags_synced == 1
        assert len(target.tags_synced) == 1

    def test_engine_skips_tags_when_disabled(self):
        table = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
            tags={"PII": "true"},
        )
        source = FakeSource([table])
        target = FakeTarget()
        engine = SyncEngine(source, target, sync_tags=False)

        result = engine.sync()

        assert result.added == 1
        assert result.tags_synced == 0
        assert len(target.tags_synced) == 0

    def test_engine_does_not_sync_tags_for_removed_tables(self):
        table = TableInfo(
            namespace="default", name="stale",
            location="s3://b/stale",
            tags={"PII": "true"},
        )
        source = FakeSource([])
        target = FakeTarget([table])
        engine = SyncEngine(source, target, sync_tags=True)

        result = engine.sync()

        assert result.removed == 1
        assert result.tags_synced == 0
        assert len(target.tags_synced) == 0

    def test_engine_syncs_tags_for_new_table_with_no_tags(self):
        table = TableInfo(
            namespace="default", name="orders",
            location="s3://b/orders",
            tags={},
        )
        source = FakeSource([table])
        target = FakeTarget()
        engine = SyncEngine(source, target, sync_tags=True)

        result = engine.sync()

        assert result.added == 1
        # sync_tags always called for new tables even with empty tags
        # (handles removal of previously-managed tags)
        assert len(target.tags_synced) == 1

    def test_sync_result_includes_tags_synced(self):
        result = SyncResult(added=1, updated=0, removed=0, tags_synced=3)
        assert result.tags_synced == 3
        assert result.total_changes == 1  # tags_synced not in total_changes


# ── Config tests for tag sync ────────────────────────────────


class TestConfigTagSync:
    def test_sync_tags_defaults_true(self, monkeypatch):
        from catalog_sync.config import SyncConfig

        monkeypatch.setenv("SOURCE_TYPE", "confluent_api")
        monkeypatch.setenv("CONFLUENT_API_KEY", "k")
        monkeypatch.setenv("CONFLUENT_API_SECRET", "s")
        monkeypatch.setenv("CONFLUENT_CLUSTER_ID", "lkc-abc")
        monkeypatch.setenv("CONFLUENT_ENVIRONMENT_ID", "env-xyz")
        monkeypatch.setenv("DATABRICKS_HOST", "https://ws.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
        monkeypatch.setenv("TARGET_CATALOG", "tf_catalog")
        monkeypatch.setenv("SCHEMA_REGISTRY_URL", "https://psrc.cloud")
        monkeypatch.setenv("SCHEMA_REGISTRY_API_KEY", "sr-k")
        monkeypatch.setenv("SCHEMA_REGISTRY_API_SECRET", "sr-s")

        config = SyncConfig.from_env()
        assert config.sync_tags is True

    def test_sync_tags_false(self, monkeypatch):
        from catalog_sync.config import SyncConfig

        monkeypatch.setenv("SOURCE_TYPE", "confluent_api")
        monkeypatch.setenv("CONFLUENT_API_KEY", "k")
        monkeypatch.setenv("CONFLUENT_API_SECRET", "s")
        monkeypatch.setenv("CONFLUENT_CLUSTER_ID", "lkc-abc")
        monkeypatch.setenv("CONFLUENT_ENVIRONMENT_ID", "env-xyz")
        monkeypatch.setenv("DATABRICKS_HOST", "https://ws.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
        monkeypatch.setenv("TARGET_CATALOG", "tf_catalog")
        monkeypatch.setenv("SYNC_TAGS", "false")

        config = SyncConfig.from_env()
        assert config.sync_tags is False

    def test_sync_tags_true_requires_sr_credentials(self, monkeypatch):
        from catalog_sync.config import SyncConfig

        monkeypatch.setenv("SOURCE_TYPE", "confluent_api")
        monkeypatch.setenv("CONFLUENT_API_KEY", "k")
        monkeypatch.setenv("CONFLUENT_API_SECRET", "s")
        monkeypatch.setenv("CONFLUENT_CLUSTER_ID", "lkc-abc")
        monkeypatch.setenv("CONFLUENT_ENVIRONMENT_ID", "env-xyz")
        monkeypatch.setenv("DATABRICKS_HOST", "https://ws.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
        monkeypatch.setenv("TARGET_CATALOG", "tf_catalog")
        monkeypatch.setenv("SYNC_TAGS", "true")
        # Clear any SR env vars that may be set in the environment
        monkeypatch.delenv("SCHEMA_REGISTRY_URL", raising=False)
        monkeypatch.delenv("SCHEMA_REGISTRY_API_KEY", raising=False)
        monkeypatch.delenv("SCHEMA_REGISTRY_API_SECRET", raising=False)

        with pytest.raises(ValueError, match="required when SYNC_TAGS is true"):
            SyncConfig.from_env()
