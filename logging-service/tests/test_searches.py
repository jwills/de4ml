import pytest

import duckdb
from fastapi.testclient import TestClient

from app.lib import etl
from app.lib.jsonschema import AppDefs
from app.lib.storage import Storage
from app.api import app


@pytest.fixture
def client():
    with TestClient(app) as client:
        yield client


@pytest.fixture
def storage(mocker, tmp_path):
    test_store = Storage(tmp_path, ["searches"])
    mocker.patch("app.lib.storage.Storage.get", return_value=test_store)
    return test_store


def test_searches(client, storage, tmp_path):
    json_schema = client.get("/openapi.json").json()
    app_defs = AppDefs.from_json_schema(json_schema)

    good_search_event = {
        "user": {"id": 1},
        "query_id": "123",
        "raw_query": "test",
        "results": [{"document_id": 1, "position": 1, "score": 1.0}],
    }
    response = client.post("/searches", json=good_search_event)
    assert response.status_code == 200

    # retrieve the search event and make sure it's logged/typed correctly
    response = client.get("/fetch", params={"table": "searches"})
    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 1
    assert rows[0]["timestamp_micros"] > 0
    assert rows[0]["query_id"] == "123"
    assert rows[0]["raw_query"] == "test"
    assert rows[0]["user"] == {"id": 1}
    assert rows[0]["results"] == [{"document_id": 1, "position": 1, "score": 1.0}]

    # Missing query_id, verify it isn't logged
    bad_search_event = {
        "user": {"id": 1},
        "raw_query": "test",
        "results": [{"document_id": 1, "position": 1, "score": 1.0}],
    }
    response = client.post("/searches", json=bad_search_event)
    assert response.status_code == 422

    # Close up the DB and run the ETL pipeline for the searches table
    output_path = storage.close()
    parquet_file = etl.etl(output_path, "searches", app_defs, tmp_path)

    conn = duckdb.connect()
    conn.install_extension("parquet")
    conn.load_extension("parquet")
    cursor = conn.execute(f"SELECT * FROM '{parquet_file}'")
    ret = cursor.fetchall()
    payload = dict(zip([x[0] for x in cursor.description], ret[0]))
    assert payload["timestamp_micros"] == rows[0]["timestamp_micros"]
    assert payload["query_id"] == "123"
    assert payload["raw_query"] == "test"
    assert payload["user__id"] == 1
    assert payload["results__document_id"] == [1]
    assert payload["results__position"] == [1]
    assert payload["results__score"] == [1.0]
