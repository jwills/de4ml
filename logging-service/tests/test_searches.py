import os

from fastapi.testclient import TestClient

from app.lib import etl
from app.lib.jsonschema import AppDefs
from app.api import app

client = TestClient(app)


def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"open": False}

    app.store.open(":memory:")

    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"open": True}


def test_searches():
    json_schema = client.get("/openapi.json").json()
    app_defs = AppDefs.from_json_schema(json_schema)
    app.store.open("/tmp/test_searches.db")

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
    assert rows[0]["__ts"] > 0
    assert rows[0]["query_id"] == "123"
    assert rows[0]["raw_query"] == "test"
    assert rows[0]["user"] == {"id": 1}
    assert rows[0]["results"] == [{"document_id": 1, "position": 1, "score": 1.0}]

    # Missing query_id
    bad_search_event = {
        "user": {"id": 1},
        "raw_query": "test",
        "results": [{"document_id": 1, "position": 1, "score": 1.0}],
    }
    response = client.post("/searches", json=bad_search_event)
    assert response.status_code == 422

    app.store.open(":memory:")
    etl.etl("/tmp/test_searches.db", "searches", "./")
    os.remove("/tmp/test_searches.db")
