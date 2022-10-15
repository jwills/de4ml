from fastapi.testclient import TestClient

from app.main import app
from app.lib import contracts

client = TestClient(app)


def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_searches():
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


def test_clicks():
    good_click_event = {
        "query_id": "123",
        "document_id": 1,
    }
    response = client.post("/clicks", json=good_click_event)
    assert response.status_code == 200

    # retrieve the search event and make sure it's logged/typed correctly
    response = client.get("/fetch", params={"table": "clicks"})
    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 1
    assert rows[0]["__ts"] > 0
    assert rows[0]["query_id"] == "123"
    assert rows[0]["document_id"] == 1

    # Invalid document id
    bad_click_event = {
        "query_id": "123",
        "document_id": "some invalid string",
    }
    response = client.post("/clicks", json=bad_click_event)
    assert response.status_code == 422
