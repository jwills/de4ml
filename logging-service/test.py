from fastapi.testclient import TestClient

from app.main import app
from app.lib import contracts

client = TestClient(app)


def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_search_log():
    good_search_event = {
        "user": {"id": 1},
        "query_id": "123",
        "raw_query": "test",
        "results": [{"document_id": 1, "position": 1, "score": 1.0}],
    }
    response = client.post("/search", json=good_search_event)
    assert response.status_code == 200

    # retrieve the search event and make sure it's logged/typed correctly
    response = client.get("/fetch", params={"log_type": contracts.LogType.SEARCH.value})
    assert response.status_code == 200
    envelopes = response.json()
    assert len(envelopes) == 1
    assert envelopes[0]["timestamp_micros"] > 0
    assert envelopes[0]["log_type"] == contracts.LogType.SEARCH
    assert envelopes[0]["search"] == good_search_event

    # Missing query_id
    bad_search_event = {
        "user": {"id": 1},
        "raw_query": "test",
        "results": [{"document_id": 1, "position": 1, "score": 1.0}],
    }
    response = client.post("/search", json=bad_search_event)
    assert response.status_code == 422


def test_click_log():
    good_click_event = {
        "query_id": "123",
        "document_id": 1,
    }
    response = client.post("/click", json=good_click_event)
    assert response.status_code == 200

    # retrieve the search event and make sure it's logged/typed correctly
    response = client.get("/fetch", params={"log_type": contracts.LogType.CLICK.value})
    assert response.status_code == 200
    envelopes = response.json()
    assert len(envelopes) == 1
    assert envelopes[0]["timestamp_micros"] > 0
    assert envelopes[0]["log_type"] == contracts.LogType.CLICK
    assert envelopes[0]["click"] == good_click_event

    # Invalid document id
    bad_click_event = {
        "query_id": "123",
        "document_id": "some invalid string",
    }
    response = client.post("/click", json=bad_click_event)
    assert response.status_code == 422
