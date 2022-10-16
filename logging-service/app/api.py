from typing import Dict, List

from fastapi import BackgroundTasks, FastAPI

from . import contracts
from .lib.storage import Storage

# The FastAPI app instance
app = FastAPI()
app.store = Storage(["searches", "clicks"])


@app.post("/searches")
def log_search_event(body: contracts.SearchEvent, background_tasks: BackgroundTasks):
    """Validates and persists a search log record to permanent storage."""
    app.store.write("searches", body.json())
    return {"ok": True}


@app.post("/clicks")
def log_click_event(body: contracts.ClickEvent, background_tasks: BackgroundTasks):
    """Validates and persists a click log record to permanent storage."""
    app.store.write("clicks", body.json())
    return {"ok": True}


@app.get("/fetch", response_model=List[Dict])
def fetch(table: str, limit: int = 10):
    """Retrieves recently logged entries from the storage engine, useful for debugging."""
    ret = app.store.fetch(table, limit)
    return ret


@app.get("/")
def is_healthy():
    """Basic health check endpoint that indicates the logging service is up and running."""
    return {"open": app.store.is_open()}
