from typing import List
from fastapi import BackgroundTasks, FastAPI

from .lib import contracts, storage

# The backend storage system that we will persist the log records
# to after they are received and validated.
store = storage.get_store()

# The FastAPI app instance
app = FastAPI()


def _write_log(log: contracts.Envelope):
    """A helper method for persisting a log record to storage in a background task."""
    store.write(log)


@app.post("/search")
def log_search(body: contracts.SearchLog, background_tasks: BackgroundTasks):
    """Validates and persists a search log record to permanent storage."""
    envelope = contracts.Envelope(log_type=contracts.LogType.SEARCH, search=body)
    background_tasks.add_task(_write_log, envelope)
    return {"ok": True}


@app.post("/click")
def log_click(body: contracts.ClickLog, background_tasks: BackgroundTasks):
    """Validates and persists a click log record to permanent storage."""
    envelope = contracts.Envelope(log_type=contracts.LogType.CLICK, click=body)
    background_tasks.add_task(_write_log, envelope)
    return {"ok": True}


@app.get("/fetch", response_model=List[contracts.Envelope])
def fetch(start: int = 0, limit: int = 10):
    """Retrieves recently logged entries from the storage engine, useful for debugging."""
    return store.fetch(start, limit)


@app.get("/")
def is_healthy():
    """Basic health check endpoint that indicates the logging service is up and running."""
    return {"ok": True}
