import os
from typing import Dict, List, Optional

from fastapi import BackgroundTasks, FastAPI

from .lib import contracts, storage

# The FastAPI app instance
working_dir = os.path.dirname(os.path.abspath(__file__))
store = storage.Storage()
init_sql_file = os.path.join(working_dir, "config", "init.sql")
if os.path.exists(init_sql_file):
    store.intialize(init_sql_file)
app = FastAPI()


@app.post("/searches")
def log_search_event(body: contracts.SearchEvent, background_tasks: BackgroundTasks):
    """Validates and persists a search log record to permanent storage."""
    background_tasks.add_task(store.write, "searches", body.dict())
    return {"ok": True}


@app.post("/clicks")
def log_click_event(body: contracts.ClickEvent, background_tasks: BackgroundTasks):
    """Validates and persists a click log record to permanent storage."""
    background_tasks.add_task(store.write, "clicks", body.dict())
    return {"ok": True}


@app.get("/sql", response_model=List[Dict])
def fetch(q: str):
    """Retrieves recently logged entries from the storage engine, useful for debugging."""
    ret = store.query(q)
    print(ret)
    return ret


@app.get("/")
def is_healthy():
    """Basic health check endpoint that indicates the logging service is up and running."""
    return {"ok": True}
