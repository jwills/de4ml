import duckdb

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from prometheus_client import Counter
from prometheus_fastapi_instrumentator import Instrumentator

from pydantic import ValidationError

from app import constants, contracts

app = FastAPI()

VALIDATION_COUNTER = Counter(
    constants.VALIDATION_COUNTER,
    "Data quality validation error counter",
    ["loc", "type"],
)


@app.on_event("startup")
def startup():
    """Create the database connection table where we will store the data the API collects."""
    columns = []
    for field in contracts.AgrawalRequest.__fields__.values():
        if field.type_ == float:
            columns.append(f"{field.name} DOUBLE")
        else:
            columns.append(f"{field.name} INT")
    app.db = duckdb.connect(constants.DUCKDB_FILE)
    app.db.execute(f"DROP TABLE IF EXISTS {constants.DATA_TABLE}")
    app.db.execute(f"CREATE TABLE {constants.DATA_TABLE} (" + ", ".join(columns) + ")")


@app.on_event("startup")
def monitor():
    """Common http metric tracking for all routes."""
    Instrumentator().instrument(app).expose(app)


@app.on_event("shutdown")
def shutdown():
    """Close the database connection."""
    app.db.close()


@app.get("/")
def is_healthy():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/collect")
async def collect(request: Request):
    """Validate the data from the user before it is collected."""
    data = await request.json()
    if data is None:
        return JSONResponse(status_code=400, content={"error": "No data provided"})

    # Parse the raw data into a Pydantic model, handle any validation errors
    try:
        parsed = contracts.AgrawalRequest(**data)
    except ValidationError as e:
        # update the validation error counters
        for error in e.errors():
            VALIDATION_COUNTER.labels(loc=error["loc"][0], type=error["type"]).inc()
        # re-raise the validation error for FastAPI to handle it
        raise e

    # Otherwise, record the data in DuckDB
    app.db.execute(
        "INSERT INTO agrawal VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        parameters=list(parsed.dict().values()),
    )
    return {"status": "ok"}
