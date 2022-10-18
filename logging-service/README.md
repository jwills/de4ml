# Data Engineering for Machine Learning: An Example Logging Service

The first concept that we introduce in the course is the need for a _logging service_: a system
that runs alongside the rest of our production services that is dedicated to documenting and
validating data events before they are sent to the rest of our data infrastructure for processing.
The primary goal of this kind of service is to give the data team the same level of control over
the form and structure of the data that they consume that we would give to any other backend
service team. The logging service should be the primary entrypoint for any data that powers
a business-critical use case that involves the data warehouse, because it provides a mechanism
for the data team to define contracts that describe the names, types, and validation rules for
the events that the service ingests and take actions when those rules are violated, such as
causing the integration tests to fail if a new build artifact is sending bad data to the logging
service, or alerting the on-call engineer that a recent deploy has caused some data validation
errors to fire. *The sooner we can find a data validation error, the sooner we can triage and fix it,
and the less impact that it will have on our business.*

There are any number of ways that a logging service could be implemented, depending on what the rest
of the production stack looked like at the company (e.g., the preferred programming language, schema
definition tooling, monitoring/observability libraries, deployment models, etc.). This repo contains
an implementation of a service that solves all of the problems that you need to solve to do it:

1. It defines an API using [FastAPI](https://fastapi.tiangolo.com/) and [Pydantic](https://pydantic-docs.helpmanual.io/)
which specifies the records it knows how to log, what fields those records contain, and any validation
rules for the values of those fields (types, required/optional, and any custom validation rules you
can imagine.) Under the covers, Pydantic uses [JSON Schema](https://json-schema.org/), a tool for
documenting and validating JSON records that has broad cross-language support.
1. It persists the records that are sent to the API to durable storage- in this example, we are using
SQLite.
1. It specifies a mapping between the API schema and the SQL DDL needed to map those records into tables in a data warehouse. Here, we are using [DuckDB](http://duckdb.org) to ETL the JSON data that we persisted in SQLite into
Parquet files for consumption in the data warehouse.
1. It includes a _migration_ tool, so that as we make changes to add new fields or new types of logged
tables, we can evolve our ETL code in a way that ensures that our Parquet files maintain backwards
compatibility.
1. And finally, it includes unit testing to ensure that the data that is written to the API conforms to the validation rules and that we can successfully ETL that data from JSON to Parquet.

## Getting Up and Running

The example here is pure Python and has minimal dependencies, but I always recommend using a
Python virtual environment for any new code. To get started, run:

```
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

...in order to create your virtual environment, activate it, and install the necessary
dependencies for the app. Then you can run `./bin/serve.sh` to start the logging service running
on [http://localhost:8080/](http://localhost:8080); you should get back the JSON payload `{"ok": true}`
if everything has started up correctly.

You can test out logging some simple search events using the [/docs](http://localhost:8080/docs) endpoint
and then using the `/fetch` endpoint to retrive the logged records from SQLite, which will be kept in
a database in your `/tmp/` directory that should contain both your hostname and a timestamp in microseconds of when the service started up. After you are finished logging events, you can shut the service
down (Ctrl-C will do it for you) and then use the `bin/etl.sh` script to transform the JSON records for the `/searches` endpoint that were stored in the SQLite database into Parquet files by running:

```
bin/etl.sh /tmp/<hostname_timestamp>.db searches ./
```

After this command runs, you should see a `searches.parquet` file in the current directory, which
you can read using DuckDB, either with its CLI or in the Python one:

```
import duckdb
conn = duckdb.connect()
conn.load_extension('parquet')
conn.execute("SELECT * FROM 'searches.parquet'").fetchall()
```

## Understanding the Code

1. `app/api.py`: The primary entrypoint for the service, where the API methods are defined
and we do the work of validating the logged events and persisting them for storage.
1. `app/contracts.py`: Defines the Pydantic models that we are using to define our data
contracts as Python classes that can include rich documentation and complex validation
logic for ensuring that the events that we receive always pass certain quality checks
before we persist them to our data infrastructure.
1. `app/migrate.py`: The tool that parses the contracts and keeps track of what records and fields
have been added over time and updates the config files in `app/config` to ensure that we can
ETL the JSON records in a backwards compatible way.
1. `app/etl.py`: The ETL tool that can transform a table from an input SQLite DB file into a
corresponding Parquet file using DuckDB.
1. `app/lib/storage.py`: The wrapper for the storage engine used by the API to persist the logged records.
1. `app/lib/jsonschema.py`: A collection of utilities for working with the JSON Schema files that
are generated by Pydantic.
1. `tests/test_searches.py`: The unit tests, written using pytest and FastAPI's excellent testing libraries, for the example `/searches` records that we are logging.
