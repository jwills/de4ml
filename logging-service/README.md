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
can imagine.)
2. It specifies a mapping between that API and the DDL needed to map those records into tables in a
data warehouse, and rules for how you can evolve the API definitions and the DDL in tandem automatically.
3. It includes unit testing to ensure that the data that is written to the API conforms to the validation
rules and can tell you what is wrong with your input records.

## Getting Up and Running

The example here is pure Python and has minimal dependencies; if you're not comfortable working
with Docker you should be able to get going quickly by creating a Python virtualenv, running
`pip3 install -r requirements.txt`, and then running `./bin/run.sh` to start the logging service on
[http://localhost:8080/](http://localhost:8080); you should get back the JSON payload `{"ok": true}`
if everything has started up correctly.

If you are comfortable with or prefer Docker, you can run:

```
docker build -t logging-svc:dev .
docker run -p 8080:8080 logging-svc:dev
```

in order to get things running locally [http://localhost:8080/](http://localhost:8080).

## Understanding the Code


1. `app/main.py`: The primary entrypoint for the service, where the API methods are defined
and we do the work of validating the logged events and persisting them for storage.
2. `app/lib/storage.py`: Defines the configuration and logic for persisting the records into
a database- for demo purposes, we're simply inserting the denormalized records into [DuckDB](http://duckdb.org)
running in-process and persisting the records to a file on disk so that they can be exported as
Parquet files later on by a cronjob.
3. `app/lib/contracts.py`: Defines the Pydantic models that we are using to define our data
contracts as Python classes that can include rich documentation and complex validation
logic for ensuring that the events that we receive always pass certain quality checks
before we persist them to our data infrastructure.
4. `migrate.py`: The tool that parses the APIs defined by the service and generates the DDL
(including both CREATE TABLE and ALTER TABLE statements) necessary for DuckDB to ingest records;
the generated DDL lives in `app/config/init.sql`.
5. `test.py`: The unit tests, written using pytest and FastAPI's excellent testing libraries, that
ensures that the records sent to the API are mapped correctly to the tables in DuckDB.

## Trying It Out

You can examine and interact with the event logging endpoints by running the logging
service and then navigating to [http://localhost:8080/docs](http://localhost:8080/docs) to
see what happens when you send a request to log a search or a click (both with valid and
invalid schemas) as well as fetch the logged results by sending SQL queries to the `/sql`
endpoint.
