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
definition tooling, monitoring/observability libraries, deployment models, etc.) The intent of this
example project is to create a simple service that is comprehensible by a data/ML engineer who is
comfortable in Python, but who may not have a ton of experience creating backend services.

## Getting Up and Running

The example here is pure Python and has minimal depenedencies; if you're not comfortable working
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

The logging service is built using the excellent [FastAPI](https://fastapi.tiangolo.com/) and
makes extensive use of [Pydantic](https://pydantic-docs.helpmanual.io/) (which ships with FastAPI)
to implement data validation and documentation. The code itself is defined in three files:


1. `app/main.py`: The primary entrypoint for the service, where the API methods are defined
and we do the work of validating the logged events and persisting them for storage.
1. `app/lib/storage.py`: Defines the `Store` abstract base class, which includes a `write`
method for persisting a logged event after it has been validated and a `fetch` method
that can be used to retrieve recently logged events (which is very helpful during development
and debugging.) The simple example here only keeps the logged events in memory, but we can
create alternative implementations that serialize the events and persist them to a file, or 
Kafka, or a database table.
1. `app/lib/contracts.py`: Defines the Pydantic models that we are using to define our data
contracts as Python classes that can include rich documentation and complex validation
logic for ensuring that the events that we receive always pass certain quality checks
before we persist them to our data infrastructure.

## Trying It Out

You can examine and interact with the event logging endpoints by running the logging
service and then navigating to [http://localhost:8080/docs](http://localhost:8080/docs) to
see what happens when you send a request to log a search or a click (both with valid and
invalid schemas) as well as fetch the logged results to see them wrapped inside of the
`Envelope` structure which includes additional metadata added by the logging service itself.

## Places To Go Next

This project provides a basic framing of a logging service, but there are lots of things that
should be added to it before we take it to production, including:

1. Support for additional storage backends (like files, Kafka, or a database) and the ability to
retry and/or persist logged records to a backup storage system in case the primary system is down,
1. Monitoring (via [Prometheus](https://prometheus.io/) or another service) to capture metrics about
the number of log events sent to each endpoint, the number of validation errors, etc., etc. to power
alerting.
1. Unit tests and the ability to verify that a code change preserves backwards-compatibility with the existing
contract definitions.
