# Data Engineering for Machine Learning: Data Quality and Monitoring

DataOps, as I define it in the course, is about using DevOps principles and tools (in particular,
performance monitoring systems like logs, metrics, and traces) in order to rapidly and reliably
capture high-quality data in order to support analytics and machine learning use cases. In this
code exercise, we are going to setup a simple data collection service like the one we did in our
first exercise, but we are now going to incorporate metrics monitoring using [Prometheus](https://prometheus.io/)
and automatically construct alerting rules for our data based on data profiles that we construct in
our data warehouse, which in this exercise will again be [DuckDB](https://duckdb.org/).

## Getting Up and Running

The example here is (almost!) pure Python and has minimal dependencies, but I always recommend using a
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

Prometheus is the metrics service that will be monitoring the metrics that our service emits as it runs and alerts
whenever one of those metrics indicates that we have a data quality problem to investigate. We can launch
the Prometheus monitoring service by executing `./bin/monitor.sh`; this will download and launch the Prometheus
Docker container and serve the Promtheus query and alerting dashboard on [http://localhost:9000](http://localhost:9000).
The metrics that the Prometheus service will be tracking on our data collection API can be viewed in text form at
[http://localhost:8080/metrics](http://localhost:8080/metrics).

Our next step is to send some actual data to the data collection API using [Locust](http://locust.io), a Python-based load testing tool. The `locustfile.py` script in the
top-level directory contains a task definition that reads in some synthetic data generated using the [Agrawal data generator](https://riverml.xyz/0.14.0/api/datasets/synth/Agrawal/)
included in the [River](https://riverml.xyz) streaming ML library; if we run `bin/datagen.sh` we will kickoff a Locust process
in our local environment that will start sending data into our collection endpoint and storing it in our local DuckDB database
in `/tmp/agrawal.duckdb`. While the data generator runs, we can refresh the [http://localhost:8080/metrics](http://localhost:8080/metrics) endpoint
on the service to see how the metrics tracked by the Prometheus client library change.

After we have let the data generation process run for a few minutes, we shut it down along with our local server in order to profile
the data that we collected using DuckDB's built in [SUMMARIZE](https://duckdb.org/docs/guides/meta/summarize.html) operator. By running
the `bin/profile.sh` script, we convert the summary statistics into data validation rules in [JSON Schema](https://json-schema.org/) that we
then convert into [Pydantic](https://pydantic-docs.helpmanual.io/) models via [code generation](https://pydantic-docs.helpmanual.io/datamodel_code_generator/)
as well as associated [alerting rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/) for the Prometheus monitoring
service to track and fire when our API encounters data that is outside of the bounds specified in the Pydantic models.

## Understanding The Code

1. `app/api.py`: The primary entrypoint for the FastAPI service, where we define our `/collect` endpoint for gathering data
and the Prometheus counters that we update whenever we detect a data quality validation error.
1. `app/contracts.py`: The module that defines the Pydantic models for our API endpoints and will contain the updated Pydantic models
that include data validation rules based on the profiles we construct from the data we collect.
1. `app/profile.py`: A script to convert a data profile from DuckDB's `SUMMARIZE` operator into a Pydantic model that defines lower
and upper bounds for the numeric fields in our data schema (and thus overwrites `app/contracts.py`) as well as the Prometheus alerting rules
that are defined in `promconfig/data_quality_rules.yml`.
1. `promconfig/prometheus.yml`: The top-level config file for the Prometheus service that tells Prometheus where to find the endpoints that
it will be monitoring, how often it should scrape the `/metrics` endpoint for updates, and where to look for any rules. You can see
more detail on the structure of this config file [here](https://prometheus.io/docs/prometheus/latest/configuration/configuration/).
1. `locustfile.py`: The Locust file that defines how we generate the synthetic data that we send to the data collection API, both to generate
our initial dataset that we use for profiling as well as the additional data we generate that triggers our data quality alerting rules.
