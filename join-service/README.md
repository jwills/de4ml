# Data Engineering for Machine Learning: A Simple Join Service for Contextual Bandits

In the third session of the course, one of our discussion topics is the paper
[Making Contextual Decisions with Low Technical Debt](https://arxiv.org/abs/1606.03966), which
describes an architecture for a [contextual bandit](https://miguelgfierro.com/blog/2020/a-gentle-introduction-to-contextual-bandits/)
system that is based on the authors' collective experience with building these systems at
Microsoft, discovering a set of common failure patterns, and then designing their system so
as to eliminate the classes of failures that they ran into most often.

This repo provides a simple (but still interesting and educational!) implementation of the
_join service_ described in the paper. Contextual bandits are online, learning systems - they dont just take in requests to make decisions, they take in feedback that alters the decisions they make
in the future. The join service has the responsibility of delaying feedback to a standard time window (10s, in this example). This ensures the system doesnt suffer from bias due to variations in how long it takes for the feedback on the consequences of the action to arrive. It also sets an upper bound after which positive/negative feedback is ignored by the online learning component.

Specifically, the join service needs to implement the following spec:

1. Capture a _decision_ that was made by the contextual bandit service, including
a) the action the bandit decided to take, b) the probability of the chosen action being selected
within the bandit's explore-exploit tradeoff model, and c) the context (a.k.a., features) that
the bandit had access to when it was making this decision.
1. At some point in time after the decision is made, the join service may be notified of a
_reward_ that the bandit received as a result of the specific decision that was made.
1. After receiving a decision, the join service should wait a fixed amount of time (in this example,
10 seconds) for an associated reward to arrive. If a reward for the decision has arrived within
that interval of time, it should be joined to an output record that should include all of the
attributes of the decision (i.e., the action, the probability, and the context) along with the value
of the reward for the agent. If no reward arrives during that window of time, then the join service
should still output the original decision, but set the reward for that decision to 0.

The join service is especially interesting for streaming data processing engines because
it's a use case in which time (specifically, the passage of time between when a decision arrives at
the service and when it needs to be emitted) is integral to the correct function of the system;
the time delay exists to ensure that the learning component of the contextual bandit system is
not misled by decisions that appear to be good at first, but actually have a negative payoff (e.g.,
clickbait headlines for stories in a recommendation service, where a user is likely to click on the
link but then quickly return to the original site.)

## Getting Up and Running

This system is implemented in Python and SQL and depends on the [Materialize](http://materialize.com)
streaming SQL database. The easiest way to run it locally is to use [docker-compose](https://docs.docker.com/compose/install/)
on your system of choice with the command:

```
docker-compose up --build
```

to build the local app and then start the system running on [http://localhost:8080](http://localhost:8080), assuming you
have everything up and running you will receive a JSON payload that says `{"ok": true}` from that endpoint.

If you aren't a fan of Docker and are working on OS X or Linux, you can do a [local installation of Materialize](https://materialize.com/docs/install/)
and run the `materialized` daemon on localhost by running `materialized --workers 1` in a shell. In a separate
shell with a python virtualenv setup, you can then execute `pip install -r requirements.txt` to get the dependencies you need installed and
then execute `bin/run.sh` to start the [FastAPI](https://fastapi.tiangolo.com/)-based service at [http://localhost:8080](http://localhost:8080).

## Understanding the Code

All of the code for the join service is contained in the `app/main.py` file, and it makes for
a reasonably quick read. At the start of the file, we import our dependencies, connect
to an instance of the Materialize streaming database, and create an instance of the `FastAPI` class
for setting up the application itself.

The `init_app` function is annotated with `@app.on_event("startup")` so that it will always run
when the app is launched. For demo purposes, we're going to include all of the logic for creating
the materialized views that power the join service in this method. Our first step is to create
tables that define the `decisions` and `rewards` information that we will be processing in the
join service. (In Materialize, tables are only kept in-memory and do not persist between database
restarts; in a production version of this service, we would use a streaming system like Kafka or
Redpanda to store these events and make them available in Materialize as a [SOURCE](https://materialize.com/docs/sql/create-source/).)

Next, we declare windowed views of the `decisions` and `rewards` which define the period of time
that we will want the records from each source available for joining (the length of this window is defined by the EXP_UNIT_MS environment variable, with a default value of 10000 milliseconds = 10 seconds.)

Then, we define the materialized view that expresses the core logic of the join service as a simple
LEFT JOIN over the `decisions_window` and `rewards_window` views that is evaluated precisely
`EXP_UNIT_MS` milliseconds _after_ the decision record was inserted into the `decisions` table. If
one or more records are in the `rewards_window` for the `key`, then those records will be emitted
with their corresponding `reward` values populated, otherwise a single record for the decision will
be emitted with a `reward` of 0.

Finally, I included a little bit of `asyncio` magic that is defined in the `monitor_joined_decisions`
function so that the app will [TAIL](https://materialize.com/docs/sql/tail/) the contents of the `joined_decisions` materialized view and emit the complete decisions records to stdout as they happen.
In a real system, we would use a [SINK](https://materialize.com/docs/sql/create-sink/) to stream
the joined decisions out of the materialized view and into another topic for consumption downstream
by the *Learn* component of the contextual bandit system.

At the bottom of the file is some simple FastAPI boilerplate to declare the `/log_decision` and
`/log_reward` endpoints for inserting timestamped decisions and rewards into the corresponding
tables and then a basic healthcheck endpoint at `/` for verifying that the app is up and running.

## Trying It Out

Once you have the app and an instance of the Materialize database up and running, you should be
able to navigate to [http://localhost:8080/docs](http://localhost:8080/docs) and try sending records
to the `/log_decision` and `/log_rewards` endpoints with (or without) matching values for the
`key` field- if both the decision and the reward have the same key and happen within 10 seconds of
each other, you should see the joined decision with the corresponding reward emitted to stdout
by the join service app!

## Places To Go Next

This project provides a basic framing of a join service, but there are lots of things that
should be added to it before we consider taking it to production, including wiring up a
streaming engine like Kafka/Redpanda and setting up monitoring with Prometheus for both the join service app and the Materialize database. Additionally, there are at least a couple of bugs to
be fixed and some additional features to be added, which I have listed below- if you're for the
challenge, please fork the repo and make some changes, and if you're taking my data engineering class,
I would be happy to review any PRs that you send my way!

1. In real systems, it is often the case that there will be some business logic that
is allowed to override the decision made by the contextual bandit under certain circumstances.
Decisions that were overridden should _not_ be emitted by the decision service because
they are providing false information to the Learn component about what actually occurred (i.e.,
the reward that is associated with the decision does not correspond to the action that the
bandit took.) Add an `/override_decision` endpoint to the join service which takes in the
`decision_key` of the decision that was overridden and modify the SQL so that the
that the overridden decision is filtered out of the `joined_decisions` materialized view.
1. In the current implementation of the join service, the system will output _multiple_
records for each decision if multiple rewards show up during the delay period. Please
change the definition of the materialized views so that only a single output record
per decision is released, with its reward corresponding to the reward that arrived
_last_ during the delay period (i.e., the reward entry that has the largest value of `insert_ms`.)
Additionally, note that a reward that has an `insert_ms` lower than the decision's
`insert_ms` will still appear in the `joined_decisions` output as long as the reward is available
within the same delay window. Is this the correct behavior? Or should the logic here be adjusted
in some way?
1. See something else you would like to change? Can you come up with a more elegant/efficient
way to implement the join service spec? Send me a PR, I'd love to see what you come up with!