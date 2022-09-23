import asyncio
import os

import psycopg
import pydantic
from fastapi import FastAPI

# Some database setup/config to start us off
DSN = os.getenv("DSN", "postgresql://materialize@127.0.0.1:6875/materialize")
CONN = psycopg.connect(DSN)
CONN.autocommit = True

# This is the main FastAPI app
app = FastAPI()


@app.on_event("startup")
def init_app():
    """At startup, define the MZ data pipeline and start monitoring joined_decisions"""

    # The amount of time we wait before emitting the joined decisions to the learner
    exp_unit_ms = int(os.getenv("EXP_UNIT_MS", 10000))
    if exp_unit_ms <= 0:
        raise ValueError(f"EXP_UNIT_MS must be a positive integer, found {exp_unit_ms}")

    with CONN.cursor() as cur:
        # Declare the decisions table (in a real system, this would be defined as a SOURCE
        # that was backed by a Kafka topic)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS decisions (
                key TEXT NOT NULL,
                context TEXT NOT NULL,
                action TEXT NOT NULL,
                probability FLOAT NOT NULL,
                insert_ms NUMERIC NOT NULL
            )
            """
        )

        # Declare the rewards table (also backed by Kafka in a real system)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rewards (
                key TEXT NOT NULL,
                reward FLOAT NOT NULL,
                insert_ms NUMERIC NOT NULL
            )
            """
        )

        # Declare the overrides table for decisions that had a business logic
        # override applied and should not be learned from
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS overrides (
                key TEXT NOT NULL,
                insert_ms NUMERIC NOT NULL
            )
            """
        )

        # Cleanup the joined_decisions MZ view so we can recreate it and its dependencies
        # on restart (again, just for dev purposes here, you wouldn't do this in prod)
        cur.execute("DROP VIEW IF EXISTS joined_decisions")

        # Define the windowed views of the decisions and rewards data sources;
        # we're only interested in them for a certain amount of time after they
        # are written, so we define a window over the insert_ms column in each source
        cur.execute(
            f"""
            CREATE OR REPLACE MATERIALIZED VIEW decisions_window AS (
                SELECT *
                FROM decisions
                WHERE mz_logical_timestamp() BETWEEN insert_ms AND insert_ms + {exp_unit_ms}
            )
            """
        )
        cur.execute(
            f"""
            CREATE OR REPLACE MATERIALIZED VIEW rewards_window AS (
                SELECT *
                FROM rewards
                WHERE mz_logical_timestamp() BETWEEN insert_ms AND insert_ms + {exp_unit_ms}
            )
            """
        )
        cur.execute(
            f"""
            CREATE OR REPLACE MATERIALIZED VIEW overrides_window AS (
                SELECT *
                FROM overrides
                WHERE mz_logical_timestamp() BETWEEN insert_ms AND insert_ms + {exp_unit_ms}
            )
            """
        )

        # Define a view that does a GROUP BY over the rewards_window materialized view
        # to get the most recently occurring reward for each key if there are multiple
        # rewards in the MZ view
        cur.execute(
            """
            CREATE OR REPLACE VIEW most_recent_reward_in_window AS (
                SELECT key
                , MAX(insert_ms) AS insert_ms
                , list_agg(reward ORDER BY insert_ms DESC)[1] AS reward
                FROM rewards_window
                GROUP BY key
            )
            """
        )
        # Define the joined view of the decisions and their rewards; this a a LEFT JOIN
        # because we want to emit decisions that do not have a corresponding reward after
        # the window has expired; we also do a LEFT JOIN with the overrides table to
        # filter out any decisions that had a business logic override applied
        cur.execute(
            f"""
            CREATE OR REPLACE MATERIALIZED VIEW joined_decisions AS (
                SELECT d.key as key
                , d.context
                , d.action
                , d.probability
                , COALESCE(r.reward, 0.0) as reward
                , d.insert_ms as decision_insert_ms
                , r.insert_ms - d.insert_ms as reward_delta_ms
                FROM decisions_window d
                LEFT JOIN most_recent_reward_in_window r ON d.key = r.key
                LEFT JOIN overrides_window o ON d.key = o.key
                WHERE mz_logical_timestamp() >= d.insert_ms + {exp_unit_ms}
                AND o.key is NULL
            )
            """
        )

        # Create an async loop that tails the joined_decisions view and emits the
        # decisions to stdout; this simulates how the Learner component
        # would consume the decisions from a stream/Kafka topic
        ret = cur.execute("SHOW COLUMNS FROM joined_decisions").fetchall()
        column_names = [r[0] for r in ret]
        loop = asyncio.get_running_loop()
        loop.create_task(monitor_joined_decisions(column_names))


async def monitor_joined_decisions(column_names):
    """
    Monitor the joined_decisions view and emit the decisions to stdout.
    """
    conn = await psycopg.AsyncConnection.connect(DSN)
    cursor = conn.cursor()

    tail_query = "TAIL joined_decisions"
    print("Streaming joined_decisions results to the logger...")
    async for (timestamp, diff, *columns) in cursor.stream(tail_query):
        if diff > 0:
            # We only care about inserts for this exercise
            decision = dict(zip(column_names, columns))
            print(f"Decision Received: {decision} at timestamp {timestamp}")


## The rest of the code is just FastAPI boilerplate to expose the endpoints

# Essentially SQL-injecting myself with this snippet,
# please don't judge me too harshly
INSERT_MS_SQL = "extract(epoch from now()) * 1000"


class Decision(pydantic.BaseModel):
    key: str  # GUID for the decision
    context: str  # JSON/byte encoded string
    action: str  # identifier for the action chosen
    probability: float = pydantic.Field(
        ge=0, le=1
    )  # probability of the chosen action being selected


@app.post("/log_decision")
def log_decision(decision: Decision):
    with CONN.cursor() as cur:
        cur.execute(
            f"INSERT INTO decisions (key, context, action, probability, insert_ms) VALUES (%s, %s, %s, %s, {INSERT_MS_SQL})",
            (decision.key, decision.context, decision.action, decision.probability),
        )
        return {"ok": True}


class Reward(pydantic.BaseModel):
    key: str  # GUID for the decision
    reward: float  # reward value (may be positive/negative)


@app.post("/log_reward")
def log_reward(reward: Reward):
    with CONN.cursor() as cur:
        cur.execute(
            f"INSERT INTO rewards (key, reward, insert_ms) VALUES (%s, %s, {INSERT_MS_SQL})",
            (reward.key, reward.reward),
        )
        return {"ok": True}


@app.post("/log_override")
def log_override(key: str):
    with CONN.cursor() as cur:
        cur.execute(
            f"INSERT INTO overrides (key, insert_ms) VALUES (%s, {INSERT_MS_SQL})",
            (key,),
        )
        return {"ok": True}


@app.get("/")
def is_healthy():
    """Simple health check endpoint"""
    return {"ok": True}
