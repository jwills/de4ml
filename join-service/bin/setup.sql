# Basic create table statements for the exercise to define the input schemas;
# this is included here as a convenience if you would like to play with the setup yourself
in MZ using `psql` or another Postgres-compatible SQL client.

CREATE TABLE IF NOT EXISTS decisions (
  key text,
  context text,
  action text,
  probability float,
  insert_ms numeric
);

CREATE TABLE IF NOT EXISTS rewards (
  key text,
  reward float,
  insert_ms numeric
);

DROP VIEW IF EXISTS joined_decisions;

CREATE OR REPLACE MATERIALIZED VIEW decisions_window AS (
  SELECT *
  FROM decisions
  WHERE mz_logical_timestamp() BETWEEN insert_ms AND insert_ms + 10000
);

CREATE OR REPLACE MATERIALIZED VIEW rewards_window AS (
  SELECT *
  FROM rewards
  WHERE mz_logical_timestamp() BETWEEN insert_ms AND insert_ms + 10000
);

CREATE OR REPLACE MATERIALIZED VIEW joined_decisions AS (
  SELECT d.key as key
    , d.context
    , d.action
    , d.probability
    , COALESCE(r.reward, 0.0) as reward
    , d.insert_ms as decision_insert_ms
    , r.insert_ms - d.insert_ms as reward_delta_ms
  FROM decision_windows d
  LEFT JOIN reward_windows r ON d.key = r.key
  WHERE mz_logical_timestamp() >= d.insert_ms + 10000
);