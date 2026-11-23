-- How old a table's newest event may be. NULL where the source publishes no event time.
ALTER TABLE ops.table_freshness ADD COLUMN data_sla interval;

-- Problems found by `nyc-lake health`. One open row per problem; resolved when it is gone.
CREATE TABLE ops.incidents (
    id           bigserial PRIMARY KEY,
    key          text NOT NULL,
    source       text,
    kind         text NOT NULL,
    severity     text NOT NULL CHECK (severity IN ('warn', 'error')),
    message      text NOT NULL,
    opened_at    timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    resolved_at  timestamptz
);
CREATE UNIQUE INDEX incidents_open ON ops.incidents (key) WHERE resolved_at IS NULL;
CREATE INDEX incidents_opened ON ops.incidents (opened_at DESC);

-- What maintenance, backup and verification last did.
CREATE TABLE ops.jobs (
    job         text NOT NULL,
    finished_at timestamptz NOT NULL DEFAULT now(),
    ok          boolean NOT NULL,
    detail      jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX jobs_recent ON ops.jobs (job, finished_at DESC);
