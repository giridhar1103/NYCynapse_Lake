-- One row per registered source, refreshed from its contract on every run.
CREATE TABLE ops.sources (
    source           text PRIMARY KEY,
    domain           text NOT NULL,
    cadence          text NOT NULL,
    freshness_sla    interval NOT NULL,
    contract_version text NOT NULL,
    description      text,
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ops.runs (
    run_id           uuid PRIMARY KEY,
    source           text NOT NULL,
    started_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    status           text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'skipped')),
    rows_in          bigint NOT NULL DEFAULT 0,
    rows_written     bigint NOT NULL DEFAULT 0,
    rows_quarantined bigint NOT NULL DEFAULT 0,
    snapshot_id      bigint,
    error            text,
    detail           jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX runs_source_started ON ops.runs (source, started_at DESC);

-- Watermarks and cursors. Only advanced after the lake commit succeeds.
CREATE TABLE ops.checkpoints (
    source     text NOT NULL,
    key        text NOT NULL,
    value      jsonb NOT NULL,
    run_id     uuid,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, key)
);

-- Upstream file identity, used for conditional requests and to record where a load came from.
CREATE TABLE ops.upstream_files (
    url            text PRIMARY KEY,
    source         text NOT NULL,
    etag           text,
    last_modified  text,
    sha256         text,
    content_length bigint,
    loaded_run_id  uuid,
    checked_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ops.breakers (
    source      text PRIMARY KEY,
    state       text NOT NULL CHECK (state IN ('closed', 'open', 'half_open')),
    failures    integer NOT NULL DEFAULT 0,
    opened_at   timestamptz,
    retry_after timestamptz,
    last_error  text
);

CREATE TABLE ops.dq_results (
    id          bigserial PRIMARY KEY,
    run_id      uuid NOT NULL,
    source      text NOT NULL,
    table_name  text NOT NULL,
    check_name  text NOT NULL,
    severity    text NOT NULL CHECK (severity IN ('info', 'warn', 'error')),
    passed      boolean NOT NULL,
    observed    double precision,
    threshold   double precision,
    detail      jsonb NOT NULL DEFAULT '{}',
    checked_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX dq_results_table_checked ON ops.dq_results (table_name, checked_at DESC);

-- Rows that failed a contract. Parsed records only, kept for 30 days.
CREATE TABLE ops.quarantine (
    id             bigserial PRIMARY KEY,
    run_id         uuid NOT NULL,
    source         text NOT NULL,
    table_name     text NOT NULL,
    reason         text NOT NULL,
    record         jsonb NOT NULL,
    quarantined_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX quarantine_at ON ops.quarantine (quarantined_at);

-- Periods where a live feed was not captured.
CREATE TABLE ops.feed_gaps (
    source     text NOT NULL,
    gap_start  timestamptz NOT NULL,
    gap_end    timestamptz NOT NULL,
    reason     text NOT NULL,
    PRIMARY KEY (source, gap_start)
);

CREATE TABLE ops.table_freshness (
    table_name      text PRIMARY KEY,
    source          text NOT NULL,
    event_column    text,
    coverage_start  timestamptz,
    coverage_end    timestamptz,
    row_count       bigint,
    last_loaded_at  timestamptz NOT NULL,
    last_run_id     uuid NOT NULL
);
