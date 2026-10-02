-- Snapshots that maintenance must never expire, for example the ones an evaluation set runs on.
CREATE TABLE ops.pinned_snapshots (
    snapshot_id bigint PRIMARY KEY,
    reason      text NOT NULL,
    pinned_at   timestamptz NOT NULL DEFAULT now()
);
