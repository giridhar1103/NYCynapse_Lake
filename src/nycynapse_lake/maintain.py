"""Housekeeping for the lake: compaction, snapshot expiry, file cleanup, quarantine purge.

Live feeds commit every few minutes, which leaves many small files and many snapshots.
Compaction rewrites small files into larger ones. Old snapshots are expired so the files only
they refer to can be deleted, except snapshots pinned in ops.pinned_snapshots.

Files are deleted a week after nothing refers to them any more, never at once: a catalog restored
from a backup of the last week must still find every file it lists.
"""

import contextlib
import logging
import time
from datetime import UTC, datetime, timedelta

import duckdb

from .control.db import Control
from .lake import LAKE

log = logging.getLogger(__name__)

FILE_GRACE_DAYS = 8
DQ_DAYS = 60
RUN_DAYS = 180
# Files are taken deadest first, down to this share of deleted rows, as long as what is still live
# in them is below the limit next to it. The limits keep a rewrite within this job's memory. The
# replaced files stay for weeks, so a rewrite of files that are hardly dead costs more disk than it
# frees and is only for small tables.
REWRITE_STEPS = ((0.95, 400 * 1024**2), (0.5, 400 * 1024**2), (0.05, 20 * 1024**2))
REWRITE_MIN_DELETES = 20
# A live table is written every few minutes, so the next try usually works.
MERGE_TRIES = 3
MERGE_WAIT_S = 20


def expirable(con: duckdb.DuckDBPyConnection, ctl: Control, keep_days: int) -> list[int]:
    pinned = {
        r["snapshot_id"] for r in ctl.conn.execute("SELECT snapshot_id FROM ops.pinned_snapshots")
    }
    current = con.execute(f"SELECT id FROM ducklake_current_snapshot('{LAKE}')").fetchone()[0]
    old = con.execute(
        f"SELECT snapshot_id FROM {LAKE}.snapshots() "
        f"WHERE snapshot_time < now() - INTERVAL {int(keep_days)} DAY ORDER BY snapshot_id"
    ).fetchall()
    return [s for (s,) in old if s not in pinned and s != current]


def run(
    con: duckdb.DuckDBPyConnection, ctl: Control, *, keep_days: int = 30, quarantine_days: int = 30
) -> dict:
    report = {}
    report["rewritten"], errors = rewrite(con)
    report["merged"], skipped = merge(con)
    errors += skipped
    victims = expirable(con, ctl, keep_days)
    if victims:
        con.execute(f"CALL ducklake_expire_snapshots('{LAKE}', versions => ?)", [victims])
    report["expired_snapshots"] = len(victims)
    report["deleted_files"] = len(
        con.execute(
            f"CALL ducklake_cleanup_old_files('{LAKE}', older_than => ?)",
            [datetime.now(UTC) - timedelta(days=FILE_GRACE_DAYS)],
        ).fetchall()
    )
    report["quarantine_purged"] = ctl.purge_quarantine(quarantine_days)
    report["ops_purged"] = ctl.purge_history(dq_days=DQ_DAYS, run_days=RUN_DAYS)
    if errors:
        report["skipped"] = errors
    log.info("maintenance: %s", {k: v for k, v in report.items() if k != "merged"})
    return report


def _tables(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = ? "
        "AND schema_name IN ('silver', 'gold') ORDER BY 1, 2",
        [LAKE],
    ).fetchall()


def merge(con: duckdb.DuckDBPyConnection) -> tuple[list, list[str]]:
    """Merge small files table by table. In one go, a load writing to one table ended the whole
    run, and nothing was expired or cleaned up that day."""
    done, errors = [], []
    for schema, name in _tables(con):
        for attempt in range(1, MERGE_TRIES + 1):
            try:
                done += con.execute(
                    f"CALL ducklake_merge_adjacent_files('{LAKE}', '{name}', schema => '{schema}')"
                ).fetchall()
                break
            except duckdb.TransactionException as exc:  # a write to it got there first
                if attempt == MERGE_TRIES:
                    errors.append(f"merge {schema}.{name}: {str(exc)[:160]}")
                else:
                    time.sleep(MERGE_WAIT_S)
    return done, errors


def rewrite(con: duckdb.DuckDBPyConnection) -> tuple[dict, list[str]]:
    """Rewrite files that are mostly deleted rows. Updates leave a file and a delete file each time,
    and merging alone skips those: one alerts table had 6,331 files for 0.6 MB, and the 311 table
    kept 17 million dead rows next to 11 million live ones."""
    done, errors = {}, []
    for schema, name in _tables(con):
        for threshold, limit in REWRITE_STEPS:
            files, live = _dead_files(con, schema, name, threshold)
            if files < REWRITE_MIN_DELETES or live > limit:
                continue
            try:
                _rewrite_table(con, schema, name, threshold)
                done[name] = done.get(name, 0) + files
            except duckdb.Error as exc:  # a live write got there first; tomorrow is soon enough
                errors.append(f"{schema}.{name}: {str(exc)[:160]}")
                break
    return done, errors


def _dead_files(
    con: duckdb.DuckDBPyConnection, schema: str, name: str, threshold: float
) -> tuple[int, int]:
    """Files with at least this share of deleted rows, and the bytes still live in them."""
    meta = f"__ducklake_metadata_{LAKE}.ducklake"
    files, live = con.execute(
        f"""
        WITH dead AS (
            SELECT data_file_id, sum(delete_count) AS n FROM {meta}.ducklake_delete_file
            WHERE end_snapshot IS NULL GROUP BY 1
        )
        SELECT count(*), COALESCE(sum(f.file_size_bytes * (1 - dead.n / f.record_count)), 0)
        FROM {meta}.ducklake_data_file f
        JOIN dead USING (data_file_id)
        JOIN {meta}.ducklake_table t USING (table_id)
        JOIN {meta}.ducklake_schema s USING (schema_id)
        WHERE f.end_snapshot IS NULL AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL
          AND s.schema_name = ? AND t.table_name = ?
          AND f.record_count > 0 AND dead.n >= ? * f.record_count
        """,
        [schema, name, threshold],
    ).fetchone()
    return files, int(live)


def _rewrite_table(
    con: duckdb.DuckDBPyConnection, schema: str, name: str, threshold: float
) -> None:
    """One rewrite, kept only if the table still has the same rows."""
    count = f"SELECT count(*) FROM {LAKE}.{schema}.{name}"
    con.execute("BEGIN")
    try:
        before = con.execute(count).fetchone()
        con.execute(
            f"CALL ducklake_rewrite_data_files('{LAKE}', '{name}', schema => '{schema}', "
            f"delete_threshold => {threshold})"
        ).fetchall()
        after = con.execute(count).fetchone()
        if before != after:
            raise duckdb.DataError(f"{before[0]} rows became {after[0]}, not kept")
        con.execute("COMMIT")
    except duckdb.Error:
        with contextlib.suppress(duckdb.Error):
            con.execute("ROLLBACK")
        raise
