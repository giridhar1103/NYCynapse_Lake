"""Housekeeping for the lake: compaction, snapshot expiry, file cleanup, quarantine purge.

Live feeds commit every few minutes, which leaves many small files and many snapshots.
Compaction rewrites small files into larger ones. Old snapshots are expired so the files only
they refer to can be deleted, except snapshots pinned in ops.pinned_snapshots.

Files are deleted a week after nothing refers to them any more, never at once: a catalog restored
from a backup of the last week must still find every file it lists.
"""

import logging
from datetime import UTC, datetime, timedelta

import duckdb

from .control.db import Control
from .lake import LAKE

log = logging.getLogger(__name__)

FILE_GRACE_DAYS = 8
DQ_DAYS = 60
RUN_DAYS = 180
# Rewriting reads a whole table, so only tables that fit comfortably in this job's memory.
REWRITE_MAX_BYTES = 400 * 1024**2
REWRITE_MIN_DELETES = 20


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
    report["merged"] = con.execute(f"CALL ducklake_merge_adjacent_files('{LAKE}')").fetchall()
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
        report["rewrite_errors"] = errors
    log.info("maintenance: %s", {k: v for k, v in report.items() if k != "merged"})
    return report


def rewrite(con: duckdb.DuckDBPyConnection) -> tuple[dict, list[str]]:
    """Rewrite tables whose files are mostly deleted rows. Updates leave a file and a delete file
    each time, and merging alone skips those: one alerts table had 6,331 files for 0.6 MB."""
    done, errors = {}, []
    tables = con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = ? "
        "AND schema_name = 'silver' ORDER BY 1",
        [LAKE],
    ).fetchall()
    for (name,) in tables:
        files, size, deletes = con.execute(
            "SELECT count(*), COALESCE(sum(data_file_size_bytes), 0), count(delete_file) "
            f"FROM ducklake_list_files('{LAKE}', '{name}', schema => 'silver')"
        ).fetchone()
        if deletes < REWRITE_MIN_DELETES or size > REWRITE_MAX_BYTES:
            continue
        try:
            con.execute(
                f"CALL ducklake_rewrite_data_files('{LAKE}', '{name}', schema => 'silver', "
                "delete_threshold => 0.05)"
            ).fetchall()
            done[name] = files
        except duckdb.Error as exc:  # a live write got there first; tomorrow is soon enough
            errors.append(f"{name}: {str(exc)[:160]}")
    return done, errors
