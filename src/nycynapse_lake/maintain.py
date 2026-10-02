"""Housekeeping for the lake: compaction, snapshot expiry, file cleanup, quarantine purge.

Live feeds commit every few minutes, which leaves many small files and many snapshots.
Compaction rewrites small files into larger ones. Old snapshots are expired so the files only
they refer to can be deleted, except snapshots pinned in ops.pinned_snapshots.
"""

import logging

import duckdb

from .control.db import Control
from .lake import LAKE

log = logging.getLogger(__name__)


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
    report["merged"] = con.execute(f"CALL ducklake_merge_adjacent_files('{LAKE}')").fetchall()
    victims = expirable(con, ctl, keep_days)
    if victims:
        con.execute(f"CALL ducklake_expire_snapshots('{LAKE}', versions => ?)", [victims])
    report["expired_snapshots"] = len(victims)
    report["deleted_files"] = len(
        con.execute(f"CALL ducklake_cleanup_old_files('{LAKE}', cleanup_all => true)").fetchall()
    )
    report["quarantine_purged"] = ctl.purge_quarantine(quarantine_days)
    log.info("maintenance: %s", {k: v for k, v in report.items() if k != "merged"})
    return report
