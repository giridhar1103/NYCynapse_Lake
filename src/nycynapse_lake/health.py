"""Finds problems the pipeline cannot report about itself, and keeps them as incidents.

A load that fails says so. A load that never starts, a source that answers but has stopped
publishing, a backup that did not happen or a disk filling up say nothing. This runs every few
minutes, compares what is there with what should be, and keeps one open row per problem in
ops.incidents until it is gone. The site's data page reads them.
"""

import logging
import shutil
from dataclasses import dataclass
from datetime import timedelta

from .config import Settings
from .control.db import Control
from .spool import Spool

log = logging.getLogger(__name__)

LIVE = ("subway_realtime", "citibike_live")
FAILING_AFTER = 2  # failed runs in a row
VOLUME_FLOOR = 0.1  # a normal run brings at least this share of its usual rows
DISK_WARN = 15 * 1024**3
DISK_ERROR = 8 * 1024**3
CATALOG_WARN = 20_000  # tables in the catalog, dead ones included
JOB_LIMITS = {
    "backup": timedelta(hours=30),
    "maintain": timedelta(hours=30),
    "verify": timedelta(hours=30),
    "restore-check": timedelta(days=8),
}


@dataclass(frozen=True)
class Problem:
    key: str
    kind: str
    severity: str
    message: str
    source: str | None = None


def _ago(delta: timedelta) -> str:
    hours = delta.total_seconds() / 3600
    return (
        f"{hours * 60:.0f} minutes"
        if hours < 2
        else (f"{hours:.0f} hours" if hours < 48 else f"{hours / 24:.0f} days")
    )


def loads(ctl: Control) -> list[Problem]:
    out = []
    rows = ctl.conn.execute(
        """
        SELECT s.source, s.freshness_sla, r.last_ok, now() - r.last_ok AS age, r.streak, r.error
        FROM ops.sources s LEFT JOIN LATERAL (
            SELECT max(finished_at) FILTER (WHERE status = 'succeeded') AS last_ok,
                   count(*) FILTER (WHERE status = 'failed' AND started_at > COALESCE((
                       SELECT max(started_at) FROM ops.runs k
                       WHERE k.source = s.source AND k.status = 'succeeded'), '-infinity')
                   ) AS streak,
                   (array_agg(error ORDER BY started_at DESC)
                       FILTER (WHERE status = 'failed'))[1] AS error
            FROM ops.runs q WHERE q.source = s.source
        ) r ON true
        """
    ).fetchall()
    for r in rows:
        name = r["source"]
        if r["last_ok"] is None or r["age"] > r["freshness_sla"]:
            since = "never" if r["last_ok"] is None else f"for {_ago(r['age'])}"
            out.append(
                Problem(
                    f"late:{name}",
                    "load_late",
                    "error",
                    f"{name} has not loaded {since} (limit {_ago(r['freshness_sla'])})",
                    name,
                )
            )
        if r["streak"] and r["streak"] >= FAILING_AFTER:
            out.append(
                Problem(
                    f"failing:{name}",
                    "load_failing",
                    "error",
                    f"{name} failed {r['streak']} times in a row: {(r['error'] or '')[:200]}",
                    name,
                )
            )
    return out


def data(ctl: Control) -> list[Problem]:
    """Loads that work, from a source that has stopped publishing."""
    rows = ctl.conn.execute(
        "SELECT table_name, source, coverage_end, data_sla, now() - coverage_end AS age "
        "FROM ops.table_freshness WHERE data_sla IS NOT NULL AND coverage_end IS NOT NULL "
        "AND now() - coverage_end > data_sla"
    ).fetchall()
    return [
        Problem(
            f"data:{r['table_name']}",
            "source_late",
            "warn",
            f"{r['table_name'].rsplit('.', 1)[-1]}: the newest record is {_ago(r['age'])} old "
            f"(limit {_ago(r['data_sla'])}). The source may have stopped publishing.",
            r["source"],
        )
        for r in rows
    ]


def volume(ctl: Control) -> list[Problem]:
    """A run that succeeded with far fewer rows than usual."""
    rows = ctl.conn.execute(
        """
        WITH ok AS (
            SELECT source, rows_in, started_at,
                   row_number() OVER (PARTITION BY source ORDER BY started_at DESC) AS n
            FROM ops.runs WHERE status = 'succeeded' AND started_at > now() - interval '30 days'
        )
        SELECT a.source, a.rows_in, b.usual FROM ok a JOIN (
            SELECT source, percentile_cont(0.5) WITHIN GROUP (ORDER BY rows_in) AS usual
            FROM ok WHERE n BETWEEN 2 AND 15 GROUP BY 1 HAVING count(*) >= 7
        ) b USING (source) WHERE a.n = 1 AND b.usual >= 500
        """
    ).fetchall()
    return [
        Problem(
            f"volume:{r['source']}",
            "volume_drop",
            "warn",
            f"{r['source']} brought {r['rows_in']} rows, usually about {r['usual']:.0f}",
            r["source"],
        )
        for r in rows
        # A live poller's first write after a restart is short; its data limit watches it instead.
        if r["source"] not in LIVE and r["rows_in"] < r["usual"] * VOLUME_FLOOR
    ]


def spools(settings: Settings) -> list[Problem]:
    out = []
    for name in LIVE:
        s = Spool(settings.spool_path, name)
        waiting, dead = len(s.pending()), len(s.dead())
        if waiting > 12:
            out.append(
                Problem(
                    f"spool:{name}",
                    "spool_backlog",
                    "error",
                    f"{name}: {waiting} batches are waiting to be written",
                    name,
                )
            )
        if dead:
            out.append(
                Problem(
                    f"dead:{name}",
                    "dead_batches",
                    "warn",
                    f"{name}: {dead} batches were set aside, see dead/ in its spool",
                    name,
                )
            )
    return out


def disk(settings: Settings) -> list[Problem]:
    free = shutil.disk_usage(settings.data_path).free
    if free >= DISK_WARN:
        return []
    return [
        Problem(
            "disk",
            "disk_low",
            "error" if free < DISK_ERROR else "warn",
            f"{free / 1024**3:.0f} GB of disk left",
        )
    ]


def jobs(ctl: Control) -> list[Problem]:
    last = {
        r["job"]: r
        for r in ctl.conn.execute(
            "SELECT DISTINCT ON (job) job, ok, now() - finished_at AS age, detail "
            "FROM ops.jobs ORDER BY job, finished_at DESC"
        )
    }
    out = []
    for job, limit in JOB_LIMITS.items():
        r = last.get(job)
        if r is None or r["age"] > limit:
            when = "has never run" if r is None else f"last ran {_ago(r['age'])} ago"
            out.append(Problem(f"job:{job}", "job_late", "error", f"{job} {when}"))
        elif not r["ok"]:
            out.append(
                Problem(
                    f"job:{job}",
                    "job_failed",
                    "error",
                    f"{job} failed: {str(r['detail'].get('error', ''))[:200]}",
                )
            )
    return out


def catalog(ctl: Control) -> list[Problem]:
    row = ctl.conn.execute(
        "SELECT count(*) AS n FROM pg_class WHERE relkind = 'r' "
        "AND relnamespace = to_regnamespace('ducklake')"
    ).fetchone()
    if row["n"] < CATALOG_WARN:
        return []
    return [
        Problem(
            "catalog",
            "catalog_large",
            "warn",
            f"the catalog holds {row['n']} tables, mostly old versions",
        )
    ]


def record(ctl: Control, found: list[Problem]) -> tuple[list[Problem], list[str]]:
    """Open what is new, touch what is still there, resolve what is gone."""
    opened = []
    for p in found:
        row = ctl.conn.execute(
            """
            INSERT INTO ops.incidents (key, source, kind, severity, message)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (key) WHERE resolved_at IS NULL DO UPDATE SET
                last_seen_at = now(), message = EXCLUDED.message, severity = EXCLUDED.severity
            RETURNING (xmax = 0) AS fresh
            """,
            (p.key, p.source, p.kind, p.severity, p.message),
        ).fetchone()
        if row["fresh"]:
            opened.append(p)
    gone = ctl.conn.execute(
        "UPDATE ops.incidents SET resolved_at = now() "
        "WHERE resolved_at IS NULL AND NOT (key = ANY(%s)) RETURNING key",
        ([p.key for p in found],),
    ).fetchall()
    return opened, [r["key"] for r in gone]


def run(settings: Settings, ctl: Control) -> list[Problem]:
    found: list[Problem] = []
    checks = (
        ("loads", lambda: loads(ctl)),
        ("data", lambda: data(ctl)),
        ("volume", lambda: volume(ctl)),
        ("spools", lambda: spools(settings)),
        ("disk", lambda: disk(settings)),
        ("jobs", lambda: jobs(ctl)),
        ("catalog", lambda: catalog(ctl)),
    )
    for name, check in checks:
        try:
            found += check()
        except Exception as exc:  # noqa: BLE001 - one broken check must not hide the others
            log.exception("health check %s failed", name)
            found.append(Problem(f"check:{name}", "check_failed", "error", f"{name}: {exc}"[:300]))
    opened, resolved = record(ctl, found)
    for p in opened:
        log.warning("opened %s: %s", p.key, p.message)
    for key in resolved:
        log.info("resolved %s", key)
    return found
