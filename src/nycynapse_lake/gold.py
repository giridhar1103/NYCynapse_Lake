"""The gold tables built from taxi and app trips, which are published once a month.

Building them reads hundreds of millions of trips and takes half an hour, so they are left out of
the daily build and built when trips were loaded since the last time, or their SQL changed. After
such a change the hourly table is built for every month, a few months at a time to stay within
memory.
"""

import hashlib
import json
import os
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from .config import Settings
from .control.db import Control

JOB = "trip-tables"
SOURCE = "tlc_trips"
SELECTOR = "trips"
# What the two tables are built from, so a change to any of them counts.
MODELS = (
    "selectors.yml",
    "macros/months.sql",
    "macros/time.sql",
    "models/gold/aggregates/agg_trip_times_zone_pair.sql",
    "models/gold/aggregates/agg_trips_zone_hourly.sql",
    "models/gold/facts/fct_rideshare_trip.sql",
    "models/gold/facts/fct_taxi_trip.sql",
)
MAX_AGE = timedelta(days=40)
CHANGED = "the SQL changed"
# The hourly table keeps old months as they are, so a change to its SQL has to reach them too.
HOURLY = "agg_trips_zone_hourly"
FIRST_MONTH = date(2024, 1, 1)
CHUNK_MONTHS = 2  # four at once came to 2.6 GB and were killed
ROOT = Path(__file__).parents[2]


def code(dbt: Path) -> str:
    digest = hashlib.sha256()
    for name in MODELS:
        digest.update((dbt / name).read_bytes())
    return digest.hexdigest()[:16]


def due(ctl: Control, dbt: Path = ROOT / "dbt") -> str | None:
    """Why the tables need building, or None."""
    last = ctl.conn.execute(
        "SELECT finished_at, detail FROM ops.jobs WHERE job = %s AND ok "
        "ORDER BY finished_at DESC LIMIT 1",
        (JOB,),
    ).fetchone()
    if last is None:
        return "never built"
    if last["detail"].get("code") != code(dbt):
        return CHANGED
    # From when the build began: trips loaded while it ran may not be in it.
    since = datetime.fromisoformat(last["detail"]["began"])
    if ctl.conn.execute(
        "SELECT 1 FROM ops.runs WHERE source = %s AND rows_written > 0 "
        "AND (finished_at IS NULL OR finished_at > %s) LIMIT 1",
        (SOURCE, since),
    ).fetchone():
        return "trips were loaded"
    if datetime.now(UTC) - last["finished_at"] > MAX_AGE:
        return "not built for a while"
    return None


def build(settings: Settings, ctl: Control, reason: str, dbt: Path = ROOT / "dbt") -> dict:
    began = datetime.now(UTC)
    _dbt(dbt, "build", "--selector", SELECTOR)
    chunks = months(began.date()) if reason in (CHANGED, "asked for") else []
    for start, end in chunks:
        only = json.dumps({"from_month": start.isoformat(), "to_month": end.isoformat()})
        _dbt(dbt, "run", "--select", HOURLY, "--vars", only)
    return {
        "reason": reason,
        "began": began.isoformat(),
        "code": code(dbt),
        "chunks": len(chunks),
    }


def months(today: date) -> list[tuple[date, date]]:
    """From the first month to the current one in pieces, the newest first."""
    out, end = [], (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    while end > FIRST_MONTH:
        n = end.year * 12 + end.month - 1 - CHUNK_MONTHS
        start = max(FIRST_MONTH, date(n // 12, n % 12 + 1, 1))
        out.append((start, end))
        end = start
    return out


def _dbt(dbt: Path, *args: str) -> None:
    env = {**os.environ, "DBT_PROFILES_DIR": str(dbt)}
    command = [str(ROOT / ".venv-dbt/bin/dbt"), *args]
    done = subprocess.run(command, cwd=dbt, env=env, check=False)  # its log goes to the journal
    if done.returncode:
        raise RuntimeError(f"dbt {' '.join(args[:3])} ended with {done.returncode}")
