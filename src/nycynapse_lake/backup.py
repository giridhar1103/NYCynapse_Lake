"""Backups of what cannot be downloaded again, and the proof that they work.

Nightly: the DuckLake catalog and the control tables (without them the data files are just
files), plus the pollers' state and spool. Weekly: a second copy of the live-captured tables,
which no source can give us again. Everything else can be reloaded from its source.

The catalog keeps a table for every version of every small table of the last month, thousands
of them, and pg_dump locks every table it reads. Only the tables the current version needs are
dumped, all seen through one database snapshot.

`restore_check` loads the newest dump into a scratch database, opens the lake through it and
compares it with the real one. A backup nobody has restored is a hope.
"""

import logging
import os
import shutil
import subprocess
import tarfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from . import lake
from .config import Settings

log = logging.getLogger(__name__)

KEEP_DAILY = 14
KEEP_EXPORTS = 3
EXPORT_EVERY = timedelta(days=7)
SCRATCH = "nycynapse_lake_restore"
# Captured live. Nobody publishes their history.
IRREPLACEABLE = (
    "subway_stop_events",
    "subway_trips",
    "subway_alerts",
    "bike_station_status",
    "weather_alerts",
)

NEEDED = r"""
    SELECT n.nspname || '.' || c.relname
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r' AND (n.nspname = 'ops' OR (n.nspname = 'ducklake' AND (
        c.relname NOT LIKE 'ducklake\_inlined\_%%'
        OR c.relname = 'ducklake_inlined_data_tables'
        OR c.relname IN (
            SELECT i.table_name FROM ducklake.ducklake_inlined_data_tables i
            JOIN ducklake.ducklake_table t USING (table_id) WHERE t.end_snapshot IS NULL)
        OR c.relname IN (
            SELECT 'ducklake_inlined_delete_' || t.table_id FROM ducklake.ducklake_table t
            WHERE t.end_snapshot IS NULL)
    ))) ORDER BY 1
"""


def _env(dsn: str) -> dict:
    """libpq settings by environment, so the password never shows in the process list."""
    names = {
        "host": "PGHOST",
        "port": "PGPORT",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "dbname": "PGDATABASE",
    }
    parts = conninfo_to_dict(dsn)
    return {**os.environ, **{names[k]: str(v) for k, v in parts.items() if k in names and v}}


def _run(cmd: list[str], **kw) -> str:
    done = subprocess.run(cmd, capture_output=True, text=True, check=False, **kw)
    if done.returncode:
        raise RuntimeError(f"{cmd[0]} failed: {done.stderr.strip()[:500]}")
    return done.stdout


def dump_catalog(settings: Settings) -> Path:
    settings.backup_path.mkdir(parents=True, exist_ok=True)
    os.chmod(settings.backup_path, 0o700)
    out = settings.backup_path / f"catalog-{datetime.now(UTC):%Y%m%d-%H%M}.dump"
    part = out.with_suffix(".part")
    with psycopg.connect(settings.pg_dsn, connect_timeout=15) as c:
        c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        snapshot = c.execute("SELECT pg_export_snapshot()").fetchone()[0]
        tables = [r[0] for r in c.execute(NEEDED.replace("%%", "%")).fetchall()]
        args = [f"--table={t}" for t in tables]
        _run(
            [
                "pg_dump",
                "--format=custom",
                "--compress=6",
                "--no-owner",
                "--no-privileges",
                "--strict-names",
                f"--snapshot={snapshot}",
                f"--file={part}",
                *args,
            ],
            env=_env(settings.pg_dsn),
        )
    listed = _run(["pg_restore", "--list", str(part)]).count(" TABLE DATA ")
    if listed != len(tables):
        raise RuntimeError(f"dump holds {listed} of {len(tables)} tables")
    os.chmod(part, 0o600)
    os.replace(part, out)
    return out


def save_state(settings: Settings) -> Path:
    out = settings.backup_path / f"state-{datetime.now(UTC):%Y%m%d-%H%M}.tar.gz"
    with tarfile.open(out, "w:gz") as tar:
        for path in (settings.state_path, settings.spool_path):
            if path.exists():
                tar.add(path, arcname=path.name)
    os.chmod(out, 0o600)
    return out


def export_live(settings: Settings, con) -> Path | None:
    """A second copy of the live-captured tables, once a week."""
    root = settings.backup_path / "exports"
    root.mkdir(parents=True, exist_ok=True)
    done = sorted(d for d in root.iterdir() if d.is_dir() and not d.name.endswith(".part"))
    now = datetime.now(UTC)
    if done and now - datetime.strptime(done[-1].name, "%Y%m%d").replace(tzinfo=UTC) < EXPORT_EVERY:
        return None
    out = root / f"{now:%Y%m%d}"
    part = root / f"{now:%Y%m%d}.part"
    shutil.rmtree(part, ignore_errors=True)
    part.mkdir()
    for table in IRREPLACEABLE:
        known = con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' "
            "AND schema_name = 'silver' AND table_name = ?",
            [table],
        ).fetchone()[0]
        if known:
            con.execute(
                f"COPY lake.silver.{table} TO '{part / table}.parquet' "
                "(FORMAT parquet, COMPRESSION zstd)"
            )
    os.replace(part, out)
    for old in sorted(d for d in root.iterdir() if d.is_dir())[:-KEEP_EXPORTS]:
        shutil.rmtree(old)
    return out


def _prune(root: Path, pattern: str, keep: int) -> int:
    old = sorted(root.glob(pattern))[:-keep]
    for p in old:
        p.unlink()
    return len(old)


def run(settings: Settings) -> dict:
    dump = dump_catalog(settings)
    state = save_state(settings)
    con = lake.connect(settings, read_only=True)
    try:
        export = export_live(settings, con)
    finally:
        con.close()
    for leftover in settings.backup_path.glob("*.part"):
        if leftover.is_file():
            leftover.unlink()
    _prune(settings.backup_path, "catalog-*.dump", KEEP_DAILY)
    _prune(settings.backup_path, "state-*.tar.gz", KEEP_DAILY)
    return {
        "catalog": dump.name,
        "catalog_mb": round(dump.stat().st_size / 1e6, 1),
        "state": state.name,
        "export": export.name if export else None,
    }


# checks ------------------------------------------------------------------------------------


def tables(con) -> list[tuple[str, str]]:
    return con.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'lake' "
        "AND schema_name IN ('silver', 'gold') ORDER BY 1, 2"
    ).fetchall()


def verify(con, *, deep: int = 200) -> dict:
    """Every file the current version needs is on disk with the size the catalog remembers, and
    a random sample can be opened."""
    files, missing, wrong = [], [], []
    for schema, name in tables(con):
        rows = con.execute(
            "SELECT data_file, data_file_size_bytes, delete_file, delete_file_size_bytes "
            f"FROM ducklake_list_files('lake', '{name}', schema => '{schema}')"
        ).fetchall()
        for data_file, size, delete_file, delete_size in rows:
            for path, expected in ((data_file, size), (delete_file, delete_size)):
                if not path:
                    continue
                try:
                    actual = os.stat(path).st_size
                except FileNotFoundError:
                    missing.append(path)
                    continue
                if expected and actual != expected:
                    wrong.append(path)
                files.append(path)
    unreadable = []
    for (path,) in con.execute(
        "SELECT f FROM (SELECT unnest(?) AS f) ORDER BY random() LIMIT ?", [files, deep]
    ).fetchall():
        try:
            con.execute("SELECT num_rows FROM parquet_file_metadata(?)", [path]).fetchall()
        except Exception as exc:  # noqa: BLE001 - any failure to open it is the finding
            unreadable.append(f"{path}: {str(exc)[:80]}")
    bad = missing + wrong + unreadable
    report = {
        "files": len(files),
        "missing": len(missing),
        "wrong_size": len(wrong),
        "unreadable": len(unreadable),
        "sampled": min(deep, len(files)),
    }
    if bad:
        report["examples"] = bad[:5]
        raise RuntimeError(f"the lake is missing data: {report}")
    return report


def restore_check(settings: Settings) -> dict:
    """Restore the newest dump into a scratch database and read the lake through it."""
    dumps = sorted(settings.backup_path.glob("catalog-*.dump"))
    if not dumps:
        raise RuntimeError("there is no backup to check")
    dump = dumps[-1]
    owner = conninfo_to_dict(settings.pg_dsn).get("user")
    admin = ["runuser", "-u", "postgres", "--"]
    _run([*admin, "dropdb", "--if-exists", SCRATCH], cwd="/")
    _run([*admin, "createdb", "-O", str(owner), SCRATCH], cwd="/")
    dsn = make_conninfo(settings.pg_dsn, dbname=SCRATCH)
    try:
        with psycopg.connect(dsn, autocommit=True, connect_timeout=15) as c:
            c.execute("CREATE SCHEMA ducklake")
            c.execute("CREATE SCHEMA ops")
        _run(
            [
                "pg_restore",
                "--no-owner",
                "--no-privileges",
                "--exit-on-error",
                f"--dbname={SCRATCH}",
                str(dump),
            ],
            env=_env(dsn),
        )
        restored = lake.connect(_with_dsn(settings, dsn), read_only=True)
        live = lake.connect(settings, read_only=True)
        try:
            return _compare(restored, live, dump, _then(dsn, settings.pg_dsn))
        finally:
            restored.close()
            live.close()
    finally:
        _run([*admin, "dropdb", "--if-exists", SCRATCH], cwd="/")


def _with_dsn(settings: Settings, dsn: str) -> Settings:
    from dataclasses import replace

    return replace(settings, pg_dsn=dsn, catalog_override=None)


def _then(scratch_dsn: str, live_dsn: str) -> set[tuple[str, str]]:
    """The tables that existed when the dump was taken, as the live catalog remembers it."""
    with psycopg.connect(scratch_dsn, connect_timeout=15) as c:
        at = c.execute("SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot").fetchone()[0]
    with psycopg.connect(live_dsn, connect_timeout=15) as c:
        rows = c.execute(
            "SELECT s.schema_name, t.table_name FROM ducklake.ducklake_table t "
            "JOIN ducklake.ducklake_schema s USING (schema_id) "
            "WHERE t.begin_snapshot <= %s AND (t.end_snapshot IS NULL OR t.end_snapshot > %s) "
            "AND s.schema_name IN ('silver', 'gold')",
            (at, at),
        ).fetchall()
    return set(rows)


def _compare(restored, live, dump: Path, then: set[tuple[str, str]]) -> dict:
    want = then
    got = set(tables(restored))
    lost = sorted(".".join(t) for t in want - got)
    if lost:
        raise RuntimeError(f"{dump.name} restores without {lost[:5]}")
    total, short = 0, []
    for schema, name in sorted(got & want & set(tables(live))):
        q = f"SELECT count(*) FROM lake.{schema}.{name}"
        a, b = restored.execute(q).fetchone()[0], live.execute(q).fetchone()[0]
        total += a
        # The live lake has moved on since the dump, but only a little.
        if b >= 1000 and not 0.9 * b <= a <= 1.1 * b:
            short.append(f"{schema}.{name}: {a} restored, {b} live")
    if short:
        raise RuntimeError(f"{dump.name} restores with other row counts: {short[:5]}")
    files = verify(restored, deep=50)
    return {"dump": dump.name, "tables": len(got), "rows": total, "files": files["files"]}
