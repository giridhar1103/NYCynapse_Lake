"""DuckDB connections attached to the DuckLake catalog."""

import os
import shutil

import duckdb

from .config import Settings

LAKE = "lake"
SCHEMAS = ("silver", "gold")


def _catalog_uri(settings: Settings) -> str:
    if settings.catalog_override:
        return f"ducklake:{settings.catalog_override}"
    return f"ducklake:postgres:{settings.pg_dsn}"


def connect(settings: Settings, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if not os.environ.get("HOME"):
        # systemd services start without HOME, and DuckDB keeps its extensions under it.
        con.execute("SET home_directory = '/root'")
    settings.tmp_path.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET memory_limit = '{settings.memory_limit}'")
    con.execute(f"SET threads = {int(settings.threads)}")
    con.execute(f"SET temp_directory = '{_spill_dir(settings)}'")
    # Parquet writes do not need to keep input order, and keeping it costs memory.
    con.execute("SET preserve_insertion_order = false")
    # The host runs on Europe/Berlin. Everything in the lake is UTC unless a column says otherwise.
    con.execute("SET TimeZone = 'UTC'")
    for ext in ("ducklake", "postgres", "spatial"):
        _load(con, ext)

    options = ["METADATA_SCHEMA 'ducklake'"]
    if read_only:
        options.append("READ_ONLY")
    else:
        settings.data_path.mkdir(parents=True, exist_ok=True)
        options.append(f"DATA_PATH '{settings.data_path}/'")
    con.execute(f"ATTACH '{_catalog_uri(settings)}' AS {LAKE} ({', '.join(options)})")
    return con


def _spill_dir(settings: Settings):
    """A spill directory of this process's own. DuckDB processes sharing one collide on files."""
    root = settings.tmp_path / "duckdb"
    root.mkdir(parents=True, exist_ok=True)
    for d in root.iterdir():
        if d.is_dir() and d.name.isdigit() and not _alive(int(d.name)):
            shutil.rmtree(d, ignore_errors=True)
    mine = root / str(os.getpid())
    mine.mkdir(exist_ok=True)
    return mine


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _load(con: duckdb.DuckDBPyConnection, ext: str) -> None:
    try:
        con.execute(f"LOAD {ext}")
    except duckdb.Error:
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")


def ensure_schemas(con: duckdb.DuckDBPyConnection) -> None:
    for schema in SCHEMAS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {LAKE}.{schema}")


def current_snapshot(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(f"SELECT id FROM ducklake_current_snapshot('{LAKE}')").fetchone()[0]
