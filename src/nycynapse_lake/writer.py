"""Idempotent writes into DuckLake silver tables.

Every write runs in one DuckLake transaction, so readers see the whole batch or none of it.
Running the same batch twice leaves the table unchanged, which is what makes retries and
replays after a crash safe.
"""

import json
import time
from dataclasses import dataclass

import duckdb

from .contracts import Table
from .lake import LAKE, current_snapshot

ENVELOPE = (
    ("_source", "VARCHAR"),
    ("_load_id", "VARCHAR"),
    ("_ingested_at", "TIMESTAMPTZ"),
    ("_contract_version", "INTEGER"),
)
CONFLICT_RETRIES = 5


@dataclass(frozen=True)
class Envelope:
    source: str
    load_id: str
    contract_version: int


@dataclass(frozen=True)
class WriteResult:
    rows_written: int
    snapshot_id: int


def ensure_table(con: duckdb.DuckDBPyConnection, table: Table) -> None:
    exists = con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = ? AND schema_name = 'silver' "
        "AND table_name = ?",
        [LAKE, table.name],
    ).fetchone()[0]
    if not exists:
        cols = [f'"{c.name}" {c.type}' for c in table.columns]
        cols += [f"{n} {t}" for n, t in ENVELOPE]
        con.execute(f"CREATE TABLE {table.qualified} ({', '.join(cols)})")
        if table.partition_by:
            keys = ", ".join(table.partition_by)
            con.execute(f"ALTER TABLE {table.qualified} SET PARTITIONED BY ({keys})")
        con.execute(f"COMMENT ON TABLE {table.qualified} IS {_lit(table.description)}")
        return
    _evolve(con, table)


def _evolve(con: duckdb.DuckDBPyConnection, table: Table) -> None:
    """Add columns that a newer contract introduced. Anything else needs a deliberate migration."""
    current = {
        r[0]: r[1]
        for r in con.execute(
            "SELECT column_name, data_type FROM duckdb_columns() WHERE database_name = ? "
            "AND schema_name = 'silver' AND table_name = ?",
            [LAKE, table.name],
        ).fetchall()
    }
    for c in table.columns:
        if c.name not in current:
            if not c.nullable:
                raise RuntimeError(f"{table.name}.{c.name} is new and NOT NULL, add it by hand")
            con.execute(f'ALTER TABLE {table.qualified} ADD COLUMN "{c.name}" {c.type}')
        elif current[c.name] != c.type and not _same_type(current[c.name], c.type):
            raise RuntimeError(
                f"{table.name}.{c.name} is {current[c.name]} in the lake "
                f"but {c.type} in the contract"
            )


def _same_type(a: str, b: str) -> bool:
    aliases = {"TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ", "TEXT": "VARCHAR", "STRING": "VARCHAR"}
    return aliases.get(a, a) == aliases.get(b, b)


def write(
    con: duckdb.DuckDBPyConnection,
    table: Table,
    clean: str,
    env: Envelope,
    *,
    partition_filter: str | None = None,
) -> WriteResult:
    ensure_table(con, table)
    if table.write == "replace_partition":
        if not partition_filter:
            raise ValueError(f"{table.name} writes by partition and needs a partition_filter")
        stray = con.execute(
            f"SELECT count(*) FROM {clean} WHERE NOT ({partition_filter})"
        ).fetchone()[0]
        if stray:
            raise ValueError(f"{stray} rows in this batch fall outside {partition_filter}")

    cols = [c.name for c in table.columns]
    col_list = ", ".join(f'"{c}"' for c in cols)
    env_values = (
        f"{_lit(env.source)} AS _source, {_lit(env.load_id)} AS _load_id, "
        f"now() AS _ingested_at, {int(env.contract_version)} AS _contract_version"
    )
    env_cols = ", ".join(n for n, _ in ENVELOPE)
    source_rel = f"(SELECT {col_list}, {env_values} FROM {clean})"
    statements = _statements(table, source_rel, col_list, env_cols, partition_filter)

    for attempt in range(CONFLICT_RETRIES):
        con.execute("BEGIN")
        try:
            con.execute(
                f"CALL {LAKE}.set_commit_message('nyc-lake', ?, extra_info => ?)",
                [f"{env.source} {table.name}", json.dumps({"load_id": env.load_id})],
            )
            written = 0
            for sql in statements:
                result = con.execute(sql).fetchone()
                if sql.lstrip().startswith(("INSERT", "MERGE")) and result:
                    written += result[0]
            con.execute("COMMIT")
            return WriteResult(written, current_snapshot(con))
        except duckdb.TransactionException:
            _rollback(con)
            if attempt == CONFLICT_RETRIES - 1:
                raise
            time.sleep(0.5 * 2**attempt)
        except Exception:
            _rollback(con)
            raise
    raise AssertionError("unreachable")


def _statements(
    table: Table, source_rel: str, col_list: str, env_cols: str, partition_filter: str | None
) -> list[str]:
    target = table.qualified
    insert = f"INSERT INTO {target} ({col_list}, {env_cols}) SELECT * FROM {source_rel}"
    if table.write == "replace_all":
        return [f"DELETE FROM {target}", insert]
    if table.write == "replace_partition":
        return [f"DELETE FROM {target} WHERE {partition_filter}", insert]

    on = " AND ".join(f't."{k}" = s."{k}"' for k in table.primary_key)
    all_cols = [c.name for c in table.columns] + [n for n, _ in ENVELOPE]
    values = ", ".join(f's."{c}"' for c in all_cols)
    names = ", ".join(f'"{c}"' for c in all_cols)
    merge = f"MERGE INTO {target} AS t USING {source_rel} AS s ON {on} "
    if table.write == "append_new":
        return [merge + f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"]

    updates = ", ".join(f'"{c}" = s."{c}"' for c in all_cols if c not in table.primary_key)
    newer = ""
    if table.version_column:
        v = table.version_column
        newer = f' AND (t."{v}" IS NULL OR s."{v}" > t."{v}")'
    return [
        merge
        + f"WHEN MATCHED{newer} THEN UPDATE SET {updates} "
        + f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"
    ]


def _rollback(con: duckdb.DuckDBPyConnection) -> None:
    try:
        con.execute("ROLLBACK")
    except duckdb.Error:
        pass  # a failed COMMIT has already ended the transaction


def _lit(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"
