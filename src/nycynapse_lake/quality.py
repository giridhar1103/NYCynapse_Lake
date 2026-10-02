"""Row and batch checks applied to staged data before it reaches the lake.

A staged relation is cast to the contract's types. Rows that break a rule are set aside
with a reason and sent to quarantine. Batch rules decide whether the load goes ahead.
"""

import json
from dataclasses import dataclass, field

import duckdb

from .contracts import ContractError, Table

QUARANTINE_SAMPLE = 1000


class QualityFailure(Exception):
    pass


@dataclass
class Check:
    table: str
    check: str
    severity: str
    passed: bool
    observed: float | None = None
    threshold: float | None = None
    detail: dict = field(default_factory=dict)


@dataclass
class Validated:
    clean: str  # temp table holding rows ready to write
    rows_in: int
    rows_rejected: int
    duplicates: int
    rejected_sample: list[tuple[str, dict]]
    checks: list[Check]


def _q(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _row_rules(table: Table) -> list[str]:
    rules = []
    for c in table.columns:
        raw, typed = f's."{c.name}"', f'TRY_CAST(s."{c.name}" AS {c.type})'
        if not c.nullable:
            rules.append(f"CASE WHEN {raw} IS NULL THEN '{c.name} is missing' END")
        rules.append(
            f"CASE WHEN {raw} IS NOT NULL AND {typed} IS NULL "
            f"THEN '{c.name} is not a valid {c.type.lower()}' END"
        )
        if c.min is not None:
            rules.append(f"CASE WHEN {typed} < {c.min} THEN '{c.name} below {c.min}' END")
        if c.max is not None:
            rules.append(f"CASE WHEN {typed} > {c.max} THEN '{c.name} above {c.max}' END")
        if c.accepted is not None:
            values = ", ".join(_q(v) for v in c.accepted)
            rules.append(
                f"CASE WHEN {typed} IS NOT NULL AND CAST({typed} AS VARCHAR) NOT IN ({values}) "
                f"THEN '{c.name} has an unexpected value' END"
            )
        if c.pattern is not None:
            rules.append(
                f"CASE WHEN {typed} IS NOT NULL AND NOT regexp_full_match("
                f"CAST({typed} AS VARCHAR), {_q(c.pattern)}) THEN '{c.name} has a bad format' END"
            )
    return rules


def check_schema(con: duckdb.DuckDBPyConnection, table: Table, staged: str) -> list[Check]:
    present = {r[0] for r in con.execute(f"DESCRIBE {staged}").fetchall()}
    expected = {c.name for c in table.columns}
    missing = sorted(expected - present)
    if missing:
        raise ContractError(f"{table.name}: staged data is missing columns {missing}")
    extra = sorted(present - expected)
    return [
        Check(
            table.name,
            "schema_drift",
            "warn",
            not extra,
            float(len(extra)),
            0.0,
            {"unexpected_columns": extra},
        )
    ]


def validate(con: duckdb.DuckDBPyConnection, table: Table, staged: str) -> Validated:
    checks = check_schema(con, table, staged)

    typed_cols = ", ".join(
        f'TRY_CAST(s."{c.name}" AS {c.type}) AS "{c.name}"' for c in table.columns
    )
    reason = f"NULLIF(concat_ws('; ', {', '.join(_row_rules(table))}), '')"
    checked = f"_checked_{table.name}"
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE {checked} AS "
        f"SELECT {typed_cols}, {reason} AS _reject_reason, to_json(s) AS _original FROM {staged} s"
    )
    rows_in, rows_rejected = con.execute(
        f"SELECT count(*), count(_reject_reason) FROM {checked}"
    ).fetchone()

    sample = [
        (r, _as_dict(o))
        for r, o in con.execute(
            f"SELECT _reject_reason, _original FROM {checked} "
            f"WHERE _reject_reason IS NOT NULL LIMIT {QUARANTINE_SAMPLE}"
        ).fetchall()
    ]

    cols = ", ".join(f'"{c.name}"' for c in table.columns)
    clean = f"_clean_{table.name}"
    if table.primary_key:
        keys = ", ".join(f'"{k}"' for k in table.primary_key)
        order = f'"{table.version_column}" DESC NULLS LAST' if table.version_column else "1"
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {clean} AS SELECT {cols} FROM {checked} "
            f"WHERE _reject_reason IS NULL "
            f"QUALIFY row_number() OVER (PARTITION BY {keys} ORDER BY {order}) = 1"
        )
    else:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {clean} AS SELECT {cols} FROM {checked} "
            f"WHERE _reject_reason IS NULL"
        )
    kept = con.execute(f"SELECT count(*) FROM {clean}").fetchone()[0]
    duplicates = rows_in - rows_rejected - kept
    con.execute(f"DROP TABLE {checked}")

    reject_rate = rows_rejected / rows_in if rows_in else 0.0
    checks.append(
        Check(
            table.name,
            "min_rows",
            "error",
            rows_in >= table.min_rows,
            float(rows_in),
            float(table.min_rows),
        )
    )
    checks.append(
        Check(
            table.name,
            "reject_rate",
            "error",
            reject_rate <= table.max_reject_rate,
            reject_rate,
            table.max_reject_rate,
        )
    )
    checks.append(Check(table.name, "duplicate_keys", "info", True, float(duplicates)))

    limited = [c for c in table.columns if c.max_null_rate is not None]
    if limited and kept:
        exprs = ", ".join(f'avg(CASE WHEN "{c.name}" IS NULL THEN 1 ELSE 0 END)' for c in limited)
        rates = con.execute(f"SELECT {exprs} FROM {clean}").fetchone()
        for c, rate in zip(limited, rates, strict=True):
            checks.append(
                Check(
                    table.name,
                    f"null_rate:{c.name}",
                    "warn",
                    rate <= c.max_null_rate,
                    rate,
                    c.max_null_rate,
                )
            )

    return Validated(clean, rows_in, rows_rejected, duplicates, sample, checks)


def failed(checks: list[Check]) -> list[Check]:
    return [c for c in checks if c.severity == "error" and not c.passed]


def _as_dict(value) -> dict:
    return json.loads(value) if isinstance(value, str) else dict(value)
