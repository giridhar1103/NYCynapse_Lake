import dataclasses

import pytest

from nycynapse_lake.contracts import Column, parse
from nycynapse_lake.writer import Envelope, write


def table(write_mode, **over):
    t = {
        "name": "item",
        "description": "items",
        "grain": "one row per item",
        "primary_key": ["id"],
        "write": write_mode,
        "columns": [
            {"name": "id", "type": "integer", "nullable": False, "description": "id"},
            {"name": "month", "type": "date", "nullable": False, "description": "month"},
            {"name": "v", "type": "integer", "description": "value"},
        ],
    }
    t.update(over)
    return parse(
        {
            "source": "demo",
            "version": 1,
            "domain": "t",
            "description": "d",
            "cadence": "daily",
            "freshness_sla": "1 day",
            "tables": [t],
        }
    ).tables[0]


ENV = Envelope("demo", "load-1", 1)


def batch(con, rows, name="clean"):
    values = ", ".join(f"({i}, DATE '{m}', {v})" for i, m, v in rows)
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM (VALUES {values}) t(id, month, v)"
    )
    return name


def rows(con):
    return con.execute("SELECT id, v FROM lake.silver.item ORDER BY id").fetchall()


def test_replace_all_is_idempotent(con):
    t = table("replace_all")
    src = batch(con, [(1, "2024-01-01", 10), (2, "2024-01-01", 20)])
    write(con, t, src, ENV)
    write(con, t, src, ENV)
    assert rows(con) == [(1, 10), (2, 20)]


def test_envelope_columns_are_filled(con):
    write(con, table("replace_all"), batch(con, [(1, "2024-01-01", 1)]), ENV)
    src, load_id, ver = con.execute(
        "SELECT _source, _load_id, _contract_version FROM lake.silver.item"
    ).fetchone()
    assert (src, load_id, ver) == ("demo", "load-1", 1)


def test_replace_partition_only_touches_its_partition(con):
    t = table("replace_partition", primary_key=[])
    write(
        con,
        t,
        batch(con, [(1, "2024-01-01", 1), (2, "2024-01-01", 2)]),
        ENV,
        partition_filter="month = DATE '2024-01-01'",
    )
    write(
        con,
        t,
        batch(con, [(3, "2024-02-01", 3)]),
        ENV,
        partition_filter="month = DATE '2024-02-01'",
    )
    write(
        con,
        t,
        batch(con, [(1, "2024-01-01", 9)]),
        ENV,
        partition_filter="month = DATE '2024-01-01'",
    )
    assert rows(con) == [(1, 9), (3, 3)]


def test_replace_partition_refuses_rows_outside_the_partition(con):
    t = table("replace_partition", primary_key=[])
    src = batch(con, [(1, "2024-01-01", 1), (2, "2024-03-01", 2)])
    with pytest.raises(ValueError, match="outside"):
        write(con, t, src, ENV, partition_filter="month = DATE '2024-01-01'")


def test_merge_respects_version_column(con):
    t = table("merge", version_column="v")
    write(con, t, batch(con, [(1, "2024-01-01", 5)]), ENV)
    write(con, t, batch(con, [(1, "2024-01-01", 3), (2, "2024-01-01", 1)]), ENV)
    assert rows(con) == [(1, 5), (2, 1)]
    write(con, t, batch(con, [(1, "2024-01-01", 8)]), ENV)
    assert rows(con) == [(1, 8), (2, 1)]


def test_append_new_skips_known_keys(con):
    t = table("append_new")
    write(con, t, batch(con, [(1, "2024-01-01", 1)]), ENV)
    result = write(con, t, batch(con, [(1, "2024-01-01", 99), (2, "2024-01-01", 2)]), ENV)
    assert result.rows_written == 1
    assert rows(con) == [(1, 1), (2, 2)]


def test_contract_can_add_a_nullable_column(con):
    write(con, table("replace_all"), batch(con, [(1, "2024-01-01", 1)]), ENV)
    base = table("replace_all")
    wider = dataclasses.replace(base, columns=(*base.columns, Column("note", "VARCHAR", "a note")))
    con.execute(
        "CREATE OR REPLACE TEMP TABLE c2 AS SELECT 2 AS id, DATE '2024-01-01' AS month, "
        "2 AS v, 'hi' AS note"
    )
    write(con, wider, "c2", ENV)
    assert con.execute("SELECT id, note FROM lake.silver.item").fetchall() == [(2, "hi")]


def test_each_write_is_one_snapshot(con):
    t = table("replace_all")
    first = write(con, t, batch(con, [(1, "2024-01-01", 1)]), ENV).snapshot_id
    second = write(con, t, batch(con, [(2, "2024-01-01", 2)]), ENV).snapshot_id
    assert second == first + 1
    old = con.execute(f"SELECT id FROM lake.silver.item AT (VERSION => {first})").fetchall()
    assert old == [(1,)]
