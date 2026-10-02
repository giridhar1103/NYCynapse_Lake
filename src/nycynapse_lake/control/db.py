"""Postgres control plane: migrations, runs, checkpoints and the other ops tables."""

from contextlib import contextmanager
from importlib import resources

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

MIGRATIONS = "nycynapse_lake.control.migrations"


class Control:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
        # The host clock is Europe/Berlin. Control data is always read and written in UTC.
        self.conn.execute("SET TimeZone = 'UTC'")

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def tx(self):
        with self.conn.transaction():
            yield self.conn

    def migrate(self) -> list[str]:
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS ops.schema_migrations "
            "(version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        rows = self.conn.execute("SELECT version FROM ops.schema_migrations")
        done = {r["version"] for r in rows}
        applied = []
        files = sorted(f for f in resources.files(MIGRATIONS).iterdir() if f.name.endswith(".sql"))
        for f in files:
            version = f.name.removesuffix(".sql")
            if version in done:
                continue
            with self.tx() as c:
                c.execute(f.read_text())
                c.execute("INSERT INTO ops.schema_migrations (version) VALUES (%s)", (version,))
            applied.append(version)
        return applied

    # sources

    def register_source(self, contract) -> None:
        self.conn.execute(
            """
            INSERT INTO ops.sources (source, domain, cadence, freshness_sla, contract_version,
                                     description, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (source) DO UPDATE SET
                domain = EXCLUDED.domain, cadence = EXCLUDED.cadence,
                freshness_sla = EXCLUDED.freshness_sla,
                contract_version = EXCLUDED.contract_version,
                description = EXCLUDED.description, updated_at = now()
            """,
            (
                contract.source,
                contract.domain,
                contract.cadence,
                contract.freshness_sla,
                contract.version,
                contract.description,
            ),
        )

    # advisory locks keep two copies of the same source from running at once

    def try_lock(self, source: str) -> bool:
        row = self.conn.execute(
            "SELECT pg_try_advisory_lock(hashtext('nyc_lake:' || %s)) AS ok", (source,)
        ).fetchone()
        return row["ok"]

    def unlock(self, source: str) -> None:
        self.conn.execute("SELECT pg_advisory_unlock(hashtext('nyc_lake:' || %s))", (source,))

    # runs

    def start_run(self, run_id, source: str) -> None:
        self.conn.execute(
            "INSERT INTO ops.runs (run_id, source, status) VALUES (%s, %s, 'running')",
            (run_id, source),
        )

    def finish_run(
        self,
        run_id,
        status: str,
        *,
        stats: dict,
        error: str | None = None,
        snapshot_id: int | None = None,
        detail: dict | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE ops.runs SET finished_at = now(), status = %s, rows_in = %s,
                   rows_written = %s, rows_quarantined = %s, snapshot_id = %s,
                   error = %s, detail = %s
            WHERE run_id = %s
            """,
            (
                status,
                stats.get("rows_in", 0),
                stats.get("rows_written", 0),
                stats.get("rows_quarantined", 0),
                snapshot_id,
                error,
                Jsonb(detail or {}),
                run_id,
            ),
        )

    # checkpoints

    def get_checkpoint(self, source: str, key: str):
        row = self.conn.execute(
            "SELECT value FROM ops.checkpoints WHERE source = %s AND key = %s", (source, key)
        ).fetchone()
        return None if row is None else row["value"]

    def set_checkpoints(self, source: str, values: dict, run_id) -> None:
        with self.tx() as c:
            for key, value in values.items():
                c.execute(
                    """
                    INSERT INTO ops.checkpoints (source, key, value, run_id, updated_at)
                    VALUES (%s, %s, %s, %s, now())
                    ON CONFLICT (source, key) DO UPDATE SET
                        value = EXCLUDED.value, run_id = EXCLUDED.run_id, updated_at = now()
                    """,
                    (source, key, Jsonb(value), run_id),
                )

    # upstream files

    def get_upstream_file(self, url: str) -> dict | None:
        return self.conn.execute(
            "SELECT * FROM ops.upstream_files WHERE url = %s", (url,)
        ).fetchone()

    def record_upstream_file(
        self, source: str, url: str, *, etag, last_modified, sha256, content_length, run_id
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO ops.upstream_files (url, source, etag, last_modified, sha256,
                                            content_length, loaded_run_id, checked_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (url) DO UPDATE SET
                etag = EXCLUDED.etag, last_modified = EXCLUDED.last_modified,
                sha256 = EXCLUDED.sha256, content_length = EXCLUDED.content_length,
                loaded_run_id = EXCLUDED.loaded_run_id, checked_at = now()
            """,
            (url, source, etag, last_modified, sha256, content_length, run_id),
        )

    def touch_upstream_file(self, url: str) -> None:
        self.conn.execute("UPDATE ops.upstream_files SET checked_at = now() WHERE url = %s", (url,))

    # quality

    def record_dq(self, run_id, source: str, results: list) -> None:
        if not results:
            return
        with self.tx() as c, c.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO ops.dq_results (run_id, source, table_name, check_name, severity,
                                            passed, observed, threshold, detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        run_id,
                        source,
                        r.table,
                        r.check,
                        r.severity,
                        r.passed,
                        r.observed,
                        r.threshold,
                        Jsonb(r.detail),
                    )
                    for r in results
                ],
            )

    def quarantine(self, run_id, source: str, table: str, rows: list[tuple[str, dict]]) -> None:
        if not rows:
            return
        with self.tx() as c, c.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO ops.quarantine (run_id, source, table_name, reason, record)
                VALUES (%s, %s, %s, %s, %s)
                """,
                [(run_id, source, table, reason, Jsonb(record)) for reason, record in rows],
            )

    def purge_quarantine(self, older_than_days: int = 30) -> int:
        cur = self.conn.execute(
            "DELETE FROM ops.quarantine WHERE quarantined_at < now() - make_interval(days => %s)",
            (older_than_days,),
        )
        return cur.rowcount

    # live feed gaps and freshness

    def record_gap(self, source: str, start, end, reason: str) -> None:
        self.conn.execute(
            """
            INSERT INTO ops.feed_gaps (source, gap_start, gap_end, reason)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (source, gap_start) DO UPDATE SET
                gap_end = GREATEST(ops.feed_gaps.gap_end, EXCLUDED.gap_end),
                reason = EXCLUDED.reason
            """,
            (source, start, end, reason),
        )

    def update_freshness(
        self,
        *,
        table: str,
        source: str,
        event_column: str | None,
        coverage_start,
        coverage_end,
        row_count: int,
        run_id,
        exact: bool = False,
    ) -> None:
        """Record what a table covers.

        Incremental loads widen the range. Full reloads replace it.
        """
        merge = (
            "coverage_start = EXCLUDED.coverage_start, coverage_end = EXCLUDED.coverage_end"
            if exact
            else "coverage_start = LEAST(ops.table_freshness.coverage_start, "
            "EXCLUDED.coverage_start), coverage_end = GREATEST(ops.table_freshness.coverage_end, "
            "EXCLUDED.coverage_end)"
        )
        self.conn.execute(
            f"""
            INSERT INTO ops.table_freshness (table_name, source, event_column, coverage_start,
                                             coverage_end, row_count, last_loaded_at, last_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, now(), %s)
            ON CONFLICT (table_name) DO UPDATE SET
                source = EXCLUDED.source, event_column = EXCLUDED.event_column, {merge},
                row_count = EXCLUDED.row_count, last_loaded_at = now(),
                last_run_id = EXCLUDED.last_run_id
            """,
            (table, source, event_column, coverage_start, coverage_end, row_count, run_id),
        )
