"""A run is one execution of one source.

Order of operations matters here. Data is committed to the lake first and checkpoints second,
so a crash between the two means the next run repeats some work. The writes are idempotent,
which turns that repeat into a no-op instead of duplicate rows.
"""

import logging
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

import duckdb
import pyarrow as pa

from . import quality, writer
from .breaker import Breaker, CircuitOpen
from .config import Settings
from .contracts import Contract
from .control.db import Control
from .http import Http
from .retry import Retryable

log = logging.getLogger(__name__)


class SourceBusy(Exception):
    pass


@dataclass
class RunContext:
    run_id: uuid.UUID
    contract: Contract
    settings: Settings
    ctl: Control
    con: duckdb.DuckDBPyConnection
    http: Http
    stats: dict = field(
        default_factory=lambda: {"rows_in": 0, "rows_written": 0, "rows_quarantined": 0}
    )
    snapshot_id: int | None = None
    detail: dict = field(default_factory=dict)
    _checkpoints: dict = field(default_factory=dict)

    @property
    def source(self) -> str:
        return self.contract.source

    def checkpoint(self, key: str, default=None):
        if key in self._checkpoints:
            return self._checkpoints[key]
        value = self.ctl.get_checkpoint(self.source, key)
        return default if value is None else value

    def save(self, key: str, value) -> None:
        """Persist a checkpoint right away. Call it only after the data it covers is committed.

        Long backfills use this so a crash halfway through does not start them over.
        """
        self.ctl.set_checkpoints(self.source, {key: value}, self.run_id)
        self._checkpoints.pop(key, None)

    def advance(self, key: str, value) -> None:
        """Stage a checkpoint. It is saved only if the whole run succeeds."""
        self._checkpoints[key] = value

    def load(self, table_name: str, data, *, partition_filter: str | None = None) -> int:
        """Validate staged data against the contract and write the clean rows."""
        table = self.contract.table(table_name)
        staged = f"_staged_{table.name}"
        if isinstance(data, pa.Table):
            self.con.register(staged, data)
        elif isinstance(data, str):
            self.con.execute(f"CREATE OR REPLACE TEMP VIEW {staged} AS {data}")
        else:
            self.con.register(staged, data)

        v = None
        try:
            v = quality.validate(self.con, table, staged)
            self.stats["rows_in"] += v.rows_in
            self.stats["rows_quarantined"] += v.rows_rejected
            self.ctl.record_dq(self.run_id, self.source, v.checks)
            self.ctl.quarantine(self.run_id, self.source, table.name, v.rejected_sample)
            bad = quality.failed(v.checks)
            if bad:
                summary = ", ".join(f"{c.check}={c.observed}" for c in bad)
                raise quality.QualityFailure(f"{table.name} failed checks: {summary}")

            env = writer.Envelope(self.source, str(self.run_id), self.contract.version)
            result = writer.write(self.con, table, v.clean, env, partition_filter=partition_filter)
            self.stats["rows_written"] += result.rows_written
            self.snapshot_id = result.snapshot_id
            self._update_freshness(table, v.clean, exact=table.write == "replace_all")
            return result.rows_written
        finally:
            if v is not None:
                v.cleanup(self.con)
            try:
                self.con.unregister(staged)
            except duckdb.Error:
                self.con.execute(f"DROP VIEW IF EXISTS {staged}")

    def _update_freshness(self, table, clean: str, *, exact: bool) -> None:
        rows = self.con.execute(f"SELECT count(*) FROM {table.qualified}").fetchone()[0]
        start = end = None
        if table.event_time:
            relation = table.qualified if exact else clean
            start, end = self.con.execute(
                # As text, so naive and zoned types travel to Postgres the same way.
                f'SELECT CAST(min("{table.event_time}") AS VARCHAR), '
                f'CAST(max("{table.event_time}") AS VARCHAR) FROM {relation}'
            ).fetchone()
        self.ctl.update_freshness(
            table=table.qualified,
            source=self.source,
            event_column=table.event_time,
            coverage_start=start,
            coverage_end=end,
            row_count=rows,
            run_id=self.run_id,
            exact=exact,
        )


@contextmanager
def run_source(
    contract: Contract,
    settings: Settings,
    *,
    ctl: Control | None = None,
    http: Http | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> Iterator[RunContext]:
    own_ctl, own_http, own_con = ctl is None, http is None, con is None
    ctl = ctl or Control(settings.pg_dsn)
    http = http or Http()
    breaker = Breaker(ctl, contract.source)
    run_id = uuid.uuid4()
    locked = False
    try:
        ctl.register_source(contract)
        if not ctl.try_lock(contract.source):
            raise SourceBusy(f"{contract.source} is already running")
        locked = True
        breaker.check()

        if con is None:
            from .lake import connect, ensure_schemas

            con = connect(settings)
            ensure_schemas(con)
        ctl.start_run(run_id, contract.source)
        ctx = RunContext(run_id, contract, settings, ctl, con, http)
        try:
            yield ctx
        except Exception as exc:
            if isinstance(exc, Retryable | OSError):
                breaker.failure(str(exc))
            ctl.finish_run(
                run_id,
                "failed",
                stats=ctx.stats,
                error=_short(exc),
                snapshot_id=ctx.snapshot_id,
                detail=ctx.detail,
            )
            raise
        ctl.set_checkpoints(contract.source, ctx._checkpoints, run_id)
        ctl.finish_run(
            run_id, "succeeded", stats=ctx.stats, snapshot_id=ctx.snapshot_id, detail=ctx.detail
        )
        breaker.success()
        log.info("%s run %s: %s", contract.source, run_id, ctx.stats)
    except CircuitOpen as exc:
        ctl.start_run(run_id, contract.source)
        ctl.finish_run(run_id, "skipped", stats={}, error=str(exc))
        raise
    finally:
        if locked:
            ctl.unlock(contract.source)
        if own_con and con is not None:
            con.close()
        if own_http:
            http.close()
        if own_ctl:
            ctl.close()


def _short(exc: Exception) -> str:
    text = f"{type(exc).__name__}: {exc}"
    return text if len(text) < 2000 else text[:2000] + "..."
