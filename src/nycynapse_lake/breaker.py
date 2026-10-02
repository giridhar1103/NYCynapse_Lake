"""Per-source circuit breaker.

State lives in Postgres so it survives restarts and every worker sees the same state.
"""

from datetime import UTC, datetime, timedelta

from .control.db import Control


class CircuitOpen(Exception):
    def __init__(self, source: str, retry_after: datetime):
        super().__init__(f"{source} is paused until {retry_after.isoformat()}")
        self.source = source
        self.retry_after = retry_after


class Breaker:
    def __init__(
        self,
        ctl: Control,
        source: str,
        *,
        threshold: int = 3,
        cooldown: timedelta = timedelta(minutes=2),
        max_cooldown: timedelta = timedelta(hours=2),
    ):
        self.ctl = ctl
        self.source = source
        self.threshold = threshold
        self.cooldown = cooldown
        self.max_cooldown = max_cooldown

    def _row(self) -> dict:
        row = self.ctl.conn.execute(
            "SELECT * FROM ops.breakers WHERE source = %s", (self.source,)
        ).fetchone()
        return row or {"state": "closed", "failures": 0, "retry_after": None}

    def check(self, now: datetime | None = None) -> None:
        now = now or datetime.now(UTC)
        row = self._row()
        if row["state"] == "open":
            if row["retry_after"] and now < row["retry_after"]:
                raise CircuitOpen(self.source, row["retry_after"])
            self._set("half_open", row["failures"], None, None, None)

    def success(self) -> None:
        self._set("closed", 0, None, None, None)

    def failure(self, error: str, now: datetime | None = None) -> None:
        now = now or datetime.now(UTC)
        row = self._row()
        failures = row["failures"] + 1
        if row["state"] == "half_open" or failures >= self.threshold:
            steps = max(0, failures - self.threshold)
            wait = min(self.max_cooldown, self.cooldown * 2**steps)
            self._set("open", failures, now, now + wait, error)
        else:
            self._set("closed", failures, None, None, error)

    def _set(self, state, failures, opened_at, retry_after, error) -> None:
        self.ctl.conn.execute(
            """
            INSERT INTO ops.breakers (source, state, failures, opened_at, retry_after, last_error)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source) DO UPDATE SET
                state = EXCLUDED.state, failures = EXCLUDED.failures,
                opened_at = COALESCE(EXCLUDED.opened_at, ops.breakers.opened_at),
                retry_after = EXCLUDED.retry_after,
                last_error = COALESCE(EXCLUDED.last_error, ops.breakers.last_error)
            """,
            (self.source, state, failures, opened_at, retry_after, error),
        )
