"""Shared lifecycle for live pollers.

A poller reads its feeds every `interval` seconds and keeps what it derives in memory. Every
`flush_every` seconds it writes those rows to the lake as one run, so each write shows up in
ops.runs like a batch load. If the write fails, the rows go to the local spool and are written
on a later flush. State is saved to disk after every flush so a restart picks up where it left
off, and time the poller was not running is recorded in ops.feed_gaps.
"""

import json
import logging
import os
import signal
import threading
import time
from datetime import UTC, datetime

import pyarrow as pa

from .. import lake
from ..config import Settings
from ..contracts import Contract
from ..control.db import Control
from ..http import Http
from ..retry import RetryPolicy
from ..runs import run_source
from ..spool import Spool

log = logging.getLogger(__name__)

GAP_SECONDS = 90
MAX_STATE_AGE = 3 * 3600


class Poller:
    interval = 30
    flush_every = 300

    def __init__(self, settings: Settings, contract: Contract):
        self.settings = settings
        # Optional SQL applied to a table's rows before they are loaded, given a relation name.
        self.transforms: dict = {}
        self.contract = contract
        self.http = Http(policy=RetryPolicy(attempts=2, base=1, cap=3), timeout=15)
        self.ctl = Control(settings.pg_dsn)
        self.con = None
        self.spool = Spool(settings.spool_path, contract.source)
        self.down_since: dict[str, int] = {}
        self.stop = threading.Event()
        settings.state_path.mkdir(parents=True, exist_ok=True)
        self.state_file = settings.state_path / f"{contract.source}.json"

    # to implement

    def poll(self, now: int) -> None:
        raise NotImplementedError

    def take(self) -> dict[str, pa.Table]:
        """Rows gathered since the last flush, by table. Called once per flush."""
        raise NotImplementedError

    def dump_state(self) -> dict:
        return {}

    def load_state(self, data: dict) -> None:
        pass

    # lifecycle

    def run_forever(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        signal.signal(signal.SIGINT, lambda *_: self.stop.set())
        self.restore()
        next_flush = time.time() + self.flush_every
        while not self.stop.is_set():
            started = time.time()
            self.poll(int(started))
            if started >= next_flush:
                self.flush()
                next_flush = started + self.flush_every
            self.stop.wait(max(0.0, self.interval - (time.time() - started)))
        log.info("stopping, writing what is left")
        self.flush()

    def fetch(self, name: str, url: str, now: int, *, json_body: bool = False):
        """GET a feed, tracking outages. Returns None when the feed could not be read."""
        try:
            resp = self.http.get(url)
            body = resp.json() if json_body else resp.content
        except Exception as exc:  # noqa: BLE001 - one bad feed must not stop the others
            self.down_since.setdefault(name, now)
            log.warning("%s failed: %s", name, exc)
            return None
        since = self.down_since.pop(name, None)
        if since is not None and now - since >= GAP_SECONDS:
            self.ctl.record_gap(
                f"{self.contract.source}:{name}", _dt(since), _dt(now), "feed unavailable"
            )
        return body

    def flush(self) -> None:
        batches = {}
        try:
            batches = {k: v for k, v in self.take().items() if v is not None and v.num_rows}
            if self.con is None:
                self.con = lake.connect(self.settings)
                lake.ensure_schemas(self.con)
            with run_source(
                self.contract, self.settings, ctl=self.ctl, con=self.con, http=self.http
            ) as ctx:
                ctx.detail["spooled_batches"] = self.spool.drain(
                    lambda table, batch, meta: self._load(ctx, table, batch)
                )
                for table, batch in batches.items():
                    self._load(ctx, table, batch)
                ctx.detail.update(self.describe())
        except Exception as exc:  # noqa: BLE001 - keep the rows, retry on the next flush
            log.error("flush failed, spooling %d batches: %s", len(batches), exc)
            for table, batch in batches.items():
                self.spool.put(table, batch)
            if self.con is not None:
                self.con.close()
                self.con = None
        self.save()

    def _load(self, ctx, table: str, batch: pa.Table) -> None:
        transform = self.transforms.get(table)
        if transform is None:
            ctx.load(table, batch)
            return
        name = f"_incoming_{table}"
        ctx.con.register(name, batch)
        try:
            ctx.load(table, transform(name))
        finally:
            ctx.con.unregister(name)

    def describe(self) -> dict:
        return {}

    # state

    def save(self) -> None:
        tmp = self.state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps({"saved_at": int(time.time()), "state": self.dump_state()}))
        os.replace(tmp, self.state_file)

    def restore(self) -> None:
        if not self.state_file.exists():
            return
        data = json.loads(self.state_file.read_text())
        now = int(time.time())
        saved = data.get("saved_at", now)
        if now - saved > MAX_STATE_AGE:
            log.warning("state is %d minutes old, starting fresh", (now - saved) // 60)
        else:
            # Files written before the state key existed hold the state at the top level.
            self.load_state(data.get("state", data))
        if now - saved >= GAP_SECONDS:
            self.ctl.record_gap(self.contract.source, _dt(saved), _dt(now), "poller not running")


def _dt(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, UTC)
