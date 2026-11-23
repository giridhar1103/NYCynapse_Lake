"""Shared lifecycle for live pollers.

A poller reads its feeds every `interval` seconds and keeps what it derives in memory. Every
`flush_every` seconds the rows are written to the local spool and from there to the lake as one
run, so each write shows up in ops.runs like a batch load. If the lake cannot take them they stay
in the spool for a later flush. State is saved to disk after every flush so a restart picks up
where it left off, and time the poller was not running is recorded in ops.feed_gaps.

One bad feed, one bad message or one bad batch never stops the rest. A loop that hangs is
restarted by systemd (WATCHDOG=1 is sent on every turn).
"""

import json
import logging
import os
import signal
import socket
import threading
import time
from datetime import UTC, datetime

import pyarrow as pa

from .. import lake
from ..config import Settings
from ..contracts import Contract, ContractError
from ..control.db import Control
from ..http import Http
from ..quality import QualityFailure
from ..retry import RetryPolicy
from ..runs import run_source
from ..spool import Spool, SpoolFull

log = logging.getLogger(__name__)

GAP_SECONDS = 90
MAX_STATE_AGE = 3 * 3600
STALE_SECONDS = 300  # a feed whose own timestamp is this old has stopped, whatever its status
MAX_BAD_POLLS = 40  # polls in a row that raised, before the process gives up and restarts
# Errors that will not go away by trying the same batch again.
PERMANENT = (QualityFailure, ContractError, KeyError, pa.ArrowInvalid)


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
        bad = 0
        while not self.stop.is_set():
            started = time.time()
            _notify("WATCHDOG=1")
            try:
                self.poll(int(started))
                bad = 0
            except Exception:  # noqa: BLE001 - a message this code cannot read is skipped
                bad += 1
                log.exception("poll failed (%d in a row)", bad)
                if bad >= MAX_BAD_POLLS:
                    self.flush()
                    raise
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
            self._down(name, now, f"failed: {exc}")
            return None
        return body

    def fresh(self, name: str, stamp: int | None, now: int, limit: int = STALE_SECONDS) -> bool:
        """Whether a feed that answered is alive, by its own timestamp. Records the outage when
        it comes back. A feed that keeps answering with an old message counts as down."""
        if stamp and now - stamp > limit:
            self._down(name, now, f"stopped updating {now - stamp} s ago")
            return False
        since = self.down_since.pop(name, None)
        if since is not None and now - since >= GAP_SECONDS:
            try:
                self.ctl.record_gap(
                    f"{self.contract.source}:{name}", _dt(since), _dt(now), "feed unavailable"
                )
            except Exception:  # noqa: BLE001 - bookkeeping must not stop the capture
                log.exception("could not record a gap in %s", name)
        return True

    def _down(self, name: str, now: int, why: str) -> None:
        if name not in self.down_since:
            log.warning("%s %s", name, why)
        self.down_since.setdefault(name, now)

    def flush(self) -> None:
        self._stage()
        try:
            if self.con is None:
                self.con = lake.connect(self.settings)
                lake.ensure_schemas(self.con)
            with run_source(
                self.contract, self.settings, ctl=self.ctl, con=self.con, http=self.http
            ) as ctx:
                dead = []

                def on_dead(path, exc):
                    dead.append(f"{path.name}: {type(exc).__name__}: {exc}"[:300])
                    log.error("batch set aside, %s: %s", path.name, exc)

                ctx.detail["batches"] = self.spool.drain(
                    lambda table, batch, meta: self._load(ctx, table, batch),
                    permanent=PERMANENT,
                    on_dead=on_dead,
                )
                if dead:
                    ctx.detail["dead_batches"] = dead
                if self.down_since:
                    ctx.detail["feeds_down"] = sorted(self.down_since)
                ctx.detail.update(self.describe())
        except Exception as exc:  # noqa: BLE001 - the rows are in the spool, the next flush retries
            log.error("flush failed, %d batches wait: %s", len(self.spool.pending()), exc)
            if self.con is not None:
                try:
                    self.con.close()
                except Exception:  # noqa: BLE001 - it is being replaced anyway
                    log.warning("could not close the lake connection")
                self.con = None
        self.save()

    def _stage(self) -> None:
        """Write what was gathered to the spool before anything else can go wrong."""
        for table, batch in self.take().items():
            if batch is None or not batch.num_rows:
                continue
            try:
                self.spool.put(table, batch)
            except (SpoolFull, OSError) as exc:
                log.error(
                    "lost %d rows of %s, the spool cannot take them: %s", batch.num_rows, table, exc
                )
                self._lost(table, batch.num_rows, str(exc))

    def _lost(self, table: str, rows: int, why: str) -> None:
        now = datetime.now(UTC)
        try:
            self.ctl.record_gap(
                f"{self.contract.source}:{table}", now, now, f"lost {rows} rows: {why}"[:300]
            )
        except Exception:  # noqa: BLE001 - already logged
            log.warning("could not record the loss")

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
        try:
            tmp.write_text(json.dumps({"saved_at": int(time.time()), "state": self.dump_state()}))
            os.replace(tmp, self.state_file)
        except OSError as exc:
            log.error("could not save state: %s", exc)

    def restore(self) -> None:
        if not self.state_file.exists():
            return
        now = int(time.time())
        try:
            data = json.loads(self.state_file.read_text())
            saved = int(data.get("saved_at", now))
            if now - saved > MAX_STATE_AGE:
                log.warning("state is %d minutes old, starting fresh", (now - saved) // 60)
            else:
                # Files written before the state key existed hold the state at the top level.
                self.load_state(data.get("state", data))
        except Exception as exc:  # noqa: BLE001 - a damaged file must not stop every start
            saved = int(self.state_file.stat().st_mtime)
            os.replace(self.state_file, self.state_file.with_suffix(".damaged"))
            log.error("state file was damaged, starting fresh: %s", exc)
        if now - saved >= GAP_SECONDS:
            self.ctl.record_gap(self.contract.source, _dt(saved), _dt(now), "poller not running")


def _notify(message: str) -> None:
    """Tell systemd something, when it is listening."""
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        return
    if address.startswith("@"):
        address = "\0" + address[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.connect(address)
            s.sendall(message.encode())
    except OSError:
        pass


def _dt(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, UTC)
