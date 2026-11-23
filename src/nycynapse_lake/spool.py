"""Local write-ahead buffer for live feeds.

Every batch is written here before it is loaded and removed once the load is committed, so a
crash or an unreachable lake loses nothing: the batch is loaded on a later flush, and keyed
writes make a repeat harmless. Only processed batches go here, never the payload as received.
Files are written to a temporary name and renamed, so a crash cannot leave half a batch.

A batch the lake refuses for good (it breaks its contract, or the file is damaged) is moved to
dead/ with the reason, so it cannot hold up the batches behind it.
"""

import json
import os
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


class SpoolFull(Exception):
    pass


class Spool:
    def __init__(self, root: Path, source: str, *, max_bytes: int = 512 * 1024 * 1024):
        self.dir = root / source
        self.dir.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max_bytes

    def size(self) -> int:
        return sum(p.stat().st_size for p in self.dir.glob("*.parquet"))

    def put(self, table_name: str, batch: pa.Table, meta: dict | None = None) -> Path:
        if self.size() + batch.nbytes > self.max_bytes:
            raise SpoolFull(f"spool for {self.dir.name} is over {self.max_bytes} bytes")
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
        name = f"{stamp}-{uuid.uuid4().hex[:8]}"
        final = self.dir / f"{name}.parquet"
        tmp = self.dir / f".{name}.tmp"
        info = {"table": table_name, **(meta or {})}
        batch = batch.replace_schema_metadata({b"nyc_lake": json.dumps(info).encode()})
        pq.write_table(batch, tmp, compression="zstd")
        os.replace(tmp, final)
        return final

    def pending(self) -> list[Path]:
        return sorted(self.dir.glob("*.parquet"))

    def dead(self) -> list[Path]:
        return sorted((self.dir / "dead").glob("*.parquet"))

    def bury(self, path: Path, reason: str) -> None:
        grave = self.dir / "dead"
        grave.mkdir(exist_ok=True)
        (grave / f"{path.stem}.txt").write_text(reason[:4000])
        os.replace(path, grave / path.name)

    def revive(self) -> int:
        """Queue the dead batches again, after the cause has been fixed."""
        n = 0
        for path in self.dead():
            os.replace(path, self.dir / path.name)
            path.with_suffix(".txt").unlink(missing_ok=True)
            n += 1
        return n

    def drain(
        self,
        load: Callable[[str, pa.Table, dict], None],
        *,
        permanent: tuple[type[Exception], ...] = (),
        on_dead: Callable[[Path, Exception], None] = lambda path, exc: None,
    ) -> int:
        """Load spooled batches oldest first.

        An error in `permanent` sets that batch aside and goes on. Any other error stops the
        drain, so order is kept and the batch is tried again on the next flush.
        """
        done = 0
        for path in self.pending():
            try:
                batch = pq.read_table(path)
                info = json.loads(batch.schema.metadata[b"nyc_lake"])
                table = info.pop("table")
            except (OSError, ValueError, KeyError, TypeError, pa.ArrowInvalid) as exc:
                self.bury(path, f"unreadable: {type(exc).__name__} {exc}")
                on_dead(path, exc)
                continue
            try:
                load(table, batch.replace_schema_metadata(None), info)
            except permanent as exc:
                self.bury(path, f"{type(exc).__name__}: {exc}")
                on_dead(path, exc)
                continue
            path.unlink()
            done += 1
        return done
