"""Local buffer for live feeds when the lake cannot take a write.

Only processed, validated batches go here, never the payload as received. Files are written
to a temporary name and renamed, so a crash cannot leave a half-written batch behind.
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

    def drain(self, load: Callable[[str, pa.Table, dict], None]) -> int:
        """Replay spooled batches oldest first. Stops at the first failure so order is kept."""
        done = 0
        for path in self.pending():
            batch = pq.read_table(path)
            info = json.loads(batch.schema.metadata[b"nyc_lake"])
            batch = batch.replace_schema_metadata(None)
            load(info.pop("table"), batch, info)
            path.unlink()
            done += 1
        return done
