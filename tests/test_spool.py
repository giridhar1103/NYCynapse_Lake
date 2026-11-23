import pyarrow as pa
import pytest

from nycynapse_lake.spool import Spool, SpoolFull


def test_drains_in_order_and_removes_files(tmp_path):
    s = Spool(tmp_path, "gbfs")
    s.put("status", pa.table({"n": [1]}), {"polled_at": "a"})
    s.put("status", pa.table({"n": [2]}), {"polled_at": "b"})
    seen = []
    assert s.drain(lambda t, b, m: seen.append((t, b["n"][0].as_py(), m["polled_at"]))) == 2
    assert seen == [("status", 1, "a"), ("status", 2, "b")]
    assert s.pending() == []


def test_failed_drain_keeps_the_batch(tmp_path):
    s = Spool(tmp_path, "gbfs")
    s.put("status", pa.table({"n": [1]}))

    def broken(t, b, m):
        raise ConnectionError("lake is down")

    with pytest.raises(ConnectionError):
        s.drain(broken)
    assert len(s.pending()) == 1


def test_refuses_to_grow_past_its_limit(tmp_path):
    s = Spool(tmp_path, "gbfs", max_bytes=10)
    with pytest.raises(SpoolFull):
        s.put("status", pa.table({"n": list(range(1000))}))


def test_a_damaged_or_refused_batch_is_buried_and_the_rest_go_on(tmp_path):
    s = Spool(tmp_path, "gbfs")
    s.put("status", pa.table({"n": [1]}))
    (s.dir / "00000000T000000000000-bad.parquet").write_bytes(b"not parquet")
    s.put("status", pa.table({"n": [2]}))
    seen, dead = [], []

    def load(t, b, m):
        if b["n"][0].as_py() == 1:
            raise ValueError("refused")
        seen.append(b["n"][0].as_py())

    s.drain(load, permanent=(ValueError,), on_dead=lambda p, e: dead.append(p))
    assert seen == [2] and len(dead) == 2 and len(s.dead()) == 2
    assert s.pending() == []
