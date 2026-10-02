import httpx
import pytest

from nycynapse_lake.http import Http
from nycynapse_lake.retry import Retryable, RetryPolicy

FAST = RetryPolicy(attempts=4, base=0.01, cap=0.01)


def client(handler, sleeps=None):
    return Http(policy=FAST, transport=httpx.MockTransport(handler),
                sleep=(sleeps.append if sleeps is not None else lambda s: None))


def test_retries_server_errors():
    responses = iter([httpx.Response(503), httpx.Response(502), httpx.Response(200, json={"a": 1})])
    http = client(lambda req: next(responses))
    assert http.get_json("https://example.test/x") == {"a": 1}


def test_respects_retry_after_header():
    responses = iter([httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200)])
    sleeps = []
    client(lambda req: next(responses), sleeps).get("https://example.test/x")
    assert sleeps and sleeps[0] >= 7


def test_client_errors_fail_fast():
    calls = []

    def handler(req):
        calls.append(1)
        return httpx.Response(404)

    with pytest.raises(httpx.HTTPStatusError):
        client(handler).get("https://example.test/missing")
    assert len(calls) == 1


def test_network_errors_are_retried_then_raised():
    def handler(req):
        raise httpx.ConnectError("refused")

    with pytest.raises(Retryable):
        client(handler).get("https://example.test/x")


def test_download_streams_and_hashes(tmp_path):
    body = b"a,b\n1,2\n"
    http = client(lambda req: httpx.Response(200, content=body, headers={"ETag": '"v1"'}))
    got = http.download("https://example.test/f.csv", tmp_path)
    assert got.path.read_bytes() == body
    assert got.size == len(body)
    assert got.etag == '"v1"'
    assert len(got.sha256) == 64


def test_download_sends_validators_and_handles_304(tmp_path):
    seen = {}

    def handler(req):
        seen.update(req.headers)
        return httpx.Response(304)

    got = client(handler).download("https://example.test/f", tmp_path, etag='"v1"',
                                   last_modified="Thu, 22 Feb 2024 21:33:00 GMT")
    assert got is None
    assert seen["if-none-match"] == '"v1"'
    assert "if-modified-since" in seen
    assert not list(tmp_path.iterdir())


def test_short_download_is_retried_and_leaves_no_partial_file(tmp_path):
    responses = iter([
        httpx.Response(200, content=b"abc", headers={"Content-Length": "10"}),
        httpx.Response(200, content=b"abcdefghij"),
    ])
    got = client(lambda req: next(responses)).download("https://example.test/f", tmp_path)
    assert got.size == 10
    assert [p.name for p in tmp_path.iterdir()] == [got.path.name]
