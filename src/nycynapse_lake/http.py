"""HTTP client with retries, conditional requests and streamed downloads."""

import hashlib
import time
import uuid
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx

from . import __version__
from .retry import Retryable, RetryPolicy, retry

USER_AGENT = f"NYCynapse-Lake/{__version__} (github.com/giridhar1103/NYCynapse_Lake)"
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}
DEFAULT_POLICY = RetryPolicy()


@dataclass(frozen=True)
class Downloaded:
    url: str
    path: Path
    sha256: str
    size: int
    etag: str | None
    last_modified: str | None


def _retry_after(resp: httpx.Response) -> float | None:
    value = resp.headers.get("Retry-After")
    if not value:
        return None
    if value.isdigit():
        return float(value)
    try:
        return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
    except (TypeError, ValueError):
        return None


class Http:
    def __init__(
        self,
        *,
        policy: RetryPolicy = DEFAULT_POLICY,
        timeout: float = 60.0,
        headers: dict | None = None,
        transport: httpx.BaseTransport | None = None,
        sleep=time.sleep,
    ):
        self.policy = policy
        self.sleep = sleep
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout, connect=15.0),
            headers={"User-Agent": USER_AGENT, **(headers or {})},
            follow_redirects=True,
            transport=transport,
        )

    def close(self) -> None:
        self.client.close()

    def _check(self, resp: httpx.Response) -> httpx.Response:
        if resp.status_code in RETRY_STATUS:
            raise Retryable(f"{resp.status_code} from {resp.url}", _retry_after(resp))
        if resp.status_code != 304:
            resp.raise_for_status()
        return resp

    def get(
        self, url: str, *, params: dict | None = None, headers: dict | None = None
    ) -> httpx.Response:
        def once():
            try:
                return self._check(self.client.get(url, params=params, headers=headers))
            except httpx.TransportError as exc:
                raise Retryable(f"{type(exc).__name__} on {url}") from exc

        return retry(once, self.policy, sleep=self.sleep)

    def get_json(self, url: str, **kw):
        return self.get(url, **kw).json()

    def download(
        self,
        url: str,
        dest_dir: Path,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
        headers: dict | None = None,
    ) -> Downloaded | None:
        """Stream url to a file in dest_dir. Returns None when the server says it has not changed.

        The caller owns the file and deletes it once the load is committed.
        """
        dest_dir.mkdir(parents=True, exist_ok=True)
        headers = dict(headers or {})
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified

        def once():
            path = dest_dir / f"{uuid.uuid4().hex}.part"
            digest = hashlib.sha256()
            size = 0
            try:
                with self.client.stream("GET", url, headers=headers) as resp:
                    self._check(resp)
                    if resp.status_code == 304:
                        return None
                    expected = resp.headers.get("Content-Length")
                    with path.open("wb") as out:
                        for chunk in resp.iter_bytes(1 << 20):
                            out.write(chunk)
                            digest.update(chunk)
                            size += len(chunk)
                    encoded = "Content-Encoding" in resp.headers
                    if expected and int(expected) != size and not encoded:
                        raise Retryable(f"short read on {url}: {size} of {expected} bytes")
                    return Downloaded(
                        url,
                        path,
                        digest.hexdigest(),
                        size,
                        resp.headers.get("ETag"),
                        resp.headers.get("Last-Modified"),
                    )
            except httpx.TransportError as exc:
                path.unlink(missing_ok=True)
                raise Retryable(f"{type(exc).__name__} on {url}") from exc
            except BaseException:
                path.unlink(missing_ok=True)
                raise

        return retry(once, self.policy, sleep=self.sleep)
