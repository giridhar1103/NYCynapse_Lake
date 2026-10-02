"""Helper for sources published as whole files."""

from collections.abc import Iterator
from contextlib import contextmanager

from ..http import Downloaded
from ..runs import RunContext


@contextmanager
def fetch_if_changed(
    ctx: RunContext, url: str, *, force: bool = False
) -> Iterator[Downloaded | None]:
    """Download url into the run's temp space when it changed since the last successful load.

    Yields None when nothing changed. The file is removed on exit whatever happens, so no
    copy of the source file outlives the run.
    """
    prior = None if force else ctx.ctl.get_upstream_file(url)
    got = ctx.http.download(
        url,
        ctx.settings.tmp_path / ctx.source,
        etag=prior["etag"] if prior else None,
        last_modified=prior["last_modified"] if prior else None,
    )
    if got is None or (prior and prior["sha256"] == got.sha256):
        ctx.ctl.touch_upstream_file(url)
        ctx.detail.setdefault("unchanged", []).append(url)
        if got is not None:
            got.path.unlink(missing_ok=True)
        yield None
        return
    try:
        yield got
        ctx.ctl.record_upstream_file(
            ctx.source,
            url,
            etag=got.etag,
            last_modified=got.last_modified,
            sha256=got.sha256,
            content_length=got.size,
            run_id=ctx.run_id,
        )
        ctx.detail.setdefault("loaded", []).append({"url": url, "sha256": got.sha256})
    finally:
        got.path.unlink(missing_ok=True)
