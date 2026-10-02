"""Shared loading pattern for Socrata datasets whose rows change after publication."""

from collections.abc import Callable, Iterable

from ..runs import RunContext
from ..socrata import Dataset, Page, Socrata

PAGES_PER_LOAD = 6
CHANGE_KEYS = (":updated_at", ":id")


def load_pages(
    ctx: RunContext,
    table: str,
    pages: Iterable[Page],
    transform: Callable[[list[str]], str],
    on_commit: Callable[[tuple], None] = lambda last: None,
) -> int:
    """Load pages a few at a time and call on_commit with the last key of each committed group.

    Page files are deleted as soon as their group is written, whether or not it succeeded.
    """
    batch, total = [], 0
    for page in pages:
        batch.append(page)
        if len(batch) == PAGES_PER_LOAD:
            total += _flush(ctx, table, batch, transform, on_commit)
            batch = []
    if batch:
        total += _flush(ctx, table, batch, transform, on_commit)
    return total


def _flush(ctx, table, batch, transform, on_commit) -> int:
    try:
        written = ctx.load(table, transform([str(p.file.path) for p in batch]))
        on_commit(batch[-1].last)
        return written
    finally:
        for p in batch:
            p.file.path.unlink(missing_ok=True)


def follow_changes(
    ctx: RunContext,
    soda: Socrata,
    ds: Dataset,
    *,
    table: str,
    columns: str,
    where: str,
    transform: Callable[[list[str]], str],
    key: str = "changes_after",
) -> int:
    """Read every row changed since the saved position, oldest change first.

    The position is the (:updated_at, :id) of the last committed row. When the checkpoint holds
    only a timestamp, reading starts strictly after it.
    """
    after = tuple(ctx.checkpoint(key) or ("", ""))
    resume = after if after[1] else None
    if after[0] and not after[1]:
        where = f"({where}) AND :updated_at > '{after[0]}'"
    tmp = ctx.settings.tmp_path / ctx.source
    pages = soda.pages(ds, tmp, columns=columns, where=where, keys=CHANGE_KEYS, after=resume)
    return load_pages(
        ctx, table, pages, transform, on_commit=lambda last: ctx.save(key, list(last))
    )


def in_nyc(lon: str, lat: str) -> tuple[str, str]:
    """SQL for longitude and latitude, nulled when the point is not plausibly in the city."""
    ok = (
        f"TRY_CAST({lon} AS DOUBLE) BETWEEN -74.30 AND -73.65 "
        f"AND TRY_CAST({lat} AS DOUBLE) BETWEEN 40.47 AND 40.95"
    )
    return (
        f"CASE WHEN {ok} THEN TRY_CAST({lon} AS DOUBLE) END",
        f"CASE WHEN {ok} THEN TRY_CAST({lat} AS DOUBLE) END",
    )


def local_time(col: str) -> str:
    """Open Data floating timestamps are New York time. Years before 2000 are placeholders."""
    ts = f"TRY_CAST({col} AS TIMESTAMP)"
    return f"CASE WHEN year({ts}) >= 2000 THEN timezone('America/New_York', {ts}) END"


def text(col: str) -> str:
    return f"NULLIF(trim({col}), '')"


def borough(col: str) -> str:
    return (
        f"CASE upper(trim({col})) WHEN 'MANHATTAN' THEN 'Manhattan' WHEN 'BRONX' THEN 'Bronx' "
        f"WHEN 'BROOKLYN' THEN 'Brooklyn' WHEN 'QUEENS' THEN 'Queens' "
        f"WHEN 'STATEN ISLAND' THEN 'Staten Island' END"
    )
