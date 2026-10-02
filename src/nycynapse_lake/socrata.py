"""Paged reads from Socrata datasets (NYC Open Data, data.ny.gov).

Pages use keyset pagination on system fields rather than offsets. With offsets, a row that is
updated while a window is being read moves to the end of the ordering and the row after it
gets skipped. Keysets do not have that problem.

Pages are written as CSV files into the run's temp space and read by DuckDB in one go, so a
large window never has to sit in Python memory.
"""

import csv
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode

from .http import Downloaded, Http

PAGE_SIZE = 50_000


@dataclass(frozen=True)
class Dataset:
    domain: str
    id: str

    def url(self, fmt: str = "json") -> str:
        return f"https://{self.domain}/resource/{self.id}.{fmt}"


@dataclass(frozen=True)
class Page:
    file: Downloaded
    rows: int
    last: tuple[str, ...]


class Socrata:
    def __init__(self, http: Http, app_token: str | None = None):
        self.http = http
        self.headers = {"X-App-Token": app_token} if app_token else None

    def query(self, ds: Dataset, **soql) -> list[dict]:
        params = {f"${k}": v for k, v in soql.items()}
        return self.http.get_json(ds.url("json"), params=params, headers=self.headers)

    def scalar(self, ds: Dataset, select: str, where: str | None = None):
        soql = {"select": f"{select} AS v"}
        if where:
            soql["where"] = where
        rows = self.query(ds, **soql)
        return rows[0].get("v") if rows else None

    def pages(
        self,
        ds: Dataset,
        dest: Path,
        *,
        columns: str,
        where: str,
        keys: tuple[str, ...] = (":id",),
        after: tuple[str, ...] | None = None,
        page_size: int = PAGE_SIZE,
    ) -> Iterator[Page]:
        """Yield pages of `columns` matching `where`, ordered by `keys`, until exhausted.

        The last key must be unique (":id" is). Pass `after` to resume behind a known row.
        """
        select = f"{columns}, {', '.join(keys)}"
        while True:
            clause = where if after is None else f"({where}) AND ({_after(keys, after)})"
            params = {
                "$select": select,
                "$where": clause,
                "$order": ", ".join(keys),
                "$limit": page_size,
            }
            got = self.http.download(
                f"{ds.url('csv')}?{urlencode(params)}", dest, headers=self.headers
            )
            rows, last = _scan(got.path, keys)
            if rows == 0:
                got.path.unlink(missing_ok=True)
                return
            yield Page(got, rows, last)
            if rows < page_size:
                return
            after = last


def _after(keys: tuple[str, ...], values: tuple[str, ...]) -> str:
    """SoQL for "strictly after this row" in the order given by keys."""
    q = [f"'{v.replace(chr(39), chr(39) * 2)}'" for v in values]
    if len(keys) == 1:
        return f"{keys[0]} > {q[0]}"
    if len(keys) == 2:
        return f"{keys[0]} > {q[0]} OR ({keys[0]} = {q[0]} AND {keys[1]} > {q[1]})"
    raise ValueError("keyset paging supports one or two keys")


def _scan(path: Path, keys: tuple[str, ...]) -> tuple[int, tuple[str, ...]]:
    # Quoted fields can contain newlines, so count records with the csv module, not lines.
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return 0, ()
        idx = [header.index(k) for k in keys]
        rows, last = 0, ()
        for record in reader:
            rows += 1
            last = tuple(record[i] for i in idx)
        return rows, last
