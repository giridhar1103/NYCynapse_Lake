"""Source contracts: one YAML file per source describing its tables, keys, types and checks."""

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

WRITE_MODES = {"replace_all", "replace_partition", "merge", "append_new"}
IDENT = re.compile(r"^[a-z][a-z0-9_]*$")


class ContractError(Exception):
    pass


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    description: str
    nullable: bool = True
    unit: str | None = None
    min: float | None = None
    max: float | None = None
    accepted: tuple | None = None
    pattern: str | None = None
    max_null_rate: float | None = None


@dataclass(frozen=True)
class Table:
    name: str
    description: str
    grain: str
    primary_key: tuple[str, ...]
    write: str
    columns: tuple[Column, ...]
    version_column: str | None = None
    event_time: str | None = None
    partition_by: tuple[str, ...] = ()
    min_rows: int = 0
    max_reject_rate: float = 0.0

    @property
    def qualified(self) -> str:
        return f"lake.silver.{self.name}"

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(name)


@dataclass(frozen=True)
class Contract:
    source: str
    version: int
    domain: str
    description: str
    cadence: str
    freshness_sla: str
    upstream: tuple[str, ...]
    tables: tuple[Table, ...] = field(default_factory=tuple)

    def table(self, name: str) -> Table:
        for t in self.tables:
            if t.name == name:
                return t
        raise ContractError(f"{self.source} has no table {name}")


def _ident(value: str, what: str) -> str:
    if not isinstance(value, str) or not IDENT.match(value):
        raise ContractError(f"bad {what} name: {value!r}")
    return value


def _column(raw: dict, table: str) -> Column:
    try:
        col = Column(
            name=_ident(raw["name"], "column"),
            type=str(raw["type"]).upper(),
            description=raw["description"],
            nullable=bool(raw.get("nullable", True)),
            unit=raw.get("unit"),
            min=raw.get("min"),
            max=raw.get("max"),
            accepted=tuple(raw["accepted"]) if raw.get("accepted") is not None else None,
            pattern=raw.get("pattern"),
            max_null_rate=raw.get("max_null_rate"),
        )
    except KeyError as exc:
        raise ContractError(f"{table}: column is missing {exc}") from exc
    if not col.description.strip():
        raise ContractError(f"{table}.{col.name}: description is empty")
    return col


def _table(raw: dict) -> Table:
    name = _ident(raw.get("name"), "table")
    columns = tuple(_column(c, name) for c in raw.get("columns", []))
    names = [c.name for c in columns]
    if len(names) != len(set(names)):
        raise ContractError(f"{name}: duplicate column names")
    if any(n.startswith("_") for n in names):
        raise ContractError(f"{name}: leading underscore is reserved for envelope columns")

    t = Table(
        name=name,
        description=raw["description"],
        grain=raw["grain"],
        primary_key=tuple(raw.get("primary_key", [])),
        write=raw["write"],
        columns=columns,
        version_column=raw.get("version_column"),
        event_time=raw.get("event_time"),
        partition_by=tuple(raw.get("partition_by", [])),
        min_rows=int(raw.get("min_rows", 0)),
        max_reject_rate=float(raw.get("max_reject_rate", 0.0)),
    )
    if t.write not in WRITE_MODES:
        raise ContractError(f"{name}: write must be one of {sorted(WRITE_MODES)}")
    if t.write in {"merge", "append_new"} and not t.primary_key:
        raise ContractError(f"{name}: {t.write} needs a primary_key")
    for ref in (*t.primary_key, t.version_column, t.event_time):
        if ref and ref not in names:
            raise ContractError(f"{name}: {ref} is not a column")
    for key in t.primary_key:
        if t.column(key).nullable:
            raise ContractError(f"{name}: key column {key} must be nullable: false")
    return t


def parse(raw: dict) -> Contract:
    try:
        contract = Contract(
            source=_ident(raw["source"], "source"),
            version=int(raw["version"]),
            domain=raw["domain"],
            description=raw["description"],
            cadence=raw["cadence"],
            freshness_sla=raw["freshness_sla"],
            upstream=tuple(raw.get("upstream", [])),
            tables=tuple(_table(t) for t in raw["tables"]),
        )
    except KeyError as exc:
        raise ContractError(f"contract is missing {exc}") from exc
    return contract


def load(path: Path) -> Contract:
    with path.open() as f:
        return parse(yaml.safe_load(f))


def load_all(directory: Path) -> dict[str, Contract]:
    out = {}
    for path in sorted(directory.glob("*.yaml")):
        c = load(path)
        if c.source in out:
            raise ContractError(f"source {c.source} defined twice")
        out[c.source] = c
    return out
