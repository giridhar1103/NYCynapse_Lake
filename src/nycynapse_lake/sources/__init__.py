"""Source registry. Each module defines a run(ctx) function and the contract it fills."""

from collections.abc import Callable
from importlib import import_module

from ..runs import RunContext

MODULES = {
    "tlc_zones": "nycynapse_lake.sources.tlc_zones",
    "nyc_geo": "nycynapse_lake.sources.nyc_geo",
}


def runner(source: str) -> Callable[..., None]:
    if source not in MODULES:
        raise KeyError(f"unknown source {source}; known: {sorted(MODULES)}")
    return import_module(MODULES[source]).run


__all__ = ["RunContext", "runner", "MODULES"]
