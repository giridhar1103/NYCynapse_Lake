"""Source registry. Each module defines a run(ctx) function and the contract it fills."""

from collections.abc import Callable
from importlib import import_module

from ..runs import RunContext

MODULES = {
    "citibike_trips": "nycynapse_lake.sources.citibike_trips",
    "tlc_zones": "nycynapse_lake.sources.tlc_zones",
    "tlc_trips": "nycynapse_lake.sources.tlc_trips",
    "nyc_geo": "nycynapse_lake.sources.nyc_geo",
    "nyc_311": "nycynapse_lake.sources.nyc_311",
    "nyc_collisions": "nycynapse_lake.sources.nyc_collisions",
    "mta_gtfs": "nycynapse_lake.sources.mta_gtfs",
    "mta_ridership": "nycynapse_lake.sources.mta_ridership",
    "weather": "nycynapse_lake.sources.weather",
    "weather_alerts": "nycynapse_lake.sources.weather_alerts",
}


def runner(source: str) -> Callable[..., None]:
    if source not in MODULES:
        raise KeyError(f"unknown source {source}; known: {sorted(MODULES)}")
    return import_module(MODULES[source]).run


__all__ = ["RunContext", "runner", "MODULES"]
