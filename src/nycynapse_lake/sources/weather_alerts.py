"""Weather Service alerts for the NYC forecast zones."""

import re
from datetime import UTC, date, datetime

import pyarrow as pa

from ..runs import RunContext

ACTIVE = "https://api.weather.gov/alerts/active"
ARCHIVE = "https://mesonet.agron.iastate.edu/json/vtec_events_byugc.php"
START = date(2024, 1, 1)
ZONES = {"NYZ072": 1, "NYZ073": 2, "NYZ075": 3, "NYZ176": 4, "NYZ178": 4, "NYZ074": 5}
HEADERS = {"Accept": "application/geo+json"}
VTEC = re.compile(
    r"/[OTEX]\.(?P<action>[A-Z]{3})\.K?(?P<office>[A-Z]{3})\.(?P<phen>[A-Z]{2})\."
    r"(?P<sig>[A-Z])\.(?P<num>\d{4})\.(?P<begin>\d{6}T\d{4}Z)-(?P<end>\d{6}T\d{4}Z)/"
)

COLUMNS = [
    "office",
    "phenomena",
    "significance",
    "event_number",
    "event_year",
    "zone",
    "boro_code",
    "event_name",
    "starts_at",
    "ends_at",
    "last_action",
    "severity",
    "certainty",
    "urgency",
    "headline",
    "captured_from",
    "source_updated_at",
]


def _vtec_time(value: str) -> datetime | None:
    if value.startswith("000000"):
        return None
    return datetime.strptime(value, "%y%m%dT%H%MZ").replace(tzinfo=UTC)


def _iso(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value).astimezone(UTC) if value else None


def live_rows(payload: dict) -> list[dict]:
    rows = []
    for feature in payload.get("features", []):
        p = feature["properties"]
        if p.get("status") != "Actual":
            continue
        zones = [z.rsplit("/", 1)[-1] for z in p.get("affectedZones", [])]
        sent = _iso(p.get("sent"))
        for vtec in p.get("parameters", {}).get("VTEC", []):
            m = VTEC.match(vtec)
            if not m:
                continue
            begin, end = _vtec_time(m["begin"]), _vtec_time(m["end"])
            action = m["action"]
            if action in {"CAN", "EXP"}:
                end = sent
            for zone in zones:
                if zone not in ZONES:
                    continue
                rows.append(
                    {
                        "office": m["office"],
                        "phenomena": m["phen"],
                        "significance": m["sig"],
                        "event_number": int(m["num"]),
                        "event_year": (begin or sent).year,
                        "zone": zone,
                        "boro_code": ZONES[zone],
                        "event_name": p.get("event"),
                        "starts_at": begin or _iso(p.get("onset")) or sent,
                        "ends_at": end or _iso(p.get("ends")) or _iso(p.get("expires")),
                        "last_action": action,
                        "severity": p.get("severity"),
                        "certainty": p.get("certainty"),
                        "urgency": p.get("urgency"),
                        "headline": p.get("headline"),
                        "captured_from": "live",
                        "source_updated_at": sent,
                    }
                )
    return rows


def archive_rows(payload: dict, zone: str) -> list[dict]:
    generated = _iso(payload.get("generated_at")) or datetime.now(UTC)
    rows = []
    for e in payload.get("events", []):
        year = re.search(r"year=(\d{4})", e.get("url", ""))
        issued = _iso(e["issue"])
        rows.append(
            {
                "office": e["wfo"],
                "phenomena": e["phenomena"],
                "significance": e["significance"],
                "event_number": int(e["eventid"]),
                "event_year": int(year.group(1)) if year else issued.year,
                "zone": zone,
                "boro_code": ZONES[zone],
                "event_name": e["name"],
                "starts_at": issued,
                "ends_at": _iso(e.get("expire")),
                "last_action": None,
                "severity": None,
                "certainty": None,
                "urgency": None,
                "headline": None,
                "captured_from": "archive",
                "source_updated_at": generated,
            }
        )
    return rows


def _table(rows: list[dict]) -> pa.Table:
    return (
        pa.Table.from_pylist(rows)
        if rows
        else pa.table({c: pa.array([], pa.string()) for c in COLUMNS})
    )


def run(ctx: RunContext, *, force: bool = False) -> None:
    if not ctx.checkpoint("archive_loaded"):
        today = date.today().isoformat()
        rows = []
        for zone in ZONES:
            payload = ctx.http.get_json(
                ARCHIVE, params={"ugc": zone, "sdate": START.isoformat(), "edate": today}
            )
            rows += archive_rows(payload, zone)
        ctx.load("weather_alerts", _table(rows))
        ctx.save("archive_loaded", today)

    payload = ctx.http.get_json(ACTIVE, params={"zone": ",".join(ZONES)}, headers=HEADERS)
    rows = live_rows(payload)
    ctx.detail["active_messages"] = len(payload.get("features", []))
    if rows:
        ctx.load("weather_alerts", _table(rows))
