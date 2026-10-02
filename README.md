# NYCynapse Lake

The data side of NYCynapse. It pulls New York City transport and city data on each source's own schedule, checks it against a contract, and writes typed tables into a [DuckLake](https://ducklake.select/) lakehouse. The query side lives in [NYCynapse.Ai](https://github.com/giridhar1103/NYCynapse.Ai).

History starts in January 2024 and grows from there.

## Sources

| Domain | Source | Cadence |
|---|---|---|
| Rideshare and taxi | TLC high volume FHV, yellow and green trip records, taxi zones | monthly |
| Bike | Citi Bike trip history, Citi Bike GBFS station feeds | monthly, every minute |
| Subway | MTA GTFS schedule, GTFS-realtime trip updates, service alerts, hourly ridership | weekly, every 30 seconds, every minute, weekly |
| City services | NYC 311 service requests | daily |
| Safety | NYPD motor vehicle collisions | daily |
| Traffic | NYC DOT traffic speeds | every few minutes |
| Weather | Open-Meteo hourly weather, National Weather Service alerts | hourly, every 5 minutes |
| Geography | NTAs, community districts, ZIP areas, council districts, precincts | checked monthly |

Geography, weather, weather alerts, 311 and collisions are live. Transit, TLC trips, Citi Bike and traffic come next.

| Table | Rows (October 2026) |
|---|---|
| `requests_311` | 10.1 million |
| `collision_crashes`, `collision_persons` | 213 thousand, 733 thousand |
| `weather_hourly` | 120 thousand |
| `weather_alerts` | 1.1 thousand |
| boundary tables | 975 polygons |

## How a load works

1. **Fetch.** HTTP calls retry with exponential backoff and jitter, honour `Retry-After`, and use `ETag` and `Last-Modified` so an unchanged file is never downloaded twice. A circuit breaker per source stops a failing upstream from being hammered.
2. **Stage.** Files are streamed to a temporary path and read by DuckDB. Live feeds are parsed in memory. Nothing as-received is kept: the temporary file is deleted when the run ends, success or not.
3. **Check.** Each source has a contract in [`contracts/`](contracts) with types, keys, allowed values, ranges and batch rules. Rows that break a rule go to a quarantine table with the reason. A batch with too many bad rows or too few rows is stopped.
4. **Write.** Clean rows are written to `lake.silver` in one DuckLake transaction, using a write mode that makes repeats harmless: replace a table, replace a partition, merge on key with an optional version column, or insert only new keys.
5. **Checkpoint.** Cursors and watermarks are saved after the lake commit, never before. A crash in between means the next run repeats work, and the write mode turns that into a no-op.

Rows with coordinates are tagged during the load with their borough, neighborhood (NTA), community district, ZIP area, council district, police precinct and taxi zone. Queries then filter on plain codes instead of running spatial joins. To make tagging fast, every boundary is cut along a 0.01 degree grid once, so each point is tested against a few small pieces instead of whole precincts. A million points tag in about seven seconds on two cores.

Every row carries `_source`, `_load_id`, `_ingested_at` and `_contract_version`, and every commit is a DuckLake snapshot tagged with its load id. Any table can be read as it was at an earlier snapshot.

Since raw payloads are not stored, replay works in two ways. Batch sources are re-downloaded from upstream, and the exact file used for each load is recorded by URL, ETag and SHA-256. Live feeds have no upstream archive, so their silver tables keep every field the feed sends.

## Storage

* DuckLake catalog in Postgres, data as Parquet files on local disk
* `lake.silver`: one table per source entity, typed and deduplicated
* `lake.gold`: models built with dbt for querying
* Postgres schema `ops`: runs, checkpoints, upstream files, circuit breakers, quality results, quarantine, feed gaps and table coverage

## Scheduling

Each source runs as a one-shot systemd service on its own timer: alerts every 5 minutes, weather hourly, 311 and collisions daily, boundaries monthly. A service exits with code 75 when its source is already running or its circuit breaker is open, and systemd treats that as success. A daily maintenance job compacts small files, expires snapshots older than 30 days (except ones pinned in `ops.pinned_snapshots`) and purges old quarantine rows.

```bash
sudo ./deploy/install.sh
systemctl list-timers 'nyc-lake-*'
```

## Running it

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp .env.example .env   # then fill in the Postgres DSN
set -a; . ./.env; set +a

.venv/bin/nyc-lake migrate
.venv/bin/nyc-lake check-contracts
.venv/bin/nyc-lake run tlc_zones
.venv/bin/nyc-lake sources        # list what can run
.venv/bin/nyc-lake maintain
```

Tests need a Postgres database for the control tables:

```bash
export NYC_LAKE_TEST_PG_DSN="dbname=nycynapse_lake_test host=127.0.0.1 user=nyc_lake password=..."
.venv/bin/pytest -q
```

## Layout

```
contracts/                 one YAML contract per source
src/nycynapse_lake/
  lake.py                  DuckDB connection attached to the DuckLake catalog
  geo.py                   boundary pieces and point tagging
  socrata.py               keyset paging for NYC Open Data and data.ny.gov
  http.py retry.py         fetching, backoff, conditional requests
  breaker.py               per-source circuit breaker
  contracts.py quality.py  contract parsing, row and batch checks
  writer.py                idempotent writes and schema evolution
  runs.py                  run lifecycle, checkpoints, coverage
  spool.py                 local buffer for live feeds when the lake is unavailable
  maintain.py              compaction, snapshot expiry, quarantine purge
  control/                 Postgres ops tables and migrations
  sources/                 one module per source
deploy/                    systemd services and timers
tests/
```
