# NYCynapse Lake

The data side of NYCynapse. It pulls New York City transport and city data on each source's own schedule, checks it against a contract, and writes typed tables into a [DuckLake](https://ducklake.select/) lakehouse. The query side lives in [NYCynapse.Ai](https://github.com/giridhar1103/NYCynapse.Ai).

History starts in January 2024 and grows from there.

## Sources

| Domain | Source | Cadence |
|---|---|---|
| Rideshare and taxi | TLC high volume FHV, yellow and green trip records, taxi zones | monthly |
| Bike | Citi Bike trip history, Citi Bike GBFS station feeds | monthly, every minute |
| Subway | MTA GTFS schedule, GTFS-realtime trip updates, service alerts, hourly ridership, station list | daily check, every 30 seconds, every minute, weekly |
| City services | NYC 311 service requests | daily |
| Safety | NYPD motor vehicle collisions | daily |
| Traffic | NYC DOT traffic speeds | every few minutes |
| Weather | Open-Meteo hourly weather, National Weather Service alerts | hourly, every 5 minutes |
| Geography | NTAs, community districts, ZIP areas, council districts, precincts | checked monthly |

Geography, weather, weather alerts, 311, collisions, the subway schedule, live subway movements, subway alerts, ridership and TLC trips are live. Citi Bike and traffic come next.

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

Since raw payloads are not stored, replay works in two ways. Batch sources are re-downloaded from upstream, and the exact file used for each load is recorded by URL, ETag and SHA-256. Live feeds have no upstream archive. Alert and station feeds keep every field they send. Subway realtime is different: it sends a full set of predictions for every train every 30 seconds, and keeping those would mean hundreds of millions of rows a day that say almost the same thing. The tracker keeps what they resolve to instead, one row per train per stop.

## Subway realtime

The MTA feed never says a train arrived. It lists, for every active train, the stops it has not left yet and when it is expected at each. The poller reads all eight feeds every 30 seconds and keeps each train's stops in memory. When a stop drops off a train's list, the train has left it:

* if the last prediction for that stop was already due, or the train reported standing there, it **arrived**
* if the stop vanished while still in the future, it was **skipped** (reroutes, trains running express)
* when a whole trip leaves the feed, its remaining stops are **unreached**, and a one-row trip summary is written

Rows are written every five minutes as one run. If the write fails they go to a local spool and are replayed on the next flush, and trips in progress are saved to disk, so a restart or a deploy loses nothing. Outages of a feed or of the poller itself are written to `ops.feed_gaps`.

## Gold layer

`dbt/` builds the tables meant for querying into `lake.gold`. Every model and column is described in the YAML next to it, and those descriptions feed the semantic catalog in NYCynapse.Ai.

| Kind | Models |
|---|---|
| Dimensions | `dim_date`, `dim_borough`, `dim_neighborhood`, `dim_community_district`, `dim_taxi_zone`, `dim_subway_route`, `dim_subway_station`, `dim_subway_complex`, `dim_bike_station`, `dim_traffic_link` |
| Facts | `fct_rideshare_trip`, `fct_taxi_trip`, `fct_service_request`, `fct_collision`, `fct_collision_person`, `fct_weather_hourly`, `fct_weather_alert`, `fct_subway_arrival`, `fct_subway_stop_event`, `fct_subway_trip`, `fct_subway_alert`, `fct_subway_ridership_hourly`, `fct_bike_trip`, `fct_bike_station_status`, `fct_traffic_speed` |
| Aggregates | `agg_trips_zone_hourly` |
| Pipeline | `ops_source_freshness`, `ops_feed_gap` |

The large trip facts are views over silver, so the 18 GB of trip files is not stored twice. `fct_subway_arrival` matches each observed arrival to the schedule in force that day, through the service calendar, and computes `delay_seconds`.

Every fact has an instant column such as `pickup_at` and New York local columns such as `pickup_date` and `pickup_hour`. Group by the local columns, but filter time on the instant column with local boundaries:

```sql
where pickup_at >= timezone('America/New_York', timestamp '2025-06-01')
  and pickup_at <  timezone('America/New_York', timestamp '2025-07-01')
```

On the 651 million app trips, that filter answers a month in half a second because whole files are skipped. The same filter written on `pickup_date` reads every file and takes three minutes.

The ops tables refresh every 15 minutes, subway arrivals and weather hourly, and the full build with tests runs once a day after the nightly loads.

```bash
cd dbt && DBT_PROFILES_DIR=. ../.venv-dbt/bin/dbt build --selector daily
```

dbt runs in its own virtualenv (`.venv-dbt`) because it pins an older protobuf than the GTFS-realtime bindings need.

## Storage

* DuckLake catalog in Postgres, data as Parquet files on local disk
* `lake.silver`: one table per source entity, typed and deduplicated
* `lake.gold`: models built with dbt for querying
* Postgres schema `ops`: runs, checkpoints, upstream files, circuit breakers, quality results, quarantine, feed gaps and table coverage

## Scheduling

Each batch source runs as a one-shot systemd service on its own timer: weather alerts every 5 minutes, weather hourly, 311, collisions, TLC and the subway schedule daily, ridership weekly, boundaries monthly. The subway poller runs as a long-lived service that restarts itself. A service exits with code 75 when its source is already running or its circuit breaker is open, and systemd treats that as success. A daily maintenance job compacts small files, expires snapshots older than 30 days (except ones pinned in `ops.pinned_snapshots`) and purges old quarantine rows.

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
  realtime/                subway train tracker and the poller that runs it
  maintain.py              compaction, snapshot expiry, quarantine purge
  control/                 Postgres ops tables and migrations
  sources/                 one module per source
deploy/                    systemd services and timers
tests/
```
