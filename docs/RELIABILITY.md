# How the lake stays right when a source does not

Sources go down, answer with error pages, cut files off, change ids, publish future dates and go
quiet. This page lists what happens then, and how to recover.

## Loading

| When a source... | The lake... |
|---|---|
| is unreachable or answers 429 or 5xx | retries with jittered backoff and honours Retry-After, then fails the run; three failures open a circuit breaker for that source |
| answers 200 with an error page, or a cut-off body | treats it as a failure and retries (JSON, Socrata pages, zip and Parquet files are checked end to end) |
| has not published a file yet | knows it only from 403, 404 or 410; a server error is never taken for "not yet" |
| sends rows that break the contract | sets those rows aside in `ops.quarantine` with the reason and loads the rest, up to the table's limit; past it, nothing from that batch is loaded |
| sends far fewer rows for a table or month that is replaced whole | keeps the old rows (more than 20% fewer is refused; `nyc-lake run SOURCE --force` overrides) |
| adds a column | loads without it and records the drift; a missing or retyped column stops the load |
| dates a row in the future | does not let it move the checkpoint or hide that the feed stopped |
| fails for one month, file or dataset | still loads the others; the run ends as failed and names the part |

Every write is one transaction, and repeating a load changes nothing, so a crash at any point is
recovered by running again. Checkpoints move only after the data is committed. A run left open by a
killed process is closed as failed by the next one. Nothing is written below 5 GB of free disk. A
failed nightly load tries again after 20 minutes, three times; a load whose data was refused does not.

## Live feeds

Nobody publishes the history of a live feed, so a missed minute is gone.

* Every batch goes to a spool on disk before the lake and leaves it after the commit. If the lake is
  down the spool holds it.
* A batch the lake refuses for good goes to `spool/SOURCE/dead/` with the reason, so it does not block
  later ones. After fixing the cause: `nyc-lake replay-dead SOURCE`.
* One bad feed or message does not stop the others. A feed that answers with a message more than five
  minutes old counts as down. Outages of 90 seconds or more are in `ops.feed_gaps`.
* A damaged state file is set aside and the poller starts fresh. A hung poller is restarted after ten
  minutes, and each restarts on its own about every 20 hours, writing what it holds first.

## Noticing

`nyc-lake health` runs every ten minutes and keeps one row per problem in `ops.incidents`: loads that
are late or keep failing, sources that answer but stopped publishing (`data_sla` in the contract),
runs with far fewer rows than usual, spool backlogs, dead batches, low disk, and housekeeping that
failed or did not run. The site's data page shows them.

Load freshness (`freshness_sla`) and data freshness (`data_sla`) are separate on purpose. A source
that publishes two months late is fine as long as the load ran last night.

## Backups

| What | How often | Kept |
|---|---|---|
| DuckLake catalog and control tables | nightly | 14 |
| Poller state and spool | nightly | 14 |
| Second copy of the live-captured tables | weekly | 3 |
| Proof: newest dump restored into a scratch database, every table counted, files checked | weekly | |
| Every file of the current version present with the right size, 200 opened | daily | |

Data files are deleted eight days after nothing refers to them, so a catalog from the last week still
finds all of its files. Everything else in the lake can be reloaded from its source. All copies are on
the same machine: losing the disk loses the live captures.

### Recovering

* **A bad load.** Find the snapshot before it in `ducklake_snapshots('lake')`, read the table with
  `AT (VERSION => n)` and write it back, or run the source with `--force` once it is right again.
* **The catalog is lost or damaged.** Stop the pollers and timers, create an empty database with
  schemas `ducklake` and `ops`, `pg_restore --no-owner` the newest dump into it, point
  `NYC_LAKE_PG_DSN` at it, run `nyc-lake verify`, start everything. Batch sources catch up from their
  checkpoints.
* **A live table is damaged.** The weekly copy is in `exports/`; insert what is missing by key.
