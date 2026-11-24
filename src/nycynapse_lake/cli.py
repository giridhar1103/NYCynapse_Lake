import argparse
import logging
import sys

from . import contracts, lake
from .breaker import CircuitOpen
from .config import Settings
from .contracts import ContractError
from .control.db import Control
from .quality import QualityFailure
from .runs import SourceBusy, run_source
from .sources import MODULES, runner


def _job(name: str, settings: Settings, work) -> int:
    """Run a housekeeping job and keep its outcome, so a job that stops running is noticed."""
    ctl = Control(settings.pg_dsn)
    try:
        detail = work(ctl)
    except Exception as exc:
        ctl.record_job(name, False, {"error": f"{type(exc).__name__}: {exc}"[:1000]})
        raise
    ctl.record_job(name, True, detail)
    print(name, detail)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nyc-lake")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="apply control plane migrations and create lake schemas")
    sub.add_parser("check-contracts", help="parse every contract and report problems")
    sub.add_parser("sources", help="list registered sources")
    r = sub.add_parser("run", help="run one source")
    r.add_argument("source")
    r.add_argument("--force", action="store_true", help="reload even if upstream is unchanged")
    po = sub.add_parser("poll", help="run a long-lived poller for a live source")
    po.add_argument("source", choices=["subway_realtime", "citibike_live"])
    m = sub.add_parser("maintain", help="compact files, expire old snapshots, purge quarantine")
    m.add_argument("--keep-days", type=int, default=30)
    sub.add_parser("health", help="look for problems and keep them in ops.incidents")
    sub.add_parser("backup", help="back up the catalog, the control tables and live captures")
    sub.add_parser("verify", help="check that every file the lake needs is on disk")
    sub.add_parser("restore-check", help="restore the newest backup into a scratch database")
    sub.add_parser("offsite", help="copy the backups off this machine, encrypted")
    sub.add_parser("offsite-check", help="restore the newest off-site backup for real")
    rd = sub.add_parser("replay-dead", help="put set-aside live batches back in the queue")
    rd.add_argument("source", choices=["subway_realtime", "citibike_live"])
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # One line per HTTP request drowns the journal once pollers run every 30 seconds.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    if args.cmd == "check-contracts":
        found = contracts.load_all(Settings.from_env().contracts_path)
        for name, c in found.items():
            print(f"{name}: v{c.version}, {len(c.tables)} table(s)")
        return 0

    settings = Settings.from_env()
    if args.cmd == "migrate":
        ctl = Control(settings.pg_dsn)
        print("applied:", ctl.migrate() or "nothing new")
        ctl.close()
        con = lake.connect(settings)
        lake.ensure_schemas(con)
        con.close()
        return 0

    if args.cmd == "sources":
        for name in sorted(MODULES):
            print(name)
        return 0

    if args.cmd == "poll":
        from .realtime.gbfs import GbfsPoller
        from .realtime.poller import SubwayPoller

        contract = contracts.load(settings.contracts_path / f"{args.source}.yaml")
        poller = {"subway_realtime": SubwayPoller, "citibike_live": GbfsPoller}[args.source]
        poller(settings, contract).run_forever()
        return 0

    if args.cmd == "maintain":
        from . import maintain

        def job(ctl):
            con = lake.connect(settings)
            report = maintain.run(con, ctl, keep_days=args.keep_days)
            return {k: v for k, v in report.items() if k != "merged"}

        return _job("maintain", settings, job)

    if args.cmd in ("offsite", "offsite-check"):
        from . import offsite

        if not offsite.ready():
            print("off-site storage is not signed in yet")
            return 75
        work = offsite.run if args.cmd == "offsite" else offsite.check
        return _job(args.cmd, settings, lambda ctl: work(settings))

    if args.cmd in ("backup", "verify", "restore-check"):
        from . import backup

        def job(ctl):
            if args.cmd == "backup":
                return backup.run(settings)
            if args.cmd == "restore-check":
                return backup.restore_check(settings)
            return backup.verify(lake.connect(settings, read_only=True))

        return _job(args.cmd, settings, job)

    if args.cmd == "health":
        from . import health

        ctl = Control(settings.pg_dsn)
        for p in health.run(settings, ctl):
            print(f"{p.severity:5} {p.key}: {p.message}")
        return 0

    if args.cmd == "replay-dead":
        from .spool import Spool

        print(Spool(settings.spool_path, args.source).revive(), "batches queued again")
        return 0

    if args.cmd == "run":
        contract = contracts.load(settings.contracts_path / f"{args.source}.yaml")
        try:
            with run_source(contract, settings, force=args.force) as ctx:
                runner(args.source)(ctx, force=args.force)
        except (SourceBusy, CircuitOpen) as exc:
            print(exc, file=sys.stderr)
            return 75  # EX_TEMPFAIL, so systemd treats it as "try later"
        except (QualityFailure, ContractError) as exc:
            print(exc, file=sys.stderr)
            return 65  # EX_DATAERR: the data is wrong, so trying again soon will not help
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
