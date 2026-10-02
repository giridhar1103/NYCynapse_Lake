import argparse
import logging
import sys

from . import contracts, lake
from .breaker import CircuitOpen
from .config import Settings
from .control.db import Control
from .runs import SourceBusy, run_source
from .sources import MODULES, runner


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
    po.add_argument("source", choices=["subway_realtime"])
    m = sub.add_parser("maintain", help="compact files, expire old snapshots, purge quarantine")
    m.add_argument("--keep-days", type=int, default=30)
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
        from .realtime.poller import SubwayPoller

        contract = contracts.load(settings.contracts_path / f"{args.source}.yaml")
        SubwayPoller(settings, contract).run_forever()
        return 0

    if args.cmd == "maintain":
        from . import maintain

        ctl = Control(settings.pg_dsn)
        con = lake.connect(settings)
        report = maintain.run(con, ctl, keep_days=args.keep_days)
        print({k: v for k, v in report.items() if k != "merged"})
        return 0

    if args.cmd == "run":
        contract = contracts.load(settings.contracts_path / f"{args.source}.yaml")
        try:
            with run_source(contract, settings) as ctx:
                runner(args.source)(ctx, force=args.force)
        except (SourceBusy, CircuitOpen) as exc:
            print(exc, file=sys.stderr)
            return 75  # EX_TEMPFAIL, so systemd and Prefect treat it as "try later"
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
