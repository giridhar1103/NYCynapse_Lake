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
    p = sub.add_parser("purge-quarantine", help="drop quarantined rows older than N days")
    p.add_argument("--days", type=int, default=30)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

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

    if args.cmd == "purge-quarantine":
        ctl = Control(settings.pg_dsn)
        print("removed", ctl.purge_quarantine(args.days))
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
