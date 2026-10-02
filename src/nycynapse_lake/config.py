import os
from dataclasses import dataclass
from pathlib import Path

REPO_CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"


@dataclass(frozen=True)
class Settings:
    pg_dsn: str
    data_path: Path
    tmp_path: Path
    spool_path: Path
    memory_limit: str = "1GB"
    threads: int = 2
    socrata_app_token: str | None = None
    contracts_path: Path = REPO_CONTRACTS
    # A test run can point the lake at a local DuckDB catalog instead of Postgres.
    catalog_override: str | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        dsn = os.environ.get("NYC_LAKE_PG_DSN")
        if not dsn:
            raise RuntimeError("NYC_LAKE_PG_DSN is not set")
        return cls(
            pg_dsn=dsn,
            data_path=Path(os.environ.get("NYC_LAKE_DATA_PATH", "/srv/nycynapse/lake")),
            tmp_path=Path(os.environ.get("NYC_LAKE_TMP_PATH", "/srv/nycynapse/tmp")),
            spool_path=Path(os.environ.get("NYC_LAKE_SPOOL_PATH", "/srv/nycynapse/spool")),
            memory_limit=os.environ.get("NYC_LAKE_MEMORY_LIMIT", "1GB"),
            threads=int(os.environ.get("NYC_LAKE_THREADS", "2")),
            socrata_app_token=os.environ.get("SOCRATA_APP_TOKEN") or None,
            contracts_path=Path(os.environ.get("NYC_LAKE_CONTRACTS", REPO_CONTRACTS)),
        )
