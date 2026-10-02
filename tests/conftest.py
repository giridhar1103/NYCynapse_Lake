import os

import pytest

from nycynapse_lake import lake
from nycynapse_lake.config import Settings
from nycynapse_lake.control.db import Control

TEST_DSN = os.environ.get("NYC_LAKE_TEST_PG_DSN")


@pytest.fixture
def settings(tmp_path):
    if not TEST_DSN:
        pytest.skip("NYC_LAKE_TEST_PG_DSN is not set")
    return Settings(
        pg_dsn=TEST_DSN,
        data_path=tmp_path / "lake",
        tmp_path=tmp_path / "tmp",
        spool_path=tmp_path / "spool",
        memory_limit="512MB",
        catalog_override=str(tmp_path / "catalog.ducklake"),
    )


@pytest.fixture
def ctl(settings):
    c = Control(settings.pg_dsn)
    c.conn.execute("DROP SCHEMA IF EXISTS ops CASCADE")
    c.migrate()
    yield c
    c.close()


@pytest.fixture
def con(settings):
    c = lake.connect(settings)
    lake.ensure_schemas(c)
    yield c
    c.close()
