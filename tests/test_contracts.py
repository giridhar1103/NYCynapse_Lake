import pytest

from nycynapse_lake.config import REPO_CONTRACTS
from nycynapse_lake.contracts import ContractError, load_all, parse


def base(**table):
    t = {
        "name": "thing",
        "description": "things",
        "grain": "one row per thing",
        "primary_key": ["id"],
        "write": "merge",
        "columns": [
            {"name": "id", "type": "integer", "nullable": False, "description": "id"},
            {"name": "label", "type": "varchar", "description": "label"},
        ],
    }
    t.update(table)
    return {
        "source": "demo",
        "version": 1,
        "domain": "test",
        "description": "demo source",
        "cadence": "daily",
        "freshness_sla": "2 days",
        "tables": [t],
    }


def test_parses_and_normalises_types():
    c = parse(base())
    assert c.table("thing").column("id").type == "integer"
    assert c.table("thing").qualified == "lake.silver.thing"


def test_rejects_unknown_write_mode():
    with pytest.raises(ContractError):
        parse(base(write="upsert"))


def test_merge_needs_a_key():
    with pytest.raises(ContractError):
        parse(base(primary_key=[]))


def test_key_columns_must_be_required():
    raw = base()
    raw["tables"][0]["columns"][0]["nullable"] = True
    with pytest.raises(ContractError):
        parse(raw)


def test_reserved_column_prefix():
    raw = base()
    raw["tables"][0]["columns"].append({"name": "_x", "type": "int", "description": "x"})
    with pytest.raises(ContractError):
        parse(raw)


def test_every_column_needs_a_description():
    raw = base()
    raw["tables"][0]["columns"][1]["description"] = " "
    with pytest.raises(ContractError):
        parse(raw)


def test_repo_contracts_are_valid():
    found = load_all(REPO_CONTRACTS)
    assert "tlc_zones" in found


def test_unquoted_comma_in_a_description_is_caught():
    import yaml

    raw = base()
    raw["tables"][0]["columns"] = yaml.safe_load(
        "- {name: id, type: integer, nullable: false, description: Id, unique per thing.}"
    )
    with pytest.raises(ContractError, match="unknown keys"):
        parse(raw)
