import pytest

from flux.config import FluxConfig
from flux.exceptions import InvalidMigrationModuleError
from flux.migration.read_migration import read_python_migration, read_sql_migration

EXAMPLE_SQL_WITH_DOWN = "sql_example"
EXAMPLE_SQL_WITHOUT_DOWN = "sql_example_2"
EXAMPLE_PYTHON_DOWN_STR = "example_python_migration_down_str"
EXAMPLE_PYTHON_DOWN_NONE = "example_python_migration_down_none"
EXAMPLE_PYTHON_DOWN_MISSING = "example_python_migration_down_missing"


INVALID_PYTHON_DOWN_INT = "invalid_python_migration_down_int"
INVALID_PYTHON_DOWN_RAISES = "invalid_python_migration_down_raises"
INVALID_PYTHON_UP_INT = "invalid_python_migration_up_int"
INVALID_PYTHON_UP_MISSING = "invalid_python_migration_up_missing"
INVALID_PYTHON_UP_RAISES = "invalid_python_migration_up_raises"

EXAMPLE_UP_TEXT = "create table example_table ( id serial primary key, name text );"
EXAMPLE_DOWN_TEXT = "drop table example_table;"


def test_read_sql_migration_with_down(in_memory_config: FluxConfig):
    migration = read_sql_migration(
        config=in_memory_config,
        migration_id=EXAMPLE_SQL_WITH_DOWN,
    )
    assert migration.up == f"{EXAMPLE_UP_TEXT}\n"
    assert migration.down == f"{EXAMPLE_DOWN_TEXT}\n"
    assert migration.up_hash == "a53b94e10e61374e5a20f9e361d638e2"


def test_read_sql_migration_without_down(in_memory_config: FluxConfig):
    migration = read_sql_migration(
        config=in_memory_config,
        migration_id=EXAMPLE_SQL_WITHOUT_DOWN,
    )
    assert migration.up == f"{EXAMPLE_UP_TEXT}\n"
    assert migration.down is None
    assert migration.up_hash == "a53b94e10e61374e5a20f9e361d638e2"


def test_read_sql_migration_not_exist(in_memory_config: FluxConfig):
    with pytest.raises(InvalidMigrationModuleError):
        read_sql_migration(config=in_memory_config, migration_id="fake")


def test_read_python_migration_down_str(in_memory_config: FluxConfig):
    migration = read_python_migration(
        config=in_memory_config,
        migration_id=EXAMPLE_PYTHON_DOWN_STR,
    )
    assert migration.up == f"{EXAMPLE_UP_TEXT}"
    assert migration.down == f"{EXAMPLE_DOWN_TEXT}"
    assert migration.up_hash == "2ea715441b43f1bdc8428238385bb592"


def test_read_python_migration_down_none(in_memory_config: FluxConfig):
    migration = read_python_migration(
        config=in_memory_config,
        migration_id=EXAMPLE_PYTHON_DOWN_NONE,
    )
    assert migration.up == f"{EXAMPLE_UP_TEXT}"
    assert migration.down is None
    assert migration.up_hash == "2ea715441b43f1bdc8428238385bb592"


def test_read_python_migration_down_missing(in_memory_config: FluxConfig):
    migration = read_python_migration(
        config=in_memory_config,
        migration_id=EXAMPLE_PYTHON_DOWN_MISSING,
    )
    assert migration.up == f"{EXAMPLE_UP_TEXT}"
    assert migration.down is None
    assert migration.up_hash == "2ea715441b43f1bdc8428238385bb592"


@pytest.mark.parametrize(
    "migration_id",
    [
        INVALID_PYTHON_DOWN_INT,
        INVALID_PYTHON_DOWN_RAISES,
        INVALID_PYTHON_UP_INT,
        INVALID_PYTHON_UP_MISSING,
        INVALID_PYTHON_UP_RAISES,
    ],
)
def test_read_python_migration_invalid(in_memory_config: FluxConfig, migration_id: str):
    with pytest.raises(InvalidMigrationModuleError):
        read_python_migration(config=in_memory_config, migration_id=migration_id)
