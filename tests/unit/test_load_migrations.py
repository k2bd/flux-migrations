import os

import pytest

from flux.exceptions import InvalidMigrationModule
from flux.migration.migration import read_python_migration, read_sql_migration
from tests.unit.constants import DATA_DIR

EXAMPLE_SQL_UP = os.path.join(DATA_DIR, "example_up.sql")
EXAMPLE_SQL_DOWN = os.path.join(DATA_DIR, "example_down.sql")
EXAMPLE_PYTHON_DOWN_STR = os.path.join(DATA_DIR, "example_python_migration_down_str.py")
EXAMPLE_PYTHON_DOWN_NONE = os.path.join(
    DATA_DIR, "example_python_migration_down_none.py"
)
EXAMPLE_PYTHON_DOWN_MISSING = os.path.join(
    DATA_DIR, "example_python_migration_down_missing.py"
)


INVALID_PYTHON_DOWN_INT = os.path.join(DATA_DIR, "invalid_python_migration_down_int.py")
INVALID_PYTHON_DOWN_RAISES = os.path.join(
    DATA_DIR, "invalid_python_migration_down_raises.py"
)
INVALID_PYTHON_UP_INT = os.path.join(DATA_DIR, "invalid_python_migration_up_int.py")
INVALID_PYTHON_UP_MISSING = os.path.join(
    DATA_DIR, "invalid_python_migration_up_missing.py"
)
INVALID_PYTHON_UP_RAISES = os.path.join(
    DATA_DIR, "invalid_python_migration_up_raises.py"
)

EXAMPLE_UP_TEXT = "create table example_table ( id serial primary key, name text );"
EXAMPLE_DOWN_TEXT = "drop table example_table;"


def test_read_sql_migration_with_down():
    migration = read_sql_migration(EXAMPLE_SQL_UP, EXAMPLE_SQL_DOWN)
    assert migration.up == f"{EXAMPLE_UP_TEXT}\n"
    assert migration.down == f"{EXAMPLE_DOWN_TEXT}\n"
    assert migration.up_hash == "a53b94e10e61374e5a20f9e361d638e2"


def test_read_sql_migration_without_down():
    migration = read_sql_migration(EXAMPLE_SQL_UP, None)
    assert migration.up == f"{EXAMPLE_UP_TEXT}\n"
    assert migration.down is None
    assert migration.up_hash == "a53b94e10e61374e5a20f9e361d638e2"


def test_read_sql_migration_with_up_not_exist():
    with pytest.raises(InvalidMigrationModule):
        read_sql_migration("fake.sql", EXAMPLE_SQL_DOWN)


def test_read_sql_migration_with_down_not_exist():
    with pytest.raises(InvalidMigrationModule):
        read_sql_migration(EXAMPLE_SQL_UP, "fake.sql")


def test_read_python_migration_down_str():
    migration = read_python_migration(EXAMPLE_PYTHON_DOWN_STR)
    assert migration.up == f"{EXAMPLE_UP_TEXT}"
    assert migration.down == f"{EXAMPLE_DOWN_TEXT}"
    assert migration.up_hash == "2ea715441b43f1bdc8428238385bb592"


def test_read_python_migration_down_none():
    migration = read_python_migration(EXAMPLE_PYTHON_DOWN_NONE)
    assert migration.up == f"{EXAMPLE_UP_TEXT}"
    assert migration.down is None
    assert migration.up_hash == "2ea715441b43f1bdc8428238385bb592"


def test_read_python_migration_down_missing():
    migration = read_python_migration(EXAMPLE_PYTHON_DOWN_MISSING)
    assert migration.up == f"{EXAMPLE_UP_TEXT}"
    assert migration.down is None
    assert migration.up_hash == "2ea715441b43f1bdc8428238385bb592"


@pytest.mark.parametrize(
    "migration_file",
    [
        INVALID_PYTHON_DOWN_INT,
        INVALID_PYTHON_DOWN_RAISES,
        INVALID_PYTHON_UP_INT,
        INVALID_PYTHON_UP_MISSING,
        INVALID_PYTHON_UP_RAISES,
    ],
)
def test_read_python_migration_invalid(migration_file):
    with pytest.raises(InvalidMigrationModule):
        read_python_migration(migration_file)
