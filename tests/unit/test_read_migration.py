import os

import pytest

from flux.exceptions import MigrationLoadingError
from flux.migration.migration import Migration
from flux.migration.read_migration import (
    read_migrations,
    read_post_apply_migrations,
    read_pre_apply_migrations,
    read_python_migration,
    read_repeatable_python_migration,
    read_repeatable_sql_migration,
    read_sql_migration,
)
from tests.unit.constants import DATA_DIR, MIGRATION_DIRS_DIR, MIGRATIONS_DIR
from tests.unit.helpers import in_memory_config

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

EXAMPLE_FULL_MIGRATIONS_DIR = os.path.join(MIGRATION_DIRS_DIR, "example-full")

EXAMPLE_UP_TEXT = "create table example_table ( id serial primary key, name text );"
EXAMPLE_DOWN_TEXT = "drop table example_table;"


def test_read_sql_migration_with_down():
    migration = read_sql_migration(
        config=in_memory_config(migration_directory=MIGRATIONS_DIR),
        migration_id=EXAMPLE_SQL_WITH_DOWN,
    )
    assert migration.up == f"{EXAMPLE_UP_TEXT}\n"
    assert migration.down == f"{EXAMPLE_DOWN_TEXT}\n"
    assert migration.up_hash == "a53b94e10e61374e5a20f9e361d638e2"


def test_read_sql_migration_without_down():
    migration = read_sql_migration(
        config=in_memory_config(migration_directory=MIGRATIONS_DIR),
        migration_id=EXAMPLE_SQL_WITHOUT_DOWN,
    )
    assert migration.up == f"{EXAMPLE_UP_TEXT}\n"
    assert migration.down is None
    assert migration.up_hash == "a53b94e10e61374e5a20f9e361d638e2"


def test_read_sql_migration_not_exist():
    with pytest.raises(MigrationLoadingError):
        read_sql_migration(
            config=in_memory_config(migration_directory=MIGRATIONS_DIR),
            migration_id="fake",
        )


def test_read_python_migration_down_str():
    migration = read_python_migration(
        config=in_memory_config(migration_directory=MIGRATIONS_DIR),
        migration_id=EXAMPLE_PYTHON_DOWN_STR,
    )
    assert migration.up == f"{EXAMPLE_UP_TEXT}"
    assert migration.down == f"{EXAMPLE_DOWN_TEXT}"
    assert migration.up_hash == "2ea715441b43f1bdc8428238385bb592"


def test_read_python_migration_down_none():
    migration = read_python_migration(
        config=in_memory_config(migration_directory=MIGRATIONS_DIR),
        migration_id=EXAMPLE_PYTHON_DOWN_NONE,
    )
    assert migration.up == f"{EXAMPLE_UP_TEXT}"
    assert migration.down is None
    assert migration.up_hash == "2ea715441b43f1bdc8428238385bb592"


def test_read_python_migration_down_missing():
    migration = read_python_migration(
        config=in_memory_config(migration_directory=MIGRATIONS_DIR),
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
def test_read_python_migration_invalid(migration_id: str):
    with pytest.raises(MigrationLoadingError):
        read_python_migration(
            config=in_memory_config(migration_directory=MIGRATIONS_DIR),
            migration_id=migration_id,
        )


def test_read_repeatable_sql_migration():
    migration = read_repeatable_sql_migration(
        config=in_memory_config(migration_directory=DATA_DIR),
        migration_subdir="single-migrations",
        migration_id="sql_example_2_up",
    )
    assert migration.up == f"{EXAMPLE_UP_TEXT}\n"
    assert migration.down is None
    assert migration.up_hash == "a53b94e10e61374e5a20f9e361d638e2"


def test_read_repeatable_python_migration():
    migration = read_repeatable_python_migration(
        config=in_memory_config(migration_directory=DATA_DIR),
        migration_subdir="single-migrations",
        migration_id="example_python_migration_down_missing",
    )
    assert migration.up == f"{EXAMPLE_UP_TEXT}"
    assert migration.down is None
    assert migration.up_hash == "2ea715441b43f1bdc8428238385bb592"


@pytest.mark.parametrize(
    "invalid_migration",
    [
        "example_python_migration_down_str",
        "example_python_migration_down_none",
    ],
)
def test_read_repeatable_python_migration_with_down(invalid_migration: str):
    """
    Repeatable python migrations can't have a down
    """
    with pytest.raises(MigrationLoadingError):
        read_repeatable_python_migration(
            config=in_memory_config(migration_directory=DATA_DIR),
            migration_subdir="single-migrations",
            migration_id=invalid_migration,
        )


def test_read_migrations():
    migrations = read_migrations(
        config=in_memory_config(migration_directory=EXAMPLE_FULL_MIGRATIONS_DIR)
    )
    assert migrations == [
        Migration(id="20200101_000_aaa", up="aaa up content", down=None),
        Migration(id="20200101_001_bbb", up="bbb up content", down="bbb down content"),
        Migration(id="20200102_000_ccc", up="ccc up content", down="ccc down content"),
        Migration(id="20200103_000_ddd", up="ddd up content", down=None),
    ]


def test_read_pre_apply_migrations():
    migrations = read_pre_apply_migrations(
        config=in_memory_config(migration_directory=EXAMPLE_FULL_MIGRATIONS_DIR)
    )
    assert migrations == [
        Migration(id="20200101_000_pre1", up="pre-migration 1 content", down=None),
        Migration(id="20200102_000_pre2", up="pre-migration 2 content", down=None),
        Migration(id="20200102_001_another", up="pre-migration 3 content", down=None),
    ]


def test_read_post_apply_migrations():
    migrations = read_post_apply_migrations(
        config=in_memory_config(migration_directory=EXAMPLE_FULL_MIGRATIONS_DIR)
    )
    assert migrations == [
        Migration(id="20200101_000_post1", up="post-migration 1 content", down=None),
        Migration(id="20200102_000_post2", up="post-migration 2 content", down=None),
        Migration(id="20200102_001_another", up="post-migration 3 content", down=None),
    ]
