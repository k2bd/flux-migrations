import os

import pytest

from flux.exceptions import MigrationApplyError, MigrationDirectoryCorruptedError
from flux.runner import FluxRunner
from tests.integration.postgres.backend import TestingPostgresBackend
from tests.integration.postgres.helpers import postgres_config


def _write_new_migration(migrations_dir: str):
    with open(
        os.path.join(migrations_dir, "20200103_001_add_info_to_new_table.sql"),
        "w",
    ) as f:
        f.write("alter table new_table add column new_col text;")
    with open(
        os.path.join(migrations_dir, "20200103_001_add_info_to_new_table.undo.sql"),
        "w",
    ) as f:
        f.write("alter table new_table drop column new_col;")


def _write_new_bad_migration(migrations_dir: str):
    """
    Write a migration that is invalid in that it tries to create a column that
    already exists
    """
    with open(
        os.path.join(migrations_dir, "20200103_001_add_info_to_new_table.sql"),
        "w",
    ) as f:
        f.write("alter table simple_table add column data text;")
    with open(
        os.path.join(migrations_dir, "20200103_001_add_info_to_new_table.undo.sql"),
        "w",
    ) as f:
        f.write("alter table simple_table drop column data;")


async def test_postgres_migrations_apply(
    postgres_backend: TestingPostgresBackend,
    example_migrations_dir: str,
):
    config = postgres_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations()

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    async with postgres_backend.connection():
        migrations_table_rows = await postgres_backend.get_all_rows("_flux_migrations")
        assert {m[0]: m[1] for m in migrations_table_rows} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        simple_table_info = await postgres_backend.table_info("simple_table")
        assert simple_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "data", "TEXT", 0, None, 0),
            (2, "description", "TEXT", 0, None, 0),
        ]

        another_table_info = await postgres_backend.table_info("another_table")
        assert another_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "value", "INTEGER", 0, None, 0),
            (2, "timestamp", "TEXT", 0, None, 0),
        ]

        new_table_info = await postgres_backend.table_info("new_table")
        assert new_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "info", "TEXT", 0, None, 0),
        ]

        view1_info = await postgres_backend.table_info("view1")
        assert view1_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "data", "TEXT", 0, None, 0),
        ]

        view2_info = await postgres_backend.table_info("view2")
        assert view2_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "value", "INTEGER", 0, None, 0),
        ]


async def test_postgres_migrations_apply_add_apply(
    postgres_backend: TestingPostgresBackend,
    example_migrations_dir: str,
):
    config = postgres_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.validate_applied_migrations()
        await runner.apply_migrations()

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    _write_new_migration(example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()
        await runner.apply_migrations()

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
            "20200103_001_add_info_to_new_table": "2494e84a32039ba7fff00496da758df4",
        }

    async with postgres_backend.connection():
        migrations_table_rows = await postgres_backend.get_all_rows("_flux_migrations")
        assert {m[0]: m[1] for m in migrations_table_rows} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
            "20200103_001_add_info_to_new_table": "2494e84a32039ba7fff00496da758df4",
        }

        simple_table_info = await postgres_backend.table_info("simple_table")
        assert simple_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "data", "TEXT", 0, None, 0),
            (2, "description", "TEXT", 0, None, 0),
        ]

        another_table_info = await postgres_backend.table_info("another_table")
        assert another_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "value", "INTEGER", 0, None, 0),
            (2, "timestamp", "TEXT", 0, None, 0),
        ]

        new_table_info = await postgres_backend.table_info("new_table")
        assert new_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "info", "TEXT", 0, None, 0),
            (2, "new_col", "TEXT", 0, None, 0),
        ]

        view1_info = await postgres_backend.table_info("view1")
        assert view1_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "data", "TEXT", 0, None, 0),
        ]

        view2_info = await postgres_backend.table_info("view2")
        assert view2_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "value", "INTEGER", 0, None, 0),
        ]


async def test_postgres_migrations_apply_1(
    postgres_backend: TestingPostgresBackend,
    example_migrations_dir: str,
):
    config = postgres_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations(1)

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
        }

        await runner.validate_applied_migrations()

    async with postgres_backend.connection():
        migrations_table_rows = await postgres_backend.get_all_rows("_flux_migrations")
        assert {m[0]: m[1] for m in migrations_table_rows} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
        }

        simple_table_info = await postgres_backend.table_info("simple_table")
        assert simple_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "data", "TEXT", 0, None, 0),
            (2, "description", "TEXT", 0, None, 0),
        ]

        another_table_info = await postgres_backend.table_info("another_table")
        assert another_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "value", "INTEGER", 0, None, 0),
        ]

        view1_info = await postgres_backend.table_info("view1")
        assert view1_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "data", "TEXT", 0, None, 0),
        ]

        view2_info = await postgres_backend.table_info("view2")
        assert view2_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "value", "INTEGER", 0, None, 0),
        ]


async def test_postgres_migrations_apply_sequence(
    postgres_backend: TestingPostgresBackend,
    example_migrations_dir: str,
):
    config = postgres_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations(1)

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
        }

        await runner.validate_applied_migrations()

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert len(runner.applied_migrations) == 1

        await runner.apply_migrations(2)

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    async with postgres_backend.connection():
        migrations_table_rows = await postgres_backend.get_all_rows("_flux_migrations")
        assert len(list(migrations_table_rows)) == 3
        assert {m[0]: m[1] for m in migrations_table_rows} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        simple_table_info = await postgres_backend.table_info("simple_table")
        assert simple_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "data", "TEXT", 0, None, 0),
            (2, "description", "TEXT", 0, None, 0),
        ]

        another_table_info = await postgres_backend.table_info("another_table")
        assert another_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "value", "INTEGER", 0, None, 0),
            (2, "timestamp", "TEXT", 0, None, 0),
        ]

        new_table_info = await postgres_backend.table_info("new_table")
        assert new_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "info", "TEXT", 0, None, 0),
        ]

        view1_info = await postgres_backend.table_info("view1")
        assert view1_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "data", "TEXT", 0, None, 0),
        ]

        view2_info = await postgres_backend.table_info("view2")
        assert view2_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "value", "INTEGER", 0, None, 0),
        ]


async def test_postgres_migrations_apply_undo_all(
    postgres_backend: TestingPostgresBackend,
    example_migrations_dir: str,
):
    config = postgres_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations()

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert len(runner.applied_migrations) == 3

        await runner.rollback_migrations()

        assert len(runner.applied_migrations) == 0

        await runner.validate_applied_migrations()

    async with postgres_backend.connection():
        migrations_table_rows = await postgres_backend.get_all_rows("_flux_migrations")
        assert len(list(migrations_table_rows)) == 0

        simple_table_info = await postgres_backend.table_info("simple_table")
        assert simple_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "data", "TEXT", 0, None, 0),
        ]

        another_table_info = await postgres_backend.table_info("another_table")
        assert another_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "value", "INTEGER", 0, None, 0),
        ]

        new_table_info = await postgres_backend.table_info("new_table")
        assert new_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "info", "TEXT", 0, None, 0),
        ]

        view1_info = await postgres_backend.table_info("view1")
        assert view1_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "data", "TEXT", 0, None, 0),
        ]

        view2_info = await postgres_backend.table_info("view2")
        assert view2_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "value", "INTEGER", 0, None, 0),
        ]


async def test_postgres_migrations_apply_undo_redo(
    postgres_backend: TestingPostgresBackend,
    example_migrations_dir: str,
):
    config = postgres_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations()

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert len(runner.applied_migrations) == 3

        await runner.rollback_migrations()

        assert len(runner.applied_migrations) == 0

        await runner.validate_applied_migrations()

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations()

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    async with postgres_backend.connection():
        migrations_table_rows = await postgres_backend.get_all_rows("_flux_migrations")
        assert {m[0]: m[1] for m in migrations_table_rows} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        simple_table_info = await postgres_backend.table_info("simple_table")
        assert simple_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "data", "TEXT", 0, None, 0),
            (2, "description", "TEXT", 0, None, 0),
        ]

        another_table_info = await postgres_backend.table_info("another_table")
        assert another_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "value", "INTEGER", 0, None, 0),
            (2, "timestamp", "TEXT", 0, None, 0),
        ]

        new_table_info = await postgres_backend.table_info("new_table")
        assert new_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "info", "TEXT", 0, None, 0),
        ]

        view1_info = await postgres_backend.table_info("view1")
        assert view1_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "data", "TEXT", 0, None, 0),
        ]

        view2_info = await postgres_backend.table_info("view2")
        assert view2_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "value", "INTEGER", 0, None, 0),
        ]


async def test_postgres_migrations_apply_undo_2(
    postgres_backend: TestingPostgresBackend,
    example_migrations_dir: str,
):
    config = postgres_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations()

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert len(runner.applied_migrations) == 3

        await runner.rollback_migrations(2)

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
        }

        await runner.validate_applied_migrations()

    async with postgres_backend.connection():
        migrations_table_rows = await postgres_backend.get_all_rows("_flux_migrations")
        assert len(list(migrations_table_rows)) == 1
        assert {m[0]: m[1] for m in migrations_table_rows} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
        }

        simple_table_info = await postgres_backend.table_info("simple_table")
        assert simple_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "data", "TEXT", 0, None, 0),
            (2, "description", "TEXT", 0, None, 0),
        ]

        another_table_info = await postgres_backend.table_info("another_table")
        assert another_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "value", "INTEGER", 0, None, 0),
        ]

        new_table_info = await postgres_backend.table_info("new_table")
        assert new_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "info", "TEXT", 0, None, 0),
        ]

        view1_info = await postgres_backend.table_info("view1")
        assert view1_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "data", "TEXT", 0, None, 0),
        ]

        view2_info = await postgres_backend.table_info("view2")
        assert view2_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "value", "INTEGER", 0, None, 0),
        ]


@pytest.mark.parametrize(
    "file_to_remove",
    [
        "20200101_001_add_description_to_simple_table.sql",
        "20200102_001_add_timestamp_to_another_table.py",
        "20200102_002_create_new_table.py",
    ],
)
async def test_postgres_migrations_corrupted_applied_migration_removed(
    postgres_backend: TestingPostgresBackend,
    example_migrations_dir: str,
    file_to_remove: str,
):
    config = postgres_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations()

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    os.remove(os.path.join(example_migrations_dir, file_to_remove))

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        with pytest.raises(MigrationDirectoryCorruptedError):
            await runner.validate_applied_migrations()


async def test_postgres_migrations_corrupted_migration_inserted_between_applied(
    postgres_backend: TestingPostgresBackend,
    example_migrations_dir: str,
):
    config = postgres_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations(2)

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
        }

        await runner.validate_applied_migrations()

    with open(
        os.path.join(example_migrations_dir, "20200101_002_intermediate.sql"), "w"
    ) as f:
        f.write("create table some_new_table ( id serial primary key, name text );")

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        with pytest.raises(MigrationDirectoryCorruptedError):
            await runner.validate_applied_migrations()


@pytest.mark.parametrize(
    "file_to_modify",
    [
        "20200101_001_add_description_to_simple_table.sql",
    ],
)
async def test_postgres_migrations_corrupted_applied_migration_modified_sql(
    postgres_backend: TestingPostgresBackend,
    example_migrations_dir: str,
    file_to_modify: str,
):
    config = postgres_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations()

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    with open(os.path.join(example_migrations_dir, file_to_modify), "a") as f:
        f.write("create table some_new_table ( id serial primary key, name text );")

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        with pytest.raises(MigrationDirectoryCorruptedError):
            await runner.validate_applied_migrations()


@pytest.mark.parametrize(
    "file_to_modify",
    [
        "20200102_001_add_timestamp_to_another_table.py",
        "20200102_002_create_new_table.py",
    ],
)
async def test_postgres_migrations_corrupted_applied_migration_modified_py(
    postgres_backend: TestingPostgresBackend,
    example_migrations_dir: str,
    file_to_modify: str,
):
    config = postgres_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations()

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_002_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    with open(os.path.join(example_migrations_dir, file_to_modify), "w") as f:
        f.write("def apply():\n    return 'create table a ( id serial primary key );'")

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        with pytest.raises(MigrationDirectoryCorruptedError):
            await runner.validate_applied_migrations()


async def test_postgres_migrations_with_bad_migration(
    postgres_backend: TestingPostgresBackend,
    example_migrations_dir: str,
):
    config = postgres_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.validate_applied_migrations()
        await runner.apply_migrations(2)

        assert {m.id: m.hash for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
        }

        await runner.validate_applied_migrations()

    _write_new_bad_migration(example_migrations_dir)

    with pytest.raises(MigrationApplyError):
        async with FluxRunner(config=config, backend=postgres_backend) as runner:
            initialized = await postgres_backend.is_initialized()
            assert initialized is True

            assert {m.id: m.hash for m in runner.applied_migrations} == {
                "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
                "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            }
            await runner.apply_migrations()

    async with postgres_backend.connection():
        migrations_table_rows = await postgres_backend.get_all_rows("_flux_migrations")
        assert {m[0]: m[1] for m in migrations_table_rows} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200102_001_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
        }

        simple_table_info = await postgres_backend.table_info("simple_table")
        assert simple_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "data", "TEXT", 0, None, 0),
            (2, "description", "TEXT", 0, None, 0),
        ]

        another_table_info = await postgres_backend.table_info("another_table")
        assert another_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "value", "INTEGER", 0, None, 0),
            (2, "timestamp", "TEXT", 0, None, 0),
        ]

        new_table_info = await postgres_backend.table_info("new_table")
        assert new_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "info", "TEXT", 0, None, 0),
            (2, "new_col", "TEXT", 0, None, 0),
        ]

        view1_info = await postgres_backend.table_info("view1")
        assert view1_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "data", "TEXT", 0, None, 0),
        ]

        view2_info = await postgres_backend.table_info("view2")
        assert view2_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "value", "INTEGER", 0, None, 0),
        ]