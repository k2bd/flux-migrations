from flux.runner import FluxRunner
from tests.integration.sqlite.backend import SQLiteBackend
from tests.integration.sqlite.helpers import sqlite_config


async def test_sqlite_migrations_apply(
    sqlite_backend: SQLiteBackend,
    example_migrations_dir: str,
):
    config = sqlite_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=sqlite_backend) as runner:
        initialized = await sqlite_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations()

        applied = runner.applied_migrations

        assert {m.id: m.hash for m in applied} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200101_002_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_001_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    async with sqlite_backend.connection():
        migrations_table_rows = await sqlite_backend.get_all_rows("_flux_migrations")
        assert {m[0]: m[1] for m in migrations_table_rows} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200101_002_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_001_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        simple_table_info = await sqlite_backend.table_info("simple_table")
        assert simple_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "data", "TEXT", 0, None, 0),
            (2, "description", "TEXT", 0, None, 0),
        ]

        another_table_info = await sqlite_backend.table_info("another_table")
        assert another_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "value", "INTEGER", 0, None, 0),
            (2, "timestamp", "TEXT", 0, None, 0),
        ]

        new_table_info = await sqlite_backend.table_info("new_table")
        assert new_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "info", "TEXT", 0, None, 0),
        ]

        view1_info = await sqlite_backend.table_info("view1")
        assert view1_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "data", "TEXT", 0, None, 0),
        ]

        view2_info = await sqlite_backend.table_info("view2")
        assert view2_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "value", "INTEGER", 0, None, 0),
        ]


async def test_sqlite_migrations_apply_1(
    sqlite_backend: SQLiteBackend,
    example_migrations_dir: str,
):
    config = sqlite_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=sqlite_backend) as runner:
        initialized = await sqlite_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations(1)

        applied = runner.applied_migrations

        assert {m.id: m.hash for m in applied} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
        }

        await runner.validate_applied_migrations()

    async with sqlite_backend.connection():
        migrations_table_rows = await sqlite_backend.get_all_rows("_flux_migrations")
        assert {m[0]: m[1] for m in migrations_table_rows} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
        }

        simple_table_info = await sqlite_backend.table_info("simple_table")
        assert simple_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "data", "TEXT", 0, None, 0),
            (2, "description", "TEXT", 0, None, 0),
        ]

        another_table_info = await sqlite_backend.table_info("another_table")
        assert another_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "value", "INTEGER", 0, None, 0),
        ]

        view1_info = await sqlite_backend.table_info("view1")
        assert view1_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "data", "TEXT", 0, None, 0),
        ]

        view2_info = await sqlite_backend.table_info("view2")
        assert view2_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "value", "INTEGER", 0, None, 0),
        ]


async def test_sqlite_migrations_apply_sequence(
    sqlite_backend: SQLiteBackend,
    example_migrations_dir: str,
):
    config = sqlite_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=sqlite_backend) as runner:
        initialized = await sqlite_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations(1)

        applied = runner.applied_migrations

        assert {m.id: m.hash for m in applied} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
        }

        await runner.validate_applied_migrations()

    async with FluxRunner(config=config, backend=sqlite_backend) as runner:
        initialized = await sqlite_backend.is_initialized()
        assert initialized is True

        assert len(runner.applied_migrations) == 1

        await runner.apply_migrations(2)

        applied = runner.applied_migrations

        assert {m.id: m.hash for m in applied} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200101_002_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_001_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    async with sqlite_backend.connection():
        migrations_table_rows = await sqlite_backend.get_all_rows("_flux_migrations")
        assert len(list(migrations_table_rows)) == 3
        assert {m[0]: m[1] for m in migrations_table_rows} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200101_002_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_001_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        simple_table_info = await sqlite_backend.table_info("simple_table")
        assert simple_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "data", "TEXT", 0, None, 0),
            (2, "description", "TEXT", 0, None, 0),
        ]

        another_table_info = await sqlite_backend.table_info("another_table")
        assert another_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "value", "INTEGER", 0, None, 0),
            (2, "timestamp", "TEXT", 0, None, 0),
        ]

        new_table_info = await sqlite_backend.table_info("new_table")
        assert new_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "info", "TEXT", 0, None, 0),
        ]

        view1_info = await sqlite_backend.table_info("view1")
        assert view1_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "data", "TEXT", 0, None, 0),
        ]

        view2_info = await sqlite_backend.table_info("view2")
        assert view2_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "value", "INTEGER", 0, None, 0),
        ]


async def test_sqlite_migrations_apply_undo_all(
    sqlite_backend: SQLiteBackend,
    example_migrations_dir: str,
):
    config = sqlite_config(migration_directory=example_migrations_dir)

    async with FluxRunner(config=config, backend=sqlite_backend) as runner:
        initialized = await sqlite_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations()

        applied = runner.applied_migrations

        assert {m.id: m.hash for m in applied} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200101_002_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_001_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()

    async with FluxRunner(config=config, backend=sqlite_backend) as runner:
        initialized = await sqlite_backend.is_initialized()
        assert initialized is True

        assert len(runner.applied_migrations) == 3

        await runner.rollback_migrations()

        assert len(runner.applied_migrations) == 0

        await runner.validate_applied_migrations()

    async with sqlite_backend.connection():
        migrations_table_rows = await sqlite_backend.get_all_rows("_flux_migrations")
        assert len(list(migrations_table_rows)) == 0

        simple_table_info = await sqlite_backend.table_info("simple_table")
        assert simple_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "data", "TEXT", 0, None, 0),
        ]

        another_table_info = await sqlite_backend.table_info("another_table")
        assert another_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "value", "INTEGER", 0, None, 0),
        ]

        new_table_info = await sqlite_backend.table_info("new_table")
        assert new_table_info == [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "info", "TEXT", 0, None, 0),
        ]

        view1_info = await sqlite_backend.table_info("view1")
        assert view1_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "data", "TEXT", 0, None, 0),
        ]

        view2_info = await sqlite_backend.table_info("view2")
        assert view2_info == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "value", "INTEGER", 0, None, 0),
        ]
