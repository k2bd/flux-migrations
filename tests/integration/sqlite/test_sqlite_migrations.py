from flux.runner import FluxRunner
from tests.integration.sqlite.backend import SQLiteBackend
from tests.integration.sqlite.constants import MIGRATIONS_1_DIR
from tests.integration.sqlite.helpers import sqlite_config


async def test_sqlite_migrations_apply(sqlite_backend: SQLiteBackend):
    config = sqlite_config(migration_directory=MIGRATIONS_1_DIR)

    async with FluxRunner(config=config, backend=sqlite_backend) as runner:
        initialized = await sqlite_backend.is_initialized()
        assert initialized is True

        assert runner.applied_migrations == set()

        await runner.apply_migrations()

        applied = runner.applied_migrations

        assert len(applied) == 3

        assert {m.id: m.hash for m in applied} == {
            "20200101_001_add_description_to_simple_table": "1ddd0147bd77f7dd8d9c064584a2559d",  # noqa: E501
            "20200101_002_add_timestamp_to_another_table": "a78bba561022b845e60ac752288fdee4",  # noqa: E501
            "20200102_001_create_new_table": "3aa084560ac052fa463abc88f20158d8",
        }

        await runner.validate_applied_migrations()
