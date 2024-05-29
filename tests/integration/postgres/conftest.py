import os
import random
import shutil
from string import ascii_lowercase
from tempfile import TemporaryDirectory
from typing import AsyncGenerator, Generator

import pytest
from databases import Database

from tests.integration.postgres.backend import ExamplePostgresBackend
from tests.integration.postgres.constants import (
    MIGRATIONS_1_DIR,
    TEST_PG_CONNECTION_STRING,
    TEST_PG_MANAGEMENT_DB,
)


@pytest.fixture
async def test_database() -> AsyncGenerator[str, None]:
    test_db_name = "test_" + "".join(random.choices(ascii_lowercase, k=10))
    async with Database(f"{TEST_PG_CONNECTION_STRING}/{TEST_PG_MANAGEMENT_DB}") as db:
        await db.execute(f"create database {test_db_name}")
        try:
            yield test_db_name
        finally:
            await db.execute(f"drop database {test_db_name}")


@pytest.fixture
async def postgres_backend(test_database):
    return ExamplePostgresBackend(
        database_url=f"{TEST_PG_CONNECTION_STRING}/{test_database}",
        migrations_schema="_migrations",
        migrations_table="_flux_migrations",
    )


@pytest.fixture
def example_migrations_dir() -> Generator[str, None, None]:
    """
    Create a temporary copy of the migrations directory that can be freely
    modified by tests
    """
    with TemporaryDirectory() as tempdir:
        migrations_dir = os.path.join(tempdir, "migrations")
        shutil.copytree(MIGRATIONS_1_DIR, migrations_dir)
        yield migrations_dir
