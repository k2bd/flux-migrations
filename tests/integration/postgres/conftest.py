import os
import random
import shutil
from string import ascii_lowercase
from tempfile import TemporaryDirectory
from typing import AsyncGenerator, Generator

import pytest
from databases import Database

from flux.builtins.postgres import FluxPostgresBackend
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
def database_uri(test_database) -> str:
    return f"{TEST_PG_CONNECTION_STRING}/{test_database}"


@pytest.fixture
async def postgres_backend(database_uri: str):
    return FluxPostgresBackend(
        database_url=database_uri,
        migrations_table="_flux_migrations",
    )


@pytest.fixture
def example_project_dir() -> Generator[str, None, None]:
    with TemporaryDirectory() as tempdir:
        migrations_dir = os.path.join(tempdir, "migrations")
        shutil.copytree(MIGRATIONS_1_DIR, migrations_dir)
        yield tempdir


@pytest.fixture
def example_migrations_dir(example_project_dir: str) -> str:
    """
    Create a temporary copy of the migrations directory that can be freely
    modified by tests
    """
    migrations_dir = os.path.join(example_project_dir, "migrations")
    return migrations_dir
