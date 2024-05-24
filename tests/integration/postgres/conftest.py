import os
import shutil
from tempfile import TemporaryDirectory
from typing import Generator

import pytest

from tests.integration.postgres.backend import TestingPostgresBackend
from tests.integration.postgres.constants import MIGRATIONS_1_DIR
from tests.integration.postgres.constants import TEST_PG_CONNECTION_STRING
from databases import Database
from databases.core import Connection, Transaction


async def delete_database():
    db = Database(TEST_PG_CONNECTION_STRING)
    conn = await db.connect()
    await conn.execute("drop database postgres")
    await conn.execute("create database postgres")
    await conn.close()


@pytest.fixture
async def postgres_backend():
    try:
        yield TestingPostgresBackend(
            database_url=TEST_PG_CONNECTION_STRING,
            migrations_table="_flux_migrations",
        )
    finally:
        await delete_database()


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
