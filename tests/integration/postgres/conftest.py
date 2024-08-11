import os
import random
import shutil
from importlib.metadata import EntryPoint
from string import ascii_lowercase
from tempfile import TemporaryDirectory
from typing import AsyncGenerator, Generator
from unittest import mock

import pytest
from databases import Database

from flux.backend.get_backends import __name__ as GET_BACKENDS_NAME
from flux.builtins.postgres import FluxPostgresBackend
from flux.constants import FLUX_BACKEND_PLUGIN_GROUP
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
    return FluxPostgresBackend(
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


@pytest.fixture
def mock_installed_postgres_backend():
    def mocked_iter_entry_points(group, name=None):
        plugins = [
            EntryPoint(
                name="postgres",
                group=FLUX_BACKEND_PLUGIN_GROUP,
                value="flux.builtins.postgres:FluxPostgresBackend",
            )
        ]
        if group != FLUX_BACKEND_PLUGIN_GROUP:
            return []

        if name is not None:
            return [plugin for plugin in plugins if plugin.name == name]

        return plugins

    with mock.patch(
        f"{GET_BACKENDS_NAME}.entry_points",
        side_effect=mocked_iter_entry_points,
    ) as m:
        yield m
