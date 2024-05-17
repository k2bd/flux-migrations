import os
from tempfile import TemporaryDirectory
from typing import Generator

import pytest

from tests.integration.sqlite.backend import SQLiteBackend
from tests.integration.sqlite.constants import MIGRATIONS_1_DIR
import shutil


@pytest.fixture
def temp_db_path() -> Generator[str, None, None]:
    with TemporaryDirectory() as temp_dir:
        db_file = os.path.join(temp_dir, "test.db")
        yield db_file


@pytest.fixture
def sqlite_backend(temp_db_path: str):
    yield SQLiteBackend(db_path=temp_db_path, migrations_table="_flux_migrations")


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
