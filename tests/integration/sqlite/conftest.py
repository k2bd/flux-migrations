import os
from tempfile import TemporaryDirectory
from typing import Generator

import pytest

from tests.integration.sqlite.backend import SQLiteBackend


@pytest.fixture
def temp_db_path() -> Generator[str, None, None]:
    with TemporaryDirectory() as temp_dir:
        db_file = os.path.join(temp_dir, "test.db")
        yield db_file


@pytest.fixture
def sqlite_backend(temp_db_path: str):
    yield SQLiteBackend(db_path=temp_db_path, migrations_table="_flux_migrations")
