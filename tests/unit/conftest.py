import pytest

from flux.config import FluxConfig
from tests.unit.constants import MIGRATIONS_DIR


@pytest.fixture
def in_memory_config():
    return FluxConfig(
        backend="in_memory",
        migration_directory=MIGRATIONS_DIR,
        log_level="DEBUG",
        backend_config={},
    )
