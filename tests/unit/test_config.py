import os

import pytest

from flux.config import FluxConfig
from flux.exceptions import InvalidConfigurationError
from tests.unit.constants import CONFIGS_DIR

POSTGRES_CONFIG = os.path.join(CONFIGS_DIR, "postgres.toml")
MINIMAL_CONFIG = os.path.join(CONFIGS_DIR, "minimal.toml")
INVALID_MISSING_MIGRATION_DIR_CONFIG = os.path.join(
    CONFIGS_DIR, "invalid_missing_migration_dir.toml"
)
INVALID_MISSING_BACKEND_CONFIG = os.path.join(
    CONFIGS_DIR, "invalid_missing_backend.toml"
)


def test_flux_config_from_file_postgres():
    config = FluxConfig.from_file(POSTGRES_CONFIG)
    assert config.backend == "postgres"
    assert config.migration_directory == "migrations"
    assert config.log_level == "info"
    assert config.backend_config == {
        "host": "localhost",
        "port": 5432,
        "user": "your_username",
        "password": "your_password",
        "database": "your_database",
        "sslmode": "require",
    }


def test_flux_config_from_file_minimal():
    config = FluxConfig.from_file(MINIMAL_CONFIG)
    assert config.backend == "postgres"
    assert config.migration_directory == "migrations"
    assert config.log_level == "INFO"
    assert config.backend_config == {}


@pytest.mark.parametrize(
    "invalid_config",
    [
        INVALID_MISSING_BACKEND_CONFIG,
        INVALID_MISSING_MIGRATION_DIR_CONFIG,
    ],
)
def test_flux_config_invalid(invalid_config: str):
    with pytest.raises(InvalidConfigurationError):
        FluxConfig.from_file(invalid_config)
