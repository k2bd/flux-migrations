from dataclasses import dataclass
from typing import Any

import toml

from flux.constants import (
    FLUX_APPLY_REPEATABLE_ON_DOWN_KEY,
    FLUX_BACKEND_CONFIG_SECTION_NAME,
    FLUX_BACKEND_KEY,
    FLUX_DEFAULT_APPLY_REPEATABLE_ON_DOWN,
    FLUX_DEFAULT_LOG_LEVEL,
    FLUX_GENERAL_CONFIG_SECTION_NAME,
    FLUX_LOG_LEVEL_KEY,
    FLUX_MIGRATION_DIRECTORY_KEY,
)
from flux.exceptions import InvalidConfigurationError


@dataclass
class FluxConfig:
    backend: str

    migration_directory: str

    log_level: str

    apply_repeatable_on_down: bool

    backend_config: dict[str, Any]

    @classmethod
    def from_file(cls, path: str):
        with open(path) as f:
            config = toml.load(f)

        general_config = config.get(FLUX_GENERAL_CONFIG_SECTION_NAME, {})

        backend = general_config.get(FLUX_BACKEND_KEY)
        if backend is None:
            raise InvalidConfigurationError(
                "No backend configuration found in config file"
            )

        migration_directory = general_config.get(FLUX_MIGRATION_DIRECTORY_KEY)
        if migration_directory is None:
            raise InvalidConfigurationError(
                "No migration directory found in backend configuration"
            )

        apply_repeatable_on_down = general_config.get(
            FLUX_APPLY_REPEATABLE_ON_DOWN_KEY,
            FLUX_DEFAULT_APPLY_REPEATABLE_ON_DOWN,
        )

        log_level = general_config.get(FLUX_LOG_LEVEL_KEY, FLUX_DEFAULT_LOG_LEVEL)

        backend_config = config.get(FLUX_BACKEND_CONFIG_SECTION_NAME, {})

        return cls(
            backend=backend,
            migration_directory=migration_directory,
            log_level=log_level,
            apply_repeatable_on_down=apply_repeatable_on_down,
            backend_config=backend_config,
        )
