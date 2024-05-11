from dataclasses import dataclass
from typing import Any

import toml

from flux.exceptions import InvalidConfigurationError


@dataclass
class FluxConfig:
    backend: str

    migration_directory: str

    log_level: str

    backend_config: dict[str, Any]

    @classmethod
    def from_file(cls, path: str):
        with open(path) as f:
            config = toml.load(f)

        general_config = config.get("flux", {})

        try:
            backend = general_config["backend"]
        except KeyError:
            raise InvalidConfigurationError(
                "No backend configuration found in config file"
            )

        try:
            migration_directory = general_config["migration_directory"]
        except KeyError:
            raise InvalidConfigurationError(
                "No migration directory found in backend configuration"
            )

        log_level = general_config.get("log_level", "INFO")

        backend_config = config.get("backend", {})

        return cls(
            backend=backend,
            migration_directory=migration_directory,
            log_level=log_level,
            backend_config=backend_config,
        )
