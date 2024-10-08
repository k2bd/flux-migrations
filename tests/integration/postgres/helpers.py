from flux.config import FluxConfig
from tests.helpers import example_config


def postgres_config(
    *,
    migration_directory: str,
    log_level: str = "DEBUG",
    backend_config: dict | None = None,
) -> FluxConfig:
    return example_config(
        backend="testing-postgres",
        migration_directory=migration_directory,
        log_level=log_level,
        backend_config=backend_config or {},
    )
