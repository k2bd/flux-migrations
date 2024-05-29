from tests.helpers import example_config


def in_memory_config(
    *,
    migration_directory: str,
    log_level: str = "DEBUG",
    backend_config: dict | None = None,
):
    return example_config(
        backend="in_memory",
        migration_directory=migration_directory,
        log_level=log_level,
        backend_config=backend_config or {},
    )
