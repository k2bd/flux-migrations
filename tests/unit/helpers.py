from flux.config import FluxConfig


def in_memory_config(
    *,
    migration_directory: str,
    backend_config: dict | None = None,
):
    return FluxConfig(
        backend="in_memory",
        migration_directory=migration_directory,
        log_level="DEBUG",
        backend_config=backend_config or {},
    )
