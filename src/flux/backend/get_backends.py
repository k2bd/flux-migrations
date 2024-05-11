from importlib.metadata import entry_points

from flux.backend.base import MigrationBackend
from flux.constants import FLUX_BACKEND_PLUGIN_GROUP
from flux.exceptions import BackendNotInstalledError, InvalidBackendError


def get_backends(name: str | None = None) -> dict[str, MigrationBackend]:
    kwargs = dict(group=FLUX_BACKEND_PLUGIN_GROUP)
    if name is not None:
        kwargs["name"] = name

    backends = {
        entry_point.name: entry_point.load()
        for entry_point in entry_points(**kwargs)  # type: ignore
    }

    for name, backend in backends.items():
        if not issubclass(backend, MigrationBackend):
            raise InvalidBackendError(
                f"Backend {name} does not subclass MigrationBackend"
            )

    return backends


def get_backend(name: str) -> MigrationBackend:
    if name not in (backends := get_backends(name=name)):
        raise BackendNotInstalledError(f"Backend {name} is not installed")
    return backends[name]
