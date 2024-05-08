import pkg_resources

from flux.backend.base import MigrationBackend
from flux.constants import FLUX_BACKEND_PLUGIN_GROUP
from flux.exceptions import InvalidBackendError


def get_backends() -> dict[str, MigrationBackend]:
    backends = {
        entry_point.name: entry_point.load()
        for entry_point in pkg_resources.iter_entry_points(
            group=FLUX_BACKEND_PLUGIN_GROUP
        )
    }

    for name, backend in backends.items():
        if not issubclass(backend, MigrationBackend):
            raise InvalidBackendError(
                f"Backend {name} does not subclass MigrationBackend"
            )

    return backends
