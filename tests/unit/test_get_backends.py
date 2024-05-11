import pytest

from flux.backend.get_backends import get_backend, get_backends
from flux.exceptions import BackendNotInstalledError, InvalidBackendError
from tests.helpers import InMemoryMigrationBackend


def test_get_backends_mocked(mock_installed_in_memory_backend):
    backends = get_backends()

    assert backends == {"in_memory": InMemoryMigrationBackend}


def test_get_backends_mocked_invalid(mock_installed_invalid_backend):
    with pytest.raises(InvalidBackendError):
        get_backends()


def test_get_backend_mocked(mock_installed_in_memory_backend):
    backend = get_backend("in_memory")

    assert backend == InMemoryMigrationBackend


def test_get_backend_not_installed(mock_installed_in_memory_backend):
    with pytest.raises(BackendNotInstalledError):
        get_backend("not_installed")
