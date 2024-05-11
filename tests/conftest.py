from importlib.metadata import EntryPoint
from unittest import mock

import pytest

from flux.backend.get_backends import __name__ as GET_BACKENDS_NAME
from flux.constants import FLUX_BACKEND_PLUGIN_GROUP


@pytest.fixture
def mock_installed_in_memory_backend():
    def mocked_iter_entry_points(group, name=None):
        plugins = [
            EntryPoint(
                name="in_memory",
                group=FLUX_BACKEND_PLUGIN_GROUP,
                value="tests.helpers:InMemoryMigrationBackend",
            )
        ]
        if group != FLUX_BACKEND_PLUGIN_GROUP:
            return []

        if name is not None:
            return [plugin for plugin in plugins if plugin.name == name]

        return plugins

    with mock.patch(
        f"{GET_BACKENDS_NAME}.entry_points",
        side_effect=mocked_iter_entry_points,
    ) as m:
        yield m


@pytest.fixture
def mock_installed_invalid_backend():
    def mocked_iter_entry_points(group, name=None):
        plugins = [
            EntryPoint(
                name="fake_backend",
                group=FLUX_BACKEND_PLUGIN_GROUP,
                value="tests.helpers:InvalidBackend",
            )
        ]
        if group != FLUX_BACKEND_PLUGIN_GROUP:
            return []

        if name is not None:
            return [plugin for plugin in plugins if plugin.name == name]

        return plugins

    with mock.patch(
        f"{GET_BACKENDS_NAME}.entry_points",
        side_effect=mocked_iter_entry_points,
    ) as m:
        yield m
