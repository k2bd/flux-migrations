import os
import sys
from unittest import mock

import pytest

from flux.exceptions import InvalidMigrationModule
from flux.migration.temporary_module import temporary_module
from tests.unit.constants import DATA_DIR

EXAMPLE_MODULE_FILENAME = os.path.join(DATA_DIR, "example_module.py")
INVALID_MODULE_FILENAME = os.path.join(DATA_DIR, "invalid_module.txt")


def test_temporary_module():
    with temporary_module(EXAMPLE_MODULE_FILENAME) as module:
        assert module.ping() == "pong"


def test_temporary_module_module_name():
    def mocked_random_choices(*args, **kwargs):
        return "mymodule"

    assert "mymodule" not in sys.modules

    with mock.patch("random.choices", side_effect=mocked_random_choices):
        with temporary_module(EXAMPLE_MODULE_FILENAME) as module:
            assert module.__name__ == "mymodule"
            assert "mymodule" in sys.modules
            assert module.ping() == "pong"

    assert "mymodule" not in sys.modules


def test_temporary_module_invalid_module():
    with pytest.raises(InvalidMigrationModule) as e:
        with temporary_module(INVALID_MODULE_FILENAME):
            pass

    assert (
        str(e.value)
        == f"Could not load migration module from {INVALID_MODULE_FILENAME!r}"
    )
