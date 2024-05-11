import random
import sys
from contextlib import contextmanager
from importlib.util import module_from_spec, spec_from_file_location
from string import ascii_lowercase
from types import ModuleType
from typing import Generator

from flux.exceptions import InvalidMigrationModuleError


@contextmanager
def temporary_module(source_filename: str) -> Generator[ModuleType, None, None]:
    """
    Create a temporary module from a Python source file
    """
    module_name = "".join(random.choices(ascii_lowercase, k=20))
    module_spec = spec_from_file_location(module_name, source_filename)
    if module_spec is None or module_spec.loader is None:
        raise InvalidMigrationModuleError(
            f"Could not load migration module from {source_filename!r}"
        )
    module = module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    sys.modules[module_name] = module

    try:
        yield module
    finally:
        del sys.modules[module_name]
