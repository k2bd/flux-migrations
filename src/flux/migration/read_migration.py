import os

from flux.config import FluxConfig
from flux.exceptions import InvalidMigrationModuleError
from flux.migration.migration import Migration
from flux.migration.temporary_module import temporary_module


def read_sql_migration(*, config: FluxConfig, migration_id: str) -> Migration:
    """
    Read a pair of SQL migration files and return a Migration object
    """
    up_file = os.path.join(config.migration_directory, f"{migration_id}_up.sql")
    down_file = os.path.join(config.migration_directory, f"{migration_id}_down.sql")

    try:
        with open(up_file) as f:
            up = f.read()
    except Exception as e:
        raise InvalidMigrationModuleError("Error reading up migration") from e

    if not os.path.exists(down_file):
        down = None
    else:
        try:
            with open(down_file) as f:
                down = f.read()
        except Exception as e:
            raise InvalidMigrationModuleError("Error reading down migration") from e

    return Migration(id=migration_id, up=up, down=down)


def read_python_migration(*, config: FluxConfig, migration_id: str) -> Migration:
    """
    Read a Python migration file and return a Migration object
    """
    migration_file = os.path.join(config.migration_directory, f"{migration_id}.py")

    with temporary_module(migration_file) as module:
        try:
            up_migration = module.up()
        except Exception as e:
            raise InvalidMigrationModuleError("Error reading up migration") from e
        if not isinstance(up_migration, str):
            raise InvalidMigrationModuleError("Up migration must return a string")
        if hasattr(module, "down"):
            try:
                down_migration = module.down()
            except Exception as e:
                raise InvalidMigrationModuleError("Error reading down migration") from e
            if not isinstance(down_migration, str) and down_migration is not None:
                raise InvalidMigrationModuleError(
                    "Down migration must return a string or None"
                )
        else:
            down_migration = None

    return Migration(id=migration_id, up=up_migration, down=down_migration)
