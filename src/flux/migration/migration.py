import hashlib
from dataclasses import dataclass

from flux.exceptions import InvalidMigrationModule
from flux.migration.temporary_module import temporary_module


@dataclass
class Migration:
    up: str
    down: str | None

    @property
    def up_hash(self) -> str:
        """
        Return the hash of the up-migration content
        """
        return hashlib.md5(self.up.encode()).hexdigest()


def read_sql_migration(up_file: str, down_file: str | None) -> Migration:
    """
    Read a pair of SQL migration files and return a Migration object
    """
    try:
        with open(up_file) as f:
            up = f.read()
    except Exception as e:
        raise InvalidMigrationModule("Error reading up migration") from e

    if down_file is None:
        down = None
    else:
        try:
            with open(down_file) as f:
                down = f.read()
        except Exception as e:
            raise InvalidMigrationModule("Error reading down migration") from e

    return Migration(up=up, down=down)


def read_python_migration(migration_file: str) -> Migration:
    """
    Read a Python migration file and return a Migration object
    """
    with temporary_module(migration_file) as module:
        try:
            up_migration = module.up()
        except Exception as e:
            raise InvalidMigrationModule("Error reading up migration") from e
        if not isinstance(up_migration, str):
            raise InvalidMigrationModule("Up migration must return a string")
        if hasattr(module, "down"):
            try:
                down_migration = module.down()
            except Exception as e:
                raise InvalidMigrationModule("Error reading down migration") from e
            if not isinstance(down_migration, str) and down_migration is not None:
                raise InvalidMigrationModule(
                    "Down migration must return a string or None"
                )
        else:
            down_migration = None

    return Migration(up=up_migration, down=down_migration)
