from dataclasses import dataclass

from flux.migration.temporary_module import temporary_module


@dataclass
class Migration:
    up: str
    down: str | None


def read_sql_migration(up_file: str, down_file: str | None) -> Migration:
    """
    Read a pair of SQL migration files and return a Migration object
    """
    with open(up_file) as f:
        up = f.read()

    if down_file is None:
        down = None
    else:
        with open(down_file) as f:
            down = f.read()

    return Migration(up=up, down=down)


def read_python_migration(migration_file: str) -> Migration:
    """
    Read a Python migration file and return a Migration object
    """
    with temporary_module(migration_file) as module:
        return Migration(up=module.up(), down=module.down())
