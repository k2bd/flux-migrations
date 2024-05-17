import os

from flux.config import FluxConfig
from flux.constants import POST_APPLY_DIRECTORY, PRE_APPLY_DIRECTORY
from flux.exceptions import MigrationLoadingError
from flux.migration.migration import Migration
from flux.migration.temporary_module import temporary_module


def read_migrations(*, config: FluxConfig) -> list[Migration]:
    """
    Read all normal migrations in the migration directory and return a list of
    Migration objects in apply order
    """
    migrations = []
    for migration_file in os.listdir(config.migration_directory):
        if migration_file.endswith(".sql") and not migration_file.endswith(".undo.sql"):
            migration_id = migration_file[:-4]
            migrations.append(
                read_sql_migration(config=config, migration_id=migration_id)
            )
        elif migration_file.endswith(".py"):
            migration_id = migration_file[:-3]
            migrations.append(
                read_python_migration(config=config, migration_id=migration_id)
            )

    return sorted(migrations, key=lambda m: m.id)


def _read_repeatable_migrations(
    *,
    config: FluxConfig,
    migration_subdir: str,
) -> list[Migration]:
    """
    Read all repeatable migrations in the given subdir of the migration
    directory and return a list of Migration objects in apply order
    """
    migrations = []
    migrations_dir = os.path.join(config.migration_directory, migration_subdir)
    for migration_file in os.listdir(migrations_dir):
        if migration_file.endswith(".sql"):
            migration_id = migration_file[:-4]
            migrations.append(
                read_repeatable_sql_migration(
                    config=config,
                    migration_id=migration_id,
                    migration_subdir=migration_subdir,
                )
            )
        elif migration_file.endswith(".py"):
            migration_id = migration_file[:-3]
            migrations.append(
                read_repeatable_python_migration(
                    config=config,
                    migration_id=migration_id,
                    migration_subdir=migration_subdir,
                )
            )

    return sorted(migrations, key=lambda m: m.id)


def read_pre_apply_migrations(*, config: FluxConfig) -> list[Migration]:
    """
    Read all migrations in the pre-apply directory and return a list of
    Migration objects in apply order
    """
    return _read_repeatable_migrations(
        config=config,
        migration_subdir=PRE_APPLY_DIRECTORY,
    )


def read_post_apply_migrations(*, config: FluxConfig) -> list[Migration]:
    """
    Read all migrations in the post-apply directory and return a list of
    Migration objects in apply order
    """
    return _read_repeatable_migrations(
        config=config,
        migration_subdir=POST_APPLY_DIRECTORY,
    )


def read_sql_migration(*, config: FluxConfig, migration_id: str) -> Migration:
    """
    Read a pair of SQL migration files and return a Migration object
    """
    up_file = os.path.join(config.migration_directory, f"{migration_id}.sql")
    down_file = os.path.join(config.migration_directory, f"{migration_id}.undo.sql")

    try:
        with open(up_file) as f:
            up = f.read()
    except Exception as e:
        raise MigrationLoadingError("Error reading up migration") from e

    if not os.path.exists(down_file):
        down = None
    else:
        try:
            with open(down_file) as f:
                down = f.read()
        except Exception as e:
            raise MigrationLoadingError("Error reading down migration") from e

    return Migration(id=migration_id, up=up, down=down)


def read_repeatable_sql_migration(
    *,
    config: FluxConfig,
    migration_id: str,
    migration_subdir: str,
) -> Migration:
    """
    Read a repeatable SQL migration file and return a Migration object
    """
    up_file = os.path.join(
        config.migration_directory, migration_subdir, f"{migration_id}.sql"
    )

    try:
        with open(up_file) as f:
            up = f.read()
    except Exception as e:
        raise MigrationLoadingError("Error reading up migration") from e

    if os.path.exists(
        os.path.join(
            config.migration_directory, migration_subdir, f"{migration_id}.undo.sql"
        )
    ):
        raise MigrationLoadingError("Repeatable migrations cannot have a down")

    return Migration(id=migration_id, up=up, down=None)


def read_python_migration(*, config: FluxConfig, migration_id: str) -> Migration:
    """
    Read a Python migration file and return a Migration object
    """
    migration_file = os.path.join(config.migration_directory, f"{migration_id}.py")

    with temporary_module(migration_file) as module:
        try:
            up_migration = module.apply()
        except Exception as e:
            raise MigrationLoadingError("Error reading up migration") from e
        if not isinstance(up_migration, str):
            raise MigrationLoadingError("Up migration must return a string")
        if hasattr(module, "undo"):
            try:
                down_migration = module.undo()
            except Exception as e:
                raise MigrationLoadingError("Error reading down migration") from e
            if not isinstance(down_migration, str):
                raise MigrationLoadingError(
                    "Down migration must return a string"
                )
        else:
            down_migration = None

    return Migration(id=migration_id, up=up_migration, down=down_migration)


def read_repeatable_python_migration(
    *,
    config: FluxConfig,
    migration_id: str,
    migration_subdir: str,
) -> Migration:
    """
    Read a Python migration file and return a Migration object
    """
    migration_file = os.path.join(
        config.migration_directory, migration_subdir, f"{migration_id}.py"
    )

    with temporary_module(migration_file) as module:
        try:
            up_migration = module.apply()
        except Exception as e:
            raise MigrationLoadingError("Error reading up migration") from e
        if not isinstance(up_migration, str):
            raise MigrationLoadingError("Up migration must return a string")
        if hasattr(module, "undo"):
            raise MigrationLoadingError("Repeatable migrations cannot have a down")

    return Migration(id=migration_id, up=up_migration, down=None)
