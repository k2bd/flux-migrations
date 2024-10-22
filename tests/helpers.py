import asyncio
import datetime as dt
import os
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field

from flux.backend.applied_migration import AppliedMigration
from flux.backend.base import MigrationBackend
from flux.config import FluxConfig
from flux.migration.migration import Migration


def example_config(
    *,
    backend: str,
    migration_directory: str,
    log_level: str = "DEBUG",
    apply_repeatable_on_down: bool = True,
    backend_config: dict | None = None,
):
    return FluxConfig(
        backend=backend,
        migration_directory=migration_directory,
        log_level=log_level,
        apply_repeatable_on_down=apply_repeatable_on_down,
        backend_config=backend_config or {},
    )


@dataclass
class InMemoryMigrationBackend(MigrationBackend):
    applied_migrations: set[AppliedMigration] = field(default_factory=set)

    connection_active: bool = False
    transaction_active: bool = False
    migration_lock_active: bool = False

    staged_migrations: set[AppliedMigration] = field(default_factory=set)

    @asynccontextmanager
    async def connection(self):
        """
        Create a connection that lasts as long as the context manager is
        active.
        """
        while self.connection_active:
            await asyncio.sleep(0.1)
        self.connection_active = True
        try:
            yield
        finally:
            self.connection_active = False

    @asynccontextmanager
    async def transaction(self):
        """
        Create a transaction that lasts as long as the context manager is
        active.

        The transaction is committed when the context manager exits.

        If an exception is raised inside the context manager, the transaction
        is rolled back.
        """
        while self.transaction_active:
            await asyncio.sleep(0.1)
        self.transaction_active = True
        try:
            yield
        except Exception:
            raise
        else:
            self.applied_migrations.update(self.staged_migrations)
        finally:
            self.staged_migrations.clear()
            self.transaction_active = False

    @asynccontextmanager
    async def migration_lock(self):
        """
        Create a lock that lasts as long as the context manager is active.

        The lock is released when the context manager exits.

        This lock should prevent other migration processes from running
        concurrently.
        """
        while self.migration_lock_active:
            await asyncio.sleep(0.1)
        self.migration_lock_active = True
        try:
            yield
        finally:
            self.migration_lock_active = False

    async def initialize(self):
        """
        Initialize the backend by creating any necessary tables etc in the
        database.
        """

    async def register_migration(self, migration: Migration) -> AppliedMigration:
        """
        Register a migration as applied (when up-migrated)
        """
        applied_migration = AppliedMigration(
            id=migration.id,
            hash=migration.up_hash,
            applied_at=dt.datetime.now(),
        )
        self.staged_migrations.add(applied_migration)
        return applied_migration

    async def unregister_migration(self, migration: Migration):
        """
        Unregister a migration (when down-migrated)
        """

    async def apply_migration(self, content: str):
        """
        Apply a migration to the database. This is used for both up and down
        migrations so should not register or unregister the migration hash.
        """

    async def get_applied_migrations(self) -> set[AppliedMigration]:
        """
        Get the set of applied migrations.
        """
        return self.applied_migrations


class InvalidBackend:
    """
    An empty class. Not a valid backend as it does not subclass
    ``MigrationBackend``.
    """


@contextmanager
def change_cwd(to_dir: str):
    """
    Change the current working directory temporarily.

    N.B. changes back to the current value on exit.
    """
    original_cwd = os.getcwd()
    try:
        yield os.chdir(to_dir)
    finally:
        os.chdir(original_cwd)
