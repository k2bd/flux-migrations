import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from flux.backend.applied_migration import AppliedMigration
from flux.backend.base import MigrationBackend


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

    async def register_migration(self, migration: str):
        """
        Register a migration as applied (when up-migrated)
        """

    async def unregister_migration(self, migration: str):
        """
        Unregister a migration (when down-migrated)
        """

    async def apply_migration(self, migration: str):
        """
        Apply a migration to the database.
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
