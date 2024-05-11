from abc import ABC, abstractmethod
from contextlib import asynccontextmanager

from flux.backend.applied_migration import AppliedMigration
from flux.config import FluxConfig
from flux.migration.migration import Migration


class MigrationBackend(ABC):

    @classmethod
    def from_config(cls, config: FluxConfig) -> "MigrationBackend":
        """
        Create a MigrationBackend from a configuration

        This config appears in the config.toml file in the "backend" section.
        """
        return cls()

    @asynccontextmanager
    @abstractmethod
    async def connection(self):
        """
        Create a connection that lasts as long as the context manager is
        active.
        """
        yield

    @asynccontextmanager
    @abstractmethod
    async def transaction(self):
        """
        Create a transaction that lasts as long as the context manager is
        active.

        The transaction is committed when the context manager exits.

        If an exception is raised inside the context manager, the transaction
        is rolled back.
        """
        yield

    @asynccontextmanager
    @abstractmethod
    async def migration_lock(self):
        """
        Create a lock that lasts as long as the context manager is active.

        The lock is released when the context manager exits.

        This lock should prevent other migration processes from running
        concurrently.
        """
        yield

    @abstractmethod
    async def register_migration(self, migration: Migration) -> AppliedMigration:
        """
        Register a migration as applied (when up-migrated)
        """

    @abstractmethod
    async def unregister_migration(self, migration: Migration):
        """
        Unregister a migration (when down-migrated)
        """

    @abstractmethod
    async def apply_migration(self, content: str):
        """
        Apply the content of a migration to the database. This is used for both
        up and down migrations so should not register or unregister the
        migration hash.
        """

    @abstractmethod
    async def get_applied_migrations(self) -> set[AppliedMigration]:
        """
        Get the set of applied migrations.
        """
