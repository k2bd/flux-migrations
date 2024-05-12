from dataclasses import dataclass, field
from functools import wraps

from flux.backend.applied_migration import AppliedMigration
from flux.backend.base import MigrationBackend
from flux.config import FluxConfig
from flux.exceptions import MigrationDirectoryCorruptedError
from flux.migration.migration import Migration
from flux.migration.read_migration import (
    read_migrations,
    read_post_apply_migrations,
    read_pre_apply_migrations,
)


def _initialize_if_needed(func):
    @wraps(func)
    async def wrapper(self: "FluxRunner", *args, **kwargs):
        if not await self.backend.is_initialized():
            await self.backend.initialize()

        return await func(self, *args, **kwargs)

    return wrapper


@dataclass
class FluxRunner:
    """
    Migration runner, given a config and a backend.

    Must be used within an async context manager.
    """

    config: FluxConfig

    backend: MigrationBackend

    _conn_ctx = field(init=False)
    _tx_ctx = field(init=False)
    _lock_ctx = field(init=False)

    pre_apply_migrations: list[Migration] = field(init=False)
    migrations: list[Migration] = field(init=False)
    post_apply_migrations: list[Migration] = field(init=False)

    applied_migrations: set[AppliedMigration] = field(init=False)

    async def __aenter__(self):
        self._conn_ctx = self.backend.connection()
        self._tx_ctx = self.backend.transaction()
        self._lock_ctx = self.backend.migration_lock()

        await self._conn_ctx.__aenter__()
        await self._tx_ctx.__aenter__()
        await self._lock_ctx.__aenter__()

        self.pre_apply_migrations = read_pre_apply_migrations(config=self.config)
        self.migrations = read_migrations(config=self.config)
        self.post_apply_migrations = read_post_apply_migrations(config=self.config)

        self.applied_migrations = await self.backend.get_applied_migrations()

    async def __aexit__(self, exc_type, exc, tb):
        await self._tx_ctx.__aexit__(exc_type, exc, tb)
        await self._lock_ctx.__aexit__(exc_type, exc, tb)
        await self._conn_ctx.__aexit__(exc_type, exc, tb)

    @_initialize_if_needed
    async def validate_applied_migrations(self):
        """
        Confirms the following for applied migrations:
        - There is no discontinuity in the applied migrations
        - The migration hashes of all applied migrations haven't changed
        """
        applied_migration_files = [
            m
            for m in self.migrations
            if m.id in {_m.id for _m in self.applied_migrations}
        ]

        if not [m.id for m in applied_migration_files] == [
            m.id for m in self.migrations[: len(applied_migration_files)]
        ]:
            raise MigrationDirectoryCorruptedError(
                "There is a discontinuity in the applied migrations"
            )

        for migration in applied_migration_files:
            applied_migration = next(
                m for m in self.applied_migrations if m.id == migration.id
            )
            if applied_migration.hash != migration.up_hash:
                raise MigrationDirectoryCorruptedError(
                    f"Migration {migration.id} has changed since it was applied"
                )

    @_initialize_if_needed
    async def apply_migrations(self, n: int | None = None):
        """
        Apply unapplied migrations to the database
        """
        unapplied_migrations = [
            m
            for m in self.migrations
            if m.id not in {m.id for m in self.applied_migrations}
        ]
        migrations_to_apply = unapplied_migrations[:n]

        for migration in migrations_to_apply:
            if migration.id in {m.id for m in self.applied_migrations}:
                continue

            await self.backend.apply_migration(migration.up)
            await self.backend.register_migration(migration)

        self.applied_migrations = await self.backend.get_applied_migrations()

    @_initialize_if_needed
    async def rollback_migrations(self, n: int | None = None):
        """
        Rollback the last n applied migrations
        """
        applied_migrations = [
            m
            for m in self.migrations
            if m.id in {m.id for m in self.applied_migrations}
        ]
        migrations_to_rollback = (
            applied_migrations[-n:] if n is not None else applied_migrations
        )
        migrations_to_rollback = migrations_to_rollback[::-1]

        for migration in migrations_to_rollback:
            if migration.down is not None:
                await self.backend.apply_migration(migration.down)
            await self.backend.unregister_migration(migration)

        self.applied_migrations = await self.backend.get_applied_migrations()
