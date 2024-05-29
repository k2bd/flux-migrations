from dataclasses import dataclass, field
from typing import Any

from flux.backend.applied_migration import AppliedMigration
from flux.backend.base import MigrationBackend
from flux.config import FluxConfig
from flux.exceptions import MigrationApplyError, MigrationDirectoryCorruptedError
from flux.migration.migration import Migration
from flux.migration.read_migration import (
    read_migrations,
    read_post_apply_migrations,
    read_pre_apply_migrations,
)


@dataclass
class FluxRunner:
    """
    Migration runner, given a config and a backend.

    Must be used within an async context manager.
    """

    config: FluxConfig

    backend: MigrationBackend

    _conn_ctx: Any = field(init=False)
    _tx_ctx: Any = field(init=False)
    _lock_ctx: Any = field(init=False)

    pre_apply_migrations: list[Migration] = field(init=False)
    migrations: list[Migration] = field(init=False)
    post_apply_migrations: list[Migration] = field(init=False)

    applied_migrations: set[AppliedMigration] = field(init=False)

    async def __aenter__(self):
        self._conn_ctx = self.backend.connection()
        self._lock_ctx = self.backend.migration_lock()
        self._tx_ctx = self.backend.transaction()

        await self._conn_ctx.__aenter__()
        await self._lock_ctx.__aenter__()
        await self._tx_ctx.__aenter__()

        if not await self.backend.is_initialized():
            await self.backend.initialize()

        self.pre_apply_migrations = read_pre_apply_migrations(config=self.config)
        self.migrations = read_migrations(config=self.config)
        self.post_apply_migrations = read_post_apply_migrations(config=self.config)

        self.applied_migrations = await self.backend.get_applied_migrations()

        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._tx_ctx.__aexit__(exc_type, exc, tb)
        await self._lock_ctx.__aexit__(exc_type, exc, tb)
        await self._conn_ctx.__aexit__(exc_type, exc, tb)

    async def validate_applied_migrations(self):
        """
        Confirms the following for applied migrations:
        - There is no discontinuity in the applied migrations
        - The migration hashes of all applied migrations haven't changed
        """
        applied_migrations = sorted(self.applied_migrations, key=lambda m: m.id)
        if not applied_migrations:
            return

        last_applied_migration = applied_migrations[-1]
        applied_migration_files = [
            m for m in self.migrations if m.id <= last_applied_migration.id
        ]

        if [m.id for m in applied_migration_files] != [
            m.id for m in applied_migrations
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

        for migration in self.pre_apply_migrations:
            try:
                await self.backend.apply_migration(migration.up)
            except Exception as e:
                raise MigrationApplyError(
                    f"Failed to apply pre-apply migration {migration.id}"
                ) from e

        for migration in migrations_to_apply:
            if migration.id in {m.id for m in self.applied_migrations}:
                continue

            try:
                await self.backend.apply_migration(migration.up)
                await self.backend.register_migration(migration)
            except Exception as e:
                raise MigrationApplyError(
                    f"Failed to apply migration {migration.id}"
                ) from e

        for migration in self.post_apply_migrations:
            try:
                await self.backend.apply_migration(migration.up)
            except Exception as e:
                raise MigrationApplyError(
                    f"Failed to apply post-apply migration {migration.id}"
                ) from e

        self.applied_migrations = await self.backend.get_applied_migrations()

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
            try:
                if migration.down is not None:
                    await self.backend.apply_migration(migration.down)
                await self.backend.unregister_migration(migration)
            except Exception as e:
                raise MigrationApplyError(
                    f"Failed to rollback migration {migration.id}"
                ) from e

        self.applied_migrations = await self.backend.get_applied_migrations()
