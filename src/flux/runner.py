from contextlib import AsyncExitStack
from dataclasses import dataclass, field

from flux.backend.applied_migration import AppliedMigration
from flux.backend.base import MigrationBackend
from flux.backend.get_backends import get_backend
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

    _exit_stack: AsyncExitStack = field(init=False)

    pre_apply_migrations: list[Migration] = field(init=False)
    migrations: list[Migration] = field(init=False)
    post_apply_migrations: list[Migration] = field(init=False)

    applied_migrations: set[AppliedMigration] = field(init=False)

    @classmethod
    def from_file(cls, path: str, connection_uri: str) -> "FluxRunner":
        config = FluxConfig.from_file(path)
        backend = get_backend(config.backend).from_config(config, connection_uri)
        return cls(config=config, backend=backend)

    async def __aenter__(self):
        self._exit_stack = AsyncExitStack()
        await self._exit_stack.__aenter__()

        await self._exit_stack.enter_async_context(self.backend.connection())
        await self._exit_stack.enter_async_context(self.backend.migration_lock())
        await self._exit_stack.enter_async_context(self.backend.transaction())

        if not await self.backend.is_initialized():
            await self.backend.initialize()

        self.pre_apply_migrations = read_pre_apply_migrations(config=self.config)
        self.migrations = read_migrations(config=self.config)
        self.post_apply_migrations = read_post_apply_migrations(config=self.config)

        self.applied_migrations = await self.backend.get_applied_migrations()

        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._exit_stack.__aexit__(exc_type, exc, tb)

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

    async def _apply_pre_apply_migrations(self):
        for migration in self.pre_apply_migrations:
            try:
                await self.backend.apply_migration(migration.up)
            except Exception as e:
                raise MigrationApplyError(
                    f"Failed to apply pre-apply migration {migration.id}"
                ) from e

    async def _apply_post_apply_migrations(self):
        for migration in self.post_apply_migrations:
            try:
                await self.backend.apply_migration(migration.up)
            except Exception as e:
                raise MigrationApplyError(
                    f"Failed to apply post-apply migration {migration.id}"
                ) from e

    def list_applied_migrations(self) -> list[Migration]:
        """
        List applied migrations
        """
        return [
            m
            for m in self.migrations
            if m.id in {m.id for m in self.applied_migrations}
        ]

    def list_unapplied_migrations(self) -> list[Migration]:
        """
        List unapplied migrations
        """
        return [
            m
            for m in self.migrations
            if m.id not in {m.id for m in self.applied_migrations}
        ]

    def migrations_to_apply(self, n: int | None = None):
        unapplied_migrations = self.list_unapplied_migrations()
        return unapplied_migrations[:n]

    async def apply_migrations(self, n: int | None = None):
        """
        Apply unapplied migrations to the database
        """
        await self.validate_applied_migrations()

        migrations_to_apply = self.migrations_to_apply(n=n)

        await self._apply_pre_apply_migrations()

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

        await self._apply_post_apply_migrations()

        self.applied_migrations = await self.backend.get_applied_migrations()

    def migrations_to_rollback(self, n: int | None = None):
        applied_migrations = self.list_applied_migrations()
        migrations_to_rollback = (
            applied_migrations[-n:] if n is not None else applied_migrations
        )
        return migrations_to_rollback[::-1]

    async def rollback_migrations(self, n: int | None = None):
        """
        Rollback applied migrations from the database, applying any undo
        migrations if they exist.
        """
        await self.validate_applied_migrations()

        if self.config.apply_repeatable_on_down:
            await self._apply_pre_apply_migrations()

        migrations_to_rollback = self.migrations_to_rollback(n=n)

        for migration in migrations_to_rollback:
            try:
                if migration.down is not None:
                    await self.backend.apply_migration(migration.down)
                await self.backend.unregister_migration(migration)
            except Exception as e:
                raise MigrationApplyError(
                    f"Failed to rollback migration {migration.id}"
                ) from e

        if self.config.apply_repeatable_on_down:
            await self._apply_post_apply_migrations()

        self.applied_migrations = await self.backend.get_applied_migrations()
