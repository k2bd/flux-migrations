import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import aiosqlite

from flux.backend.applied_migration import AppliedMigration
from flux.backend.base import MigrationBackend
from flux.config import FluxConfig
from flux.migration.migration import Migration

VALID_TABLE_NAME = r"^[A-Za-z0-9_]+$"


@dataclass
class SQLiteBackend(MigrationBackend):
    db_path: str
    migrations_table: str

    _conn: Any = field(init=False, repr=False)

    @classmethod
    def from_config(cls, config: FluxConfig) -> "SQLiteBackend":
        """
        Create a MigrationBackend from a configuration

        This config appears in the config.toml file in the "backend" section.
        """
        db_path = config.backend_config.get("database", ":memory:")
        migrations_table = config.backend_config.get(
            "migrations_table", "_flux_migrations"
        )
        if not re.match(VALID_TABLE_NAME, migrations_table):
            raise ValueError("Invalid table name provided.")
        return cls(
            db_path=db_path,
            migrations_table=migrations_table,
        )

    @asynccontextmanager
    async def connection(self):
        """
        Create a connection that lasts as long as the context manager is
        active.
        """
        async with aiosqlite.connect(self.db_path) as conn:
            self._conn = conn
            try:
                yield
            finally:
                self._conn = None

    @asynccontextmanager
    async def transaction(self):
        """
        Create a transaction that lasts as long as the context manager is
        active.

        The transaction is committed when the context manager exits.

        If an exception is raised inside the context manager, the transaction
        is rolled back.
        """
        try:
            yield
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise

    @asynccontextmanager
    async def migration_lock(self):
        """
        Create a lock that prevents other migration processes from running
        concurrently.

        The acceptable unlock conditions are:
        - The context manager exits
        - The transaction ends
        - The connection ends
        """
        await self._conn.execute("pragma locking_mode = exclusive;")
        await self._conn.execute("begin exclusive;")
        yield

    async def is_initialized(self) -> bool:
        """
        Check if the backend is initialized
        """
        cursor = await self._conn.execute(
            "select name from sqlite_master where type='table' and name = ?",
            (self.migrations_table,),
        )
        if (await cursor.fetchone()) is None:
            return False

        return True

    async def initialize(self):
        """
        Initialize the backend by creating any necessary tables etc in the
        database.
        """
        await self._conn.execute(
            f"""
            create table if not exists {self.migrations_table}
            (
                id text primary key,
                hash text not null,
                applied_at timestamp not null default current_timestamp
            )
            """,
        )

    async def register_migration(self, migration: Migration) -> AppliedMigration:
        """
        Register a migration as applied (when up-migrated)
        """
        cursor = await self._conn.execute(
            f"""
                insert into {self.migrations_table}
                (id, hash, applied_at)
                values (?, ?, current_timestamp)
                returning *
            """,
            (migration.id, migration.up_hash),
        )
        row = await cursor.fetchone()
        return AppliedMigration(id=row[0], hash=row[1], applied_at=row[2])

    async def unregister_migration(self, migration: Migration):
        """
        Unregister a migration (when down-migrated)
        """
        await self._conn.execute(
            f"delete from {self.migrations_table} where id = ?", (migration.id,)
        )

    async def apply_migration(self, content: str):
        """
        Apply the content of a migration to the database. This is used for both
        up and down migrations so should not register or unregister the
        migration hash.
        """
        await self._conn.executescript(content)

    async def get_applied_migrations(self) -> set[AppliedMigration]:
        """
        Get the set of applied migrations.
        """
        cursor = await self._conn.execute(
            f"select id, hash, applied_at from {self.migrations_table}"
        )
        return {
            AppliedMigration(id=row[0], hash=row[1], applied_at=row[2])
            for row in await cursor.fetchall()
        }
