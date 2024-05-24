import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from databases import Database
from databases.core import Connection, Transaction

from flux.backend.applied_migration import AppliedMigration
from flux.backend.base import MigrationBackend
from flux.config import FluxConfig
from flux.migration.migration import Migration

VALID_TABLE_NAME = r"^[A-Za-z0-9_]+$"


@dataclass
class TestingPostgresBackend(MigrationBackend):
    database_url: str
    migrations_table: str
    migrations_lock_id: int = 3589

    _db: Database = field(init=False, repr=False)
    _conn: Connection = field(init=False, repr=False)
    _tx: Transaction = field(init=False, repr=False)

    @classmethod
    def from_config(cls, config: FluxConfig) -> "TestingPostgresBackend":
        """
        Create a MigrationBackend from a configuration

        This config appears in the config.toml file in the "backend" section.
        """
        database_url = config.backend_config.get("database_url")
        if not database_url:
            raise ValueError("No database_url provided.")
        migrations_table = config.backend_config.get(
            "migrations_table", "_flux_migrations"
        )
        if not re.match(VALID_TABLE_NAME, migrations_table):
            raise ValueError("Invalid table name provided.")
        return cls(migrations_table=migrations_table)

    @asynccontextmanager
    async def connection(self):
        """
        Create a connection that lasts as long as the context manager is
        active.
        """
        self._db = Database(self.database_url)
        self._conn = await self._db.connect()
        try:
            yield
        finally:
            await self._db.disconnect()

    @asynccontextmanager
    async def transaction(self):
        """
        Create a transaction that lasts as long as the context manager is
        active.

        The transaction is committed when the context manager exits.

        If an exception is raised inside the context manager, the transaction
        is rolled back.
        """
        self._tx = self._db.transaction()
        await self._tx.start()
        try:
            yield
        except Exception as e:
            await self._tx.rollback()
            raise e
        else:
            await self._tx.commit()

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
        await self._conn.execute("select pg_advisory_lock(1)")

    async def is_initialized(self) -> bool:
        """
        Check if the backend is initialized
        """
        result = await self._conn.fetch_val(
            "select name from sqlite_master where type='table' and name = :table_name",
            {"table_name": self.migrations_table},
        )
        if result is None:
            return False

        return True

    async def initialize(self):
        """
        Initialize the backend by creating any necessary tables etc in the
        database.
        """
        await self._tx.execute(
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
        cursor = await self._tx.execute(
            f"""
                insert into {self.migrations_table}
                (id, hash, applied_at)
                values (?, ?, current_timestamp)
                returning *
            """,
            (migration.id, migration.up_hash),
        )
        row = await cursor.fetchone()
        if row is None:
            raise RuntimeError("Failed to register migration")
        return AppliedMigration(id=row[0], hash=row[1], applied_at=row[2])

    async def unregister_migration(self, migration: Migration):
        """
        Unregister a migration (when down-migrated)
        """
        await self._tx.execute(
            f"delete from {self.migrations_table} where id = ?", (migration.id,)
        )

    async def apply_migration(self, content: str):
        """
        Apply the content of a migration to the database. This is used for both
        up and down migrations so should not register or unregister the
        migration hash.
        """
        await self._tx.executescript(content)

    async def get_applied_migrations(self) -> set[AppliedMigration]:
        """
        Get the set of applied migrations.
        """
        cursor = await self._tx.execute(
            f"select id, hash, applied_at from {self.migrations_table}"
        )
        return {
            AppliedMigration(id=row[0], hash=row[1], applied_at=row[2])
            for row in await cursor.fetchall()
        }

    # -- Testing methods

    async def table_info(self, table_name: str):
        """
        Get information about a table in the database
        """
        cursor = await self._tx.execute(f"pragma table_info({table_name})")
        return await cursor.fetchall()

    async def get_all_rows(self, table_name: str):
        """
        Get all rows from a table
        """
        cursor = await self._tx.execute(f"select * from {table_name}")
        return await cursor.fetchall()
