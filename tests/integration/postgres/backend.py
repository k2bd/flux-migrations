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

DEFAULT_MIGRATIONS_SCHEMA = "public"
DEFAULT_MIGRATIONS_TABLE = "_flux_migrations"
DEFAULT_MIGRATIONS_LOCK_ID = 3589


@dataclass
class TestingPostgresBackend(MigrationBackend):
    database_url: str
    migrations_table: str = DEFAULT_MIGRATIONS_TABLE
    migrations_schema: str = DEFAULT_MIGRATIONS_SCHEMA
    migrations_lock_id: int = DEFAULT_MIGRATIONS_LOCK_ID

    _db: Database = field(init=False, repr=False)
    _tx: Transaction = field(init=False, repr=False)
    _conn: Connection = field(init=False, repr=False)

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
            "migrations_table", DEFAULT_MIGRATIONS_TABLE
        )
        migrations_lock_id = config.backend_config.get(
            "migrations_lock_id", DEFAULT_MIGRATIONS_LOCK_ID
        )
        if not re.match(VALID_TABLE_NAME, migrations_table):
            raise ValueError("Invalid table name provided.")
        migrations_schema = config.backend_config.get(
            "migrations_schema", DEFAULT_MIGRATIONS_SCHEMA
        )
        return cls(
            database_url=database_url,
            migrations_table=migrations_table,
            migrations_lock_id=migrations_lock_id,
            migrations_schema=migrations_schema,
        )

    @asynccontextmanager
    async def connection(self):
        """
        Create a connection that lasts as long as the context manager is
        active.
        """
        self._db = Database(self.database_url)
        await self._db.connect()
        async with self._db.transaction() as tx:
            self._tx = tx
            self._conn = self._db.connection()
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
        try:
            yield
        except Exception:
            await self._conn.rollback()
            raise
        else:
            await self._conn.commit()

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
        await self._conn.execute(
            "select pg_advisory_lock(:lock_id)",
            {"lock_id": self.migrations_lock_id},
        )
        try:
            yield
        finally:
            await self._conn.execute(
                "select pg_advisory_unlock(:lock_id)",
                {"lock_id": self.migrations_lock_id},
            )

    async def is_initialized(self) -> bool:
        """
        Check if the backend is initialized
        """
        schema_result = await self._conn.fetch_val(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema_name;",
            {"schema_name": self.migrations_schema},
        )
        if schema_result is None:
            return False

        table_result = await self._conn.fetch_val(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = :schema_name AND table_name = :table_name;",
            {
                "schema_name": self.migrations_schema,
                "table_name": self.migrations_table,
            },
        )
        if table_result is None:
            return False

        return True

    async def initialize(self):
        """
        Initialize the backend by creating any necessary tables etc in the
        database.
        """
        await self._conn.execute(
            f"""
            create table if not exists {self.migrations_schema}.{self.migrations_table}
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
        row = await self._conn.fetch_one(
            f"""
                insert into {self.migrations_table}
                (id, hash, applied_at)
                values (:migration_id, :up_hash, current_timestamp)
                returning *
            """,
            {
                "migration_id": migration.id,
                "up_hash": migration.up_hash,
            },
        )
        if row is None:
            raise RuntimeError("Failed to register migration")
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
        await self._conn.execute(content)

    async def get_applied_migrations(self) -> set[AppliedMigration]:
        """
        Get the set of applied migrations.
        """
        result = await self._conn.fetch_val(
            f"select id, hash, applied_at from {self.migrations_table}"
        )
        return {
            AppliedMigration(id=row[0], hash=row[1], applied_at=row[2])
            for row in result
        }

    # -- Testing methods

    async def table_info(self, table_name: str):
        """
        Get information about a table in the database
        """
        cursor = await self._conn.execute(f"pragma table_info({table_name})")
        return await cursor.fetchall()

    async def get_all_rows(self, table_name: str):
        """
        Get all rows from a table
        """
        cursor = await self._conn.execute(f"select * from {table_name}")
        return await cursor.fetchall()
