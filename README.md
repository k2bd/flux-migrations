# Flux Migrations

[![codecov](https://codecov.io/gh/k2bd/flux-migrations/graph/badge.svg?token=PJF3cYLtZh)](https://codecov.io/gh/k2bd/flux-migrations)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/flux-migrations)
[![PyPI - Version](https://img.shields.io/pypi/v/flux-migrations)](https://pypi.org/project/flux-migrations/)


`flux` is a database migration tool written in Python and built with Python projects in mind.

## N.B. this project is in a pre-release state. It is not ready for use in any form. There may be major problems and breaking changes to all aspects of the tool while some decisions are being made and changed.

## Adding `flux` to your project

### CLI

``flux`` can be installed for now from Github. For example:

```
poetry add "git+https://github.com/k2bd/flux-migrations.git[postgres]"
```

The project will be properly maintained on PyPI when it's stable. The PyPI version may therefore not be up-to-date at this time.

``flux`` commands can then be listed with ``flux --help``.
Any subcommand can also be suffixed with ``--help`` for more information about it and any options and arguments.

The main commands are:

- ``flux init {backend}`` - Create a ``flux.toml`` file in the current directory for your project with an installed ``{backend}``
- ``flux new "Migration short description"`` - Create a new migration with the given short description
- ``flux apply {database-uri}`` Apply unapplied migrations to the target ``{database-uri}``
- ``flux rollback {database-uri}`` Rollback applied migrations from the target ``{database-uri}``

For example, migrations can be initialized and started with:

```
flux init postgres

flux new "Initial tables"
```

## Writing migrations

There are two forms that migrations can take in ``flux`` - Python files and sql files.
Both forms define up and optional down migrations.

### Migrations as Python files

By default ``flux`` creates Python migration files when you run ``flux new "My new migration"``.

A migration written as a Python file must contain at minimum a function ``apply() -> str`` which returns a string representing one or multiple sql statements for the migration.
It can also define a function ``undo() -> str`` that returns a down-migration.

Because these migration files are Python files, you can write reusable tools to help accelerate producing high quality migrations.
But because these functions must return a string, ``flux`` (along with a good testing and deployment strategy) can ensure that changes to these functions can't corrupt older migrations (see [below](#migration-directory-corruption-detection)).
A common pattern to work around this, in my experience at least, is to have versioned migration helpers suffixed with `_v1`, `_v2` etc, and each new migration is expected to use the latest version of any given helper.

This means you get the power of modular code when writing migrations *and* the security that resolved migrations remain immutable (even if the file creating the migration imports code from mutable modules).

For example:

```python
# -- migration_helpers.py

def table_admin_permissions_v1(table_name: str) -> str:
    return f"""
    grant select, insert, delete on table {table_name} to admin_user;
    """


def give_admin_permissions_v2(table_name: str) -> str:
    return f"""
    {table_admin_permissions_v1(table_name)}
    grant select on table {table_name} to read_only_admin_user;
    """
```

A migration that was created when only v1 of our helper was available:

```python
# -- 20200202_001_some-old-migration.py

from some_package.migration_helpers import table_admin_permissions_v1


def apply():
    f"""
    create table users (
        id uuid not null primary key default gen_random_uuid(),
        name text not null
    );
    {table_admin_permissions_v1("users")}
    """
```

And another created later with v2:

```python
# -- 20240422_002_shiny-new-migration.py

from some_package.migration_helpers import table_admin_permissions_v2


def apply():
    f"""
    create table user_posts (
        id uuid not null primary key default gen_random_uuid(),
        user_id uuid not null references users (id),
        posted_at timestamp not null default (now() at time zone 'utc'),
        content text not null
    );
    {table_admin_permissions_v2("user_posts")}
    """
```

(These examples only contain apply functions for brevity - real migrations should have undo steps!)

### Migrations as sql files

It may be that you prefer just writing sql files for your migrations, and you just want ``flux`` for its flexibility or testing functionality.
That's cool too, just run ``flux new --kind sql "My new migration"``.

Up-migration files are just files ending with ``.sql``. They can have down-migration counterparts ending with ``.undo.sql``.

These files just contain sql, but as above the hash of the up migration is stored for [detecting migration directory corruption](#migration-directory-corruption-detection).

## Migration directory corruption detection

The hash of the up-migration is stored by ``flux`` to check for migration directory corruption.
That is, the content of past migrations are not allowed to change so the record of applied migrations is clear in all environments.
If ``flux`` sees that a previously-applied migration has changed content when validating migrations (as a standalone command or as part of e.g. ``apply``), it will raise an error.

## Use as a library

``flux`` can be used as a library in your Python project to manage migrations programmatically.
This can be particularly useful for testing.

(TODO: Demo API)

## Database backends

``flux`` is a generic migration tool that can be adapted for use in many databases. It does this by having an abstract backend specification that can be implemented for any target DBMS. Backends can also have their own configuration options.

### Inbuilt backends

#### Postgres

``flux`` comes optionally packaged with a Postgres backend if installed with the `postgres` extra. It maintains information about migrations in a configurable schema and table. Additionally, it uses an advisory lock while migrations are being applied with a configurable index. The available ``[backend]`` configs are:

- ``migrations_schema``
    - The schema in which to put the migration history table
    - (default "public")
- ``migrations_table``
    - The table used for applied migration history
    - (default "_flux_migrations")
- ``migrations_lock_id``
    - The ``pg_advisory_lock`` ID to use while applying migrations
    - (default 3589 ('flux' on a phone keypad))

### Adding a new backend

Backends are loaded as plugins through Python's entry point system.
This means that you can add a new backend by simply installing a package that provides the backend as a plugin.

To create a new backend in your package, you need to subclass ``flux.MigrationBackend`` and implement its abstract methods.
Then register that class under the ``flux.backend`` entry point group in your package setup.

For example, in ``pyproject.toml``:
    
```toml
[project.entry-points."flux.backend"]
cooldb = "my_package.my_module:CoolDbBackend"
```

When the new package is installed in the same environment as ``flux``, the backend will be available to use with ``flux``.
An example ``flux.toml`` file that uses our new backend:

```toml
[flux]
backend = "cooldb"
migration_directory = "migrations"

[backend]
coolness_level = 11
another_option = "cool_value"
```

## Why `flux`?

I have used a number of migration frameworks for databases that sit behind Python projects.
I've liked some features of different projects but the complete feature-set I'd like to use in my work has never been in one project.

A non-exhaustive list of this feature-set includes
- very flexible support for repeatable migration scripts
- migration directory corruption detection
- the ability to easily leverage Python to reuse code in migrations
- a Python library to easily manage migrations programmatically for test writing (e.g. integration tests of the effects of individual migrations)

So, the motivation for this project was to
- present a more complete feature-set you'd want to find in a migration framework for use with Python projects
- use design patterns that make it easy to adapt for different kinds of projects, such as the plugin-based backend system
