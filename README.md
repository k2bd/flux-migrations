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

``flux`` commands can then be listed with ``flux --help``

For example, migrations can be initialized and started with:

```
flux init postgres

flux new "Initial migration"
```

### Docker

(TODO)

## Writing migrations

## Use as a library

``flux`` can be used as a library in your Python project to manage migrations programmatically.
This can be particularly useful for testing.

## Database backends

``flux`` is a generic migration tool that can be adapted for use in many databases. It does this by having an abstract backend specification that can be implemented for any target database. Backends can also have their own configuration options.

### Inbuilt backends

#### Postgres

``flux`` comes packaged with a Postgres backend. It maintains information about migrations in a configurable schema and table. Additionally, it uses an advisory lock while migrations are being applied with a configurable index. The available ``[backend]`` configs are:

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

Once the package is installed, the backend will be available to use with a `flux`, as long as it's installed in the same environment.
An example configuration file for our new backend:

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
- use design patterns that make it easy to adapt for different kinds of projects, such as 
  - the plugin-based backend system
  - the co-maintenance of official Docker images
