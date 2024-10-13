import os

import pytest
from typer.testing import CliRunner

from flux.builtins.postgres import FluxPostgresBackend
from flux.cli import app
from flux.runner import FluxRunner
from tests.helpers import change_cwd
from tests.integration.postgres.helpers import postgres_config
from freezegun import freeze_time


def test_cli_init(example_project_dir: str):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 0, result.stdout

        assert os.path.exists("flux.toml")

        with open("flux.toml", "r") as f:
            config = f.read()

        assert config == (
            """\
[flux]
backend = "postgres"
migration_directory = "migrations"

[backend]
# Add backend-specific configuration here
"""
        )


def test_cli_init_twice_errors(example_project_dir: str):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        runner.invoke(app, ["init", "postgres"])
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 1

        assert "flux.toml already exists" in result.stdout


def test_cli_init_bad_backend(example_project_dir: str):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres-2"])
        assert result.exit_code == 1, result.stdout

        assert "Backend 'postgres-2' is not installed" in result.stdout


async def test_cli_apply_all_auto_approve(
    example_project_dir: str,
    example_migrations_dir: str,
    postgres_backend: FluxPostgresBackend,
    database_uri: str,
):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 0, result.stdout

        result = runner.invoke(app, ["apply", "--auto-approve", database_uri])
        assert result.exit_code == 0, result.stdout

    config = postgres_config(migration_directory=example_migrations_dir)
    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert {m.id for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table",
            "20200102_001_add_timestamp_to_another_table",
            "20200102_002_create_new_table",
        }


async def test_cli_apply_all_manual_approve(
    example_project_dir: str,
    example_migrations_dir: str,
    postgres_backend: FluxPostgresBackend,
    database_uri: str,
):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 0, result.stdout

        result = runner.invoke(app, ["apply", database_uri], input="y\n")
        assert result.exit_code == 0, result.stdout

    config = postgres_config(migration_directory=example_migrations_dir)
    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert {m.id for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table",
            "20200102_001_add_timestamp_to_another_table",
            "20200102_002_create_new_table",
        }


async def test_cli_apply_all_manual_reject(
    example_project_dir: str,
    example_migrations_dir: str,
    postgres_backend: FluxPostgresBackend,
    database_uri: str,
):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 0, result.stdout

        result = runner.invoke(app, ["apply", database_uri], input="n\n")
        assert result.exit_code == 1, result.stdout

    config = postgres_config(migration_directory=example_migrations_dir)
    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert {m.id for m in runner.applied_migrations} == set()


@pytest.mark.parametrize("n", [0, 1, 2, 3, 4, 5])
async def test_cli_apply_all_auto_approve_n(
    example_project_dir: str,
    example_migrations_dir: str,
    postgres_backend: FluxPostgresBackend,
    database_uri: str,
    n: int,
):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 0, result.stdout

        result = runner.invoke(app, ["apply", "--auto-approve", database_uri, str(n)])
        assert result.exit_code == 0, result.stdout

    config = postgres_config(migration_directory=example_migrations_dir)
    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert (
            sorted([m.id for m in runner.applied_migrations])
            == [
                "20200101_001_add_description_to_simple_table",
                "20200102_001_add_timestamp_to_another_table",
                "20200102_002_create_new_table",
            ][:n]
        )


async def test_cli_rollback_all_auto_approve(
    example_project_dir: str,
    example_migrations_dir: str,
    postgres_backend: FluxPostgresBackend,
    database_uri: str,
):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 0, result.stdout

        result = runner.invoke(app, ["apply", "--auto-approve", database_uri])
        assert result.exit_code == 0, result.stdout

        result = runner.invoke(app, ["rollback", "--auto-approve", database_uri])
        assert result.exit_code == 0, result.stdout

    config = postgres_config(migration_directory=example_migrations_dir)
    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert {m.id for m in runner.applied_migrations} == set()


async def test_cli_rollback_all_manual_approve(
    example_project_dir: str,
    example_migrations_dir: str,
    postgres_backend: FluxPostgresBackend,
    database_uri: str,
):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 0, result.stdout

        result = runner.invoke(app, ["apply", "--auto-approve", database_uri])
        assert result.exit_code == 0, result.stdout

        result = runner.invoke(app, ["rollback", database_uri], input="y\n")
        assert result.exit_code == 0, result.stdout

    config = postgres_config(migration_directory=example_migrations_dir)
    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert {m.id for m in runner.applied_migrations} == set()


async def test_cli_rollback_all_manual_reject(
    example_project_dir: str,
    example_migrations_dir: str,
    postgres_backend: FluxPostgresBackend,
    database_uri: str,
):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 0, result.stdout

        result = runner.invoke(app, ["apply", "--auto-approve", database_uri])
        assert result.exit_code == 0, result.stdout

        result = runner.invoke(app, ["rollback", database_uri], input="n\n")
        assert result.exit_code == 1, result.stdout

    config = postgres_config(migration_directory=example_migrations_dir)
    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert {m.id for m in runner.applied_migrations} == {
            "20200101_001_add_description_to_simple_table",
            "20200102_001_add_timestamp_to_another_table",
            "20200102_002_create_new_table",
        }


@pytest.mark.parametrize("n", [0, 1, 2, 3, 4, 5])
async def test_cli_rollback_all_auto_approve_n(
    example_project_dir: str,
    example_migrations_dir: str,
    postgres_backend: FluxPostgresBackend,
    database_uri: str,
    n: int,
):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 0, result.stdout

        result = runner.invoke(app, ["apply", "--auto-approve", database_uri])
        assert result.exit_code == 0, result.stdout

        result = runner.invoke(
            app, ["rollback", "--auto-approve", database_uri, str(n)]
        )
        assert result.exit_code == 0, result.stdout

    config = postgres_config(migration_directory=example_migrations_dir)
    async with FluxRunner(config=config, backend=postgres_backend) as runner:
        initialized = await postgres_backend.is_initialized()
        assert initialized is True

        assert (
            sorted([m.id for m in runner.applied_migrations])
            == [
                "20200101_001_add_description_to_simple_table",
                "20200102_001_add_timestamp_to_another_table",
                "20200102_002_create_new_table",
            ][: max(3 - n, 0)]
        )


async def test_cli_new_python(
    example_project_dir: str,
    example_migrations_dir: str,
    postgres_backend: FluxPostgresBackend,
    database_uri: str,
):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 0, result.stdout

        with freeze_time("2021-02-03T04:05:06Z"):
            result = runner.invoke(app, ["new", "A new migration"])

        assert result.exit_code == 0, result.stdout

        with freeze_time("2021-02-03T05:06:07Z"):
            result = runner.invoke(
                app, ["new", "--kind", "python", "Another new migration"]
            )

        assert result.exit_code == 0, result.stdout

    first_migration_file = os.path.join(
        example_migrations_dir, "20210203_001_a-new-migration.py"
    )
    second_migration_file = os.path.join(
        example_migrations_dir, "20210203_002_another-new-migration.py"
    )

    assert os.path.exists(first_migration_file)
    assert os.path.exists(second_migration_file)

    with open(first_migration_file, "r") as f:
        content = f.read()
        assert (
            content
            == '''\
"""
A new migration
"""


def apply():
    return """ """


def undo():
    return """ """

'''
        )

    with open(second_migration_file, "r") as f:
        content = f.read()
        assert (
            content
            == '''\
"""
Another new migration
"""


def apply():
    return """ """


def undo():
    return """ """

'''
        )


async def test_cli_new_sql(
    example_project_dir: str,
    example_migrations_dir: str,
    postgres_backend: FluxPostgresBackend,
    database_uri: str,
):
    with change_cwd(example_project_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 0, result.stdout

        with freeze_time("2021-02-03T04:05:06Z"):
            result = runner.invoke(app, ["new", "--kind", "sql", "A new migration"])

        assert result.exit_code == 0, result.stdout

        with freeze_time("2021-02-03T05:06:07Z"):
            result = runner.invoke(
                app, ["new", "--kind", "sql", "Another new migration"]
            )

        assert result.exit_code == 0, result.stdout

    first_up_migration_file = os.path.join(
        example_migrations_dir, "20210203_001_a-new-migration.sql"
    )
    first_down_migration_file = os.path.join(
        example_migrations_dir, "20210203_001_a-new-migration.undo.sql"
    )
    second_up_migration_file = os.path.join(
        example_migrations_dir, "20210203_002_another-new-migration.sql"
    )
    second_down_migration_file = os.path.join(
        example_migrations_dir, "20210203_002_another-new-migration.undo.sql"
    )

    assert os.path.exists(first_up_migration_file)
    assert os.path.exists(first_down_migration_file)
    assert os.path.exists(second_up_migration_file)
    assert os.path.exists(second_down_migration_file)

    with open(first_up_migration_file, "r") as f:
        content = f.read()
        assert content == ""

    with open(second_up_migration_file, "r") as f:
        content = f.read()
        assert content == ""

    with open(first_down_migration_file, "r") as f:
        content = f.read()
        assert content == ""

    with open(second_down_migration_file, "r") as f:
        content = f.read()
        assert content == ""
