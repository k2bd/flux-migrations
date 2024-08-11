import os

from typer.testing import CliRunner

from flux.cli import app
from tests.helpers import change_cwd


def test_cli_init(example_migrations_dir: str, mock_installed_postgres_backend):
    with change_cwd(example_migrations_dir):
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


def test_cli_init_twice_errors(
    example_migrations_dir: str, mock_installed_postgres_backend
):
    with change_cwd(example_migrations_dir):
        runner = CliRunner()
        runner.invoke(app, ["init", "postgres"])
        result = runner.invoke(app, ["init", "postgres"])
        assert result.exit_code == 1

        assert "flux.toml already exists" in result.stdout


def test_cli_init_bad_backend(
    example_migrations_dir: str, mock_installed_postgres_backend
):
    with change_cwd(example_migrations_dir):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "postgres-2"])
        assert result.exit_code == 1, result.stdout

        assert "Backend 'postgres-2' is not installed" in result.stdout
