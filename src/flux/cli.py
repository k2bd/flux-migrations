import asyncio
import datetime as dt
import os
from dataclasses import dataclass
from typing import Optional

import typer
from rich import print
from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.table import Table
from typing_extensions import Annotated

from flux.backend.get_backends import get_backend
from flux.config import FluxConfig
from flux.constants import (
    FLUX_CONFIG_FILE,
    FLUX_DEFAULT_MIGRATION_DIRECTORY,
    POST_APPLY_DIRECTORY,
    PRE_APPLY_DIRECTORY,
)
from flux.exceptions import BackendNotInstalledError
from flux.runner import FluxRunner

APPLIED_STATUS = "Applied"
TO_APPLY_STATUS = "To Apply"
TO_ROLLBACK_STATUS = "To Undo"
NOT_APPLIED_STATUS = "Not Applied"


def async_run(coro):
    # Temp ugly workaround for testing until typer supports async
    # See fastapi/typer#950
    if os.environ.get("FLUX_TESTING"):
        import nest_asyncio

        nest_asyncio.apply()
    asyncio.run(coro)


@dataclass
class _CliState:
    config: FluxConfig | None = None


app = typer.Typer()


@app.callback()
def prepare_state(ctx: typer.Context) -> FluxConfig | None:
    if os.path.exists(FLUX_CONFIG_FILE):
        config = FluxConfig.from_file(FLUX_CONFIG_FILE)
    else:
        config = None

    state = _CliState(config=config)

    ctx.obj = state


@app.command()
def init(
    ctx: typer.Context,
    backend: Annotated[
        Optional[str], typer.Argument(help="The name of the backend to use")
    ] = None,
    migration_dir: Annotated[
        str, typer.Option(help="The directory to store migration files in")
    ] = FLUX_DEFAULT_MIGRATION_DIRECTORY,
    log_level: Annotated[Optional[str], typer.Option()] = None,
):
    config = ctx.obj.config
    if config is not None:
        print(f"{FLUX_CONFIG_FILE} already exists")
        raise typer.Exit(code=1)

    if backend is None:
        backend = Prompt.ask("Enter the name of the backend to use")

    try:
        get_backend(backend)
    except BackendNotInstalledError:
        print(
            f"Backend {backend!r} is not installed. Please refer to the flux documentation for instructions on installing new backends."  # noqa: E501
        )
        raise typer.Exit(code=1)

    with open(FLUX_CONFIG_FILE, "w") as f:
        f.write(
            f"""[flux]
backend = "{backend}"
migration_directory = "{migration_dir}"
"""
        )

        if log_level is not None:
            f.write(
                f"""log_level = "{log_level}"
"""
            )

        f.write(
            """
[backend]
# Add backend-specific configuration here
"""
        )

    print(f"Created {FLUX_CONFIG_FILE}")


async def _new(
    ctx: typer.Context,
    name: str,
    sql: bool,
    pre: bool,
    post: bool,
):
    config: FluxConfig | None = ctx.obj.config
    if config is None:
        print("Please run `flux init` to create a configuration file")
        raise typer.Exit(code=1)

    if pre and post:
        print("Cannot create migration with both --pre and --post")
        raise typer.Exit(code=1)

    repeatable = pre or post

    target_dir = config.migration_directory
    if pre:
        target_dir = os.path.join(target_dir, PRE_APPLY_DIRECTORY)
    if post:
        target_dir = os.path.join(target_dir, POST_APPLY_DIRECTORY)

    os.makedirs(target_dir, exist_ok=True)

    date_part = dt.date.today().strftime("%Y%m%d")
    migration_index = 1

    name_part = "-".join(name.lower().split())

    def migration_filename_prefix() -> str:
        return f"{date_part}_{migration_index:>03}"

    migration_filenames = os.listdir(target_dir)
    while any(
        [
            filename.startswith(migration_filename_prefix())
            for filename in migration_filenames
        ]
    ):
        migration_index += 1

    migration_basename = f"{migration_filename_prefix()}_{name_part}"

    if sql:
        with open(os.path.join(target_dir, f"{migration_basename}.sql"), "w") as f:
            f.write("")
        if not repeatable:
            with open(
                os.path.join(target_dir, f"{migration_basename}.undo.sql"),
                "w",
            ) as f:
                f.write("")
    else:
        with open(os.path.join(target_dir, f"{migration_basename}.py"), "w") as f:
            f.write(
                f'''"""
{name}
"""


def apply():
    return """ """

'''
            )
            if not repeatable:
                f.write(
                    '''
def undo():
    return """ """

'''
                )


@app.command()
def new(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Migration name and default comment")],
    sql: Annotated[bool, typer.Option("--sql")] = False,
    pre: Annotated[bool, typer.Option("--pre")] = False,
    post: Annotated[bool, typer.Option("--post")] = False,
):
    async_run(_new(ctx=ctx, name=name, sql=sql, pre=pre, post=post))


def _print_status_report(runner: FluxRunner):
    table = Table(title="Status")
    table.add_column("ID")
    table.add_column("Status")

    for migration in runner.list_applied_migrations():
        table.add_row(migration.id, APPLIED_STATUS)

    for migration in runner.list_unapplied_migrations():
        status = NOT_APPLIED_STATUS
        table.add_row(migration.id, status)

    console = Console()
    console.print(table)


def _print_apply_report(runner: FluxRunner, n: int | None):
    table = Table(title="Apply Migrations")
    table.add_column("ID")
    table.add_column("Status")

    migrations_to_apply = {m.id for m in runner.migrations_to_apply(n=n)}

    for migration in runner.list_applied_migrations():
        table.add_row(migration.id, APPLIED_STATUS)

    for migration in runner.list_unapplied_migrations():
        status = (
            TO_APPLY_STATUS
            if migration.id in migrations_to_apply
            else NOT_APPLIED_STATUS
        )
        table.add_row(migration.id, status)

    console = Console()
    console.print(table)


def _print_rollback_report(runner: FluxRunner, n: int | None):
    table = Table(title="Rollback Migrations")
    table.add_column("ID")
    table.add_column("Status")

    migrations_to_rollback = {m.id for m in runner.migrations_to_rollback(n=n)}

    for migration in runner.list_applied_migrations():
        status = (
            TO_ROLLBACK_STATUS
            if migration.id in migrations_to_rollback
            else APPLIED_STATUS
        )
        table.add_row(migration.id, status)

    for migration in runner.list_unapplied_migrations():
        table.add_row(migration.id, NOT_APPLIED_STATUS)

    console = Console()
    console.print(table)


async def _status(connection_uri: str):
    async with FluxRunner.from_file(
        path=FLUX_CONFIG_FILE,
        connection_uri=connection_uri,
    ) as runner:
        _print_status_report(runner=runner)


@app.command()
def status(connection_uri: str):
    async_run(_status(connection_uri=connection_uri))


async def _apply(
    ctx: typer.Context,
    connection_uri: str,
    n: int | None,
    auto_approve: bool = False,
):
    config: FluxConfig | None = ctx.obj.config
    if config is None:
        print("Please run `flux init` to create a configuration file")
        raise typer.Exit(code=1)
    async with FluxRunner.from_file(
        path=FLUX_CONFIG_FILE,
        connection_uri=connection_uri,
    ) as runner:
        _print_apply_report(runner=runner, n=n)
        if not auto_approve:
            if not Confirm.ask("Apply these migrations?"):
                raise typer.Exit(1)
        await runner.apply_migrations(n=n)


@app.command()
def apply(
    ctx: typer.Context,
    connection_uri: Annotated[
        str, typer.Argument(help="Connection URI of the database")
    ],
    n: Annotated[
        Optional[int],
        typer.Argument(
            help="Optional number of migrations to apply (defaults to all unapplied migrations)"  # noqa: E501
        ),
    ] = None,
    auto_approve: bool = False,
):
    async_run(
        _apply(ctx, connection_uri=connection_uri, n=n, auto_approve=auto_approve)
    )


async def _rollback(
    ctx: typer.Context,
    connection_uri: str,
    n: int | None,
    auto_approve: bool = False,
    repeatable: bool | None = None,
):
    config: FluxConfig | None = ctx.obj.config
    if config is None:
        print("Please run `flux init` to create a configuration file")
        raise typer.Exit(code=1)
    async with FluxRunner.from_file(
        path=FLUX_CONFIG_FILE,
        connection_uri=connection_uri,
    ) as runner:
        _print_rollback_report(runner=runner, n=n)
        if not auto_approve:
            if not Confirm.ask("Undo these migrations?"):
                raise typer.Exit(1)

        await runner.rollback_migrations(n=n, apply_repeatable=repeatable)


@app.command()
def rollback(
    ctx: typer.Context,
    connection_uri: Annotated[
        str, typer.Argument(help="Connection URI of the database")
    ],
    n: Annotated[
        Optional[int],
        typer.Argument(
            help="Optional number of migrations to rollback (defaults to all unapplied migrations)"  # noqa: E501
        ),
    ] = None,
    auto_approve: bool = False,
    repeatable: bool | None = None,
):
    async_run(
        _rollback(
            ctx,
            connection_uri=connection_uri,
            n=n,
            auto_approve=auto_approve,
            repeatable=repeatable,
        )
    )
