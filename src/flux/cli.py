import os
from dataclasses import dataclass
from typing import Optional

import typer
from rich import print
from rich.prompt import Prompt
from typing_extensions import Annotated

from flux.config import FluxConfig
from flux.constants import FLUX_CONFIG_FILE, FLUX_DEFAULT_MIGRATION_DIRECTORY


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
