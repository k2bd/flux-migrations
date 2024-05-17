def apply() -> str:
    raise ValueError("This migration is invalid.")


def undo() -> str | None:
    return "drop table example_table;"
