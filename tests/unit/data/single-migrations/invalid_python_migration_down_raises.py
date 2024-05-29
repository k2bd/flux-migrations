def apply() -> str:
    return "create table example_table ( id serial primary key, name text );"


def undo() -> str | None:
    raise ValueError("This migration is invalid")
