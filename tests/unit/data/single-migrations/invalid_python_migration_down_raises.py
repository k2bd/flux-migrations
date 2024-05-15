def up() -> str:
    return "create table example_table ( id serial primary key, name text );"


def down() -> str | None:
    raise ValueError("This migration is invalid")
