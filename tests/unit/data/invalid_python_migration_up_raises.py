def up() -> str:
    raise ValueError("This migration is invalid.")


def down() -> str | None:
    return "drop table example_table;"
