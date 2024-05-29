def apply():
    return """
    create table if not exists new_table (
        id serial primary key,
        info text
    )
    """
