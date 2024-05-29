def apply():
    return """
    create table if not exists simple_table (
        id serial primary key,
        data text
    );
    """
