def apply():
    return """
    create table if not exists new_table (
        id integer primary key autoincrement,
        info text
    )
    """
