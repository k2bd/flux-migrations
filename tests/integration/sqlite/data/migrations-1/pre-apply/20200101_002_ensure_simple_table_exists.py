def apply():
    return """
    create table if not exists simple_table (
        id integer primary key autoincrement,
        data text
    );
    """
