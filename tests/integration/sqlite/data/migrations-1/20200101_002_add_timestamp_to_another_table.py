def up():
    return """
    alter table another_table add column timestamp text;
    """


def down():
    """
    pragma foreign_keys=off;

    create table another_table_new (
        id integer primary key autoincrement,
        value integer
    );

    insert into another_table_new (id, value)
    select id, value from another_table;

    drop table another_table;
    alter table another_table_new rename to another_table;

    pragma foreign_keys=on;
    """
