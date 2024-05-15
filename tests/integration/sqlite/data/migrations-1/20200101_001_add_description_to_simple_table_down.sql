pragma foreign_keys=off;

create table simple_table_new (
    id integer primary key autoincrement,
    data text
);

insert into simple_table_new (id, data)
select id, data from simple_table;

drop table simple_table;
alter table simple_table_new rename to simple_table;

pragma foreign_keys=on;
