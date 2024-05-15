create view if not exists view1 as
select id, data from simple_table where data is not null;
