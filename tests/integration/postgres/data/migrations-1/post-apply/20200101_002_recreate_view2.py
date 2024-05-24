def apply():
    return """
    create view if not exists view2 as
    select id, value from another_table where value > 10;
    """
