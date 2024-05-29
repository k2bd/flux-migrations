def apply():
    return """
    create or replace view view2 as
    select id, value from another_table where value > 10;
    """
