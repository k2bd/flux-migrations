def apply():
    return """
    alter table another_table add column timestamp text;
    """


def undo():
    return """
    alter table another_table drop column timestamp;
    """
