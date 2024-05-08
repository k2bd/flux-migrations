import datetime as dt
from dataclasses import dataclass


@dataclass
class AppliedMigration:
    """
    Information about a migration as applied to a database
    """

    #: The ID of the migration
    id: str

    #: The hash of the migration text content
    hash: str

    #: The timestamp when the migration was applied
    applied_at: dt.datetime
