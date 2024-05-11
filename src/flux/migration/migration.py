import hashlib
from dataclasses import dataclass


@dataclass
class Migration:
    up: str
    down: str | None

    @property
    def up_hash(self) -> str:
        """
        Return the hash of the up-migration content
        """
        return hashlib.md5(self.up.encode()).hexdigest()
