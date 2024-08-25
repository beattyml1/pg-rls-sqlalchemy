from enum import Enum


class PolicyType(Enum):
    PERMISSIVE = 'PERMISSIVE'
    RESTRICTIVE = 'RESTRICTIVE'


class PolicyCommands(Enum):
    ALL = 'ALL'
    SELECT = 'SELECT'
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'


class Policy:
    def __init__(self, name: str, as_: PolicyType, for_: PolicyCommands, using: str = None, with_check: str = None):
        self.name = name
        self.with_check = with_check
        self.using = using
        self.for_ = for_
        self.as_ = as_

    @classmethod
    def from_pg_policy(cls, row):
        return Policy(
            name=row['policyname'],
            as_=PolicyType(row['permissive']),
            for_=PolicyCommands(row['cmd']),
            using=row['qual'],
            with_check=row['with_check'],
        )
