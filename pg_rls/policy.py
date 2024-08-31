from enum import Enum

from alembic_utils.pg_policy import PGPolicy
from sqlalchemy import Table, BinaryExpression


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
    def __init__(
            self,
            name: str,
            as_: PolicyType = PolicyType.PERMISSIVE,
            for_: PolicyCommands = PolicyCommands.ALL,
            using: str | BinaryExpression | None = None,
            with_check: str | BinaryExpression | None = None
    ):
        self.name = name
        self.with_check = str(with_check)
        self.using = str(using)
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
