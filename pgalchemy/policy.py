from enum import Enum
from typing import Type

from alembic_utils.pg_policy import PGPolicy
from sqlalchemy import Table, BinaryExpression, Engine
from sqlalchemy.orm import DeclarativeBase


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
            on: Type[DeclarativeBase] | Table,
            name: str,
            as_: PolicyType = PolicyType.PERMISSIVE,
            for_: PolicyCommands = PolicyCommands.ALL,
            using: str | BinaryExpression | None = None,
            with_check: str | BinaryExpression | None = None
    ):
        self.on = on
        self.name = name
        self.with_check = str(with_check)
        self.using = str(using)
        self.for_ = for_
        self.as_ = as_


    @property
    def table(self) -> Table:
        if isinstance(self.model, type) and issubclass(self.model, DeclarativeBase):
            return self.model.__table__
        elif isinstance(self.model, Table):
            return self.model
        else:
            raise Exception('Unsupported model')

    def attach(self):
        return PGPolicy(
            on_entity=self.table.name,
            schema=self.table.schema,
            signature=self.name,
            definition=self.definition_sql()
        )

    def _as_fragment(self):
        return f"as {self.as_}\n" if self.as_ else ""

    def _from_fragment(self):
        return f"for {self.for_}\n" if self.for_ else ""

    def _with_check_fragment(self):
        return f"with check {self.with_check}\n" if self.with_check else ""

    def _using_fragment(self):
        return f"using {self.using}\n" if self.using else ""

    def definition_sql(self):
        return self._as_fragment() + self._from_fragment() + self._using_fragment() + self._with_check_fragment()