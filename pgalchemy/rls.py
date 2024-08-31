from typing import Type, List, Optional

from alembic_utils.pg_policy import PGPolicy
from sqlalchemy import Table
from sqlalchemy.event import listens_for
from sqlalchemy.orm import DeclarativeBase, Mapper

from .policy import Policy


class RlsData:
    def __init__(self, active):
        self.active = active


def rls_base(Base: Type[DeclarativeBase], default_active: bool = True):
    class WithRls(Base):
        __rls__ = RlsData(default_active)

    @listens_for(WithRls, 'after_configured')
    def receive_mapper_configured(mapper: Mapper, class_: Type[WithRls]):
        table: Table = mapper.mapped_table()
        rls = getattr(class_, '__rls__')
        table.info.setdefault('rls', rls)
    return WithRls


def rls(enabled=True, policies: Optional[List[Policy]] = None):
    def wrapper(Model: Type[DeclarativeBase]):
        Model.__rls__ = RlsData(enabled)
        return Model
    return wrapper


def rls_for_table(enabled=True, policies: Optional[List[Policy]] = None):
    def wrapper(table: Table):
        data = RlsData(enabled)
        data.policies = policies or []
        table.info.setdefault('rls', data)
        return Table
    return wrapper
