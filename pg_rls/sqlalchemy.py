from typing import Type, List

from sqlalchemy import Table
from sqlalchemy.event import listens_for
from sqlalchemy.orm import DeclarativeBase, Mapper

from pg_rls import Policy


class RlsData:
    def __init__(self, active):
        self.active = active
        self.policies: List[Policy] = []


def add_rls(Base: Type[DeclarativeBase], default_active: bool = True):
    class WithRls(Base):
        rls = RlsData(default_active)
    return WithRls

    @listens_for(WithRls, 'after_configured')
    def receive_mapper_configured(mapper: Mapper, class_: Type[WithRls]):
        table: Table = mapper.mapped_table()
        table.info.setdefault('rls', class_.rls)
