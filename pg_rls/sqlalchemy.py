from typing import Type, List, Optional

from sqlalchemy import Table
from sqlalchemy.event import listens_for
from sqlalchemy.orm import DeclarativeBase, Mapper

from pg_rls import Policy, PolicyType


class RlsData:
    def __init__(self, active):
        self.active = active
        self.policies: List[Policy] = []


def rls_base(Base: Type[DeclarativeBase], default_active: bool = True):
    class WithRls(Base):
        __rls__ = RlsData(default_active)
    return WithRls

    @listens_for(WithRls, 'after_configured')
    def receive_mapper_configured(mapper: Mapper, class_: Type[WithRls]):
        table: Table = mapper.mapped_table()
        table.info.setdefault('rls', getattr(class_, '__rls__'))


def rls(enabled=True, policies: Optional[List[Policy]] = None):
    def wrapper(Model: Type[DeclarativeBase]):
        Model.__rls__ = RlsData(enabled)
        Model.__rls__.policies = policies or []
        return Model
    return wrapper


def rls_for_table(enabled=True, policies: Optional[List[Policy]] = None):
    def wrapper(table: Table):
        data = RlsData(enabled)
        data.policies = policies or []
        table.info.setdefault('rls', data)
        return Table
    return wrapper


def policy(pol: Policy):
    def wrapper(Model: Type[DeclarativeBase]):
        if not Model.__rls__:
            Model.__rls__ = RlsData(True)
        Model.__rls__.policies.append(pol)
        return Model
    return wrapper

