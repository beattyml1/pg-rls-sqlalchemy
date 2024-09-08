from enum import Enum

from sqlalchemy import Column, Table
from sqlalchemy.orm import DeclarativeBase

from pgalchemy import PolicyCommands


def deny_for_column(operation: PolicyCommands, role: str):
    def wrapper(col: Column):
        manage_cls_for_combo(role=role, column=col)
        col.info.setdefault('__col_sec__', [])
        col.info['__col_sec__'].append([('REVOKE', operation, role)])
        return col


def allow_for_column(operation: PolicyCommands, role: str):
    def wrapper(col: Column):
        manage_cls_for_combo(role=role, column=col)
        col.info.setdefault('__col_sec__', [])
        col.info['__col_sec__'].append([('GRANT', operation, role)])
        return col



class All(Enum):
    All = "__ALL__"


_cls_registry = []

def column_is_managed(role: str, column: Column):
    return any(
        (r in (role, All.All)) and
        (s in (column.table.schema, All.All)) and
        (t in (column.table.name, All.All)) and
        (c in (column.name, All.All))
        for r, s, t, c in _cls_registry
    )

def manage_cls_for_combo(
        role: str | All = All.All,
        schema: str | All = All.All,
        table: Table | DeclarativeBase | str | All = All.All,
        column: Column | str | All = All.All
):
    if isinstance(table, DeclarativeBase):
        table = table.__table__

    if isinstance(column, Column) and column.table:
        table = column.table
        column = column.name

    if isinstance(table, Table):
        schema = table.schema
        table = table.name

    _cls_registry.append(
        (role, schema, table, column)
    )