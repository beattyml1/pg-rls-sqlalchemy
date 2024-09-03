import uuid
from datetime import datetime, date, time
from decimal import Decimal
from typing import get_origin, Optional, get_args, Union, Type, List, Generic, TypeVar, TypeVarTuple

import sqlalchemy
import sqlalchemy.dialects.postgresql
from sqlalchemy import Table, Select, Column, BinaryExpression
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql.elements import KeyedColumnElement

T = TypeVar('T')
Ts = TypeVarTuple('Ts')


def format_type(annotation: type):
    if annotation in []:
        return _format_primitive_type(annotation)
    if get_origin(annotation) is Optional:
        return format_type(get_args(annotation)[0]) + ' null'
    if get_origin(annotation) is Union:
        t = _format_union_type(annotation)
        return t
    if get_origin(annotation) is list or issubclass(get_origin(annotation), list):
        return f'table({format_type(get_args(annotation)[0])})'
    if issubclass(annotation, DeclarativeBase):
        return ', '.join([_format_column(column) for column in annotation.__table__.columns().values()])
    if issubclass(annotation, object):
        return ', '.join([f'{k} {format_type(t)}' for k, t in annotation.__annotations__.items()])
    raise ValueError("Invalid type annotation for SQL function generation: Must be: str, bool, int, float, Decimal, datetime, date, time, or a Optional or Union with None of those types")


def _format_column(column: KeyedColumnElement):
    return f'{column.name} {format_type(column.type)} {"not null" if column.nullable is False else "null"}'

def _format_type_for_select(annotation: Type[Select]) -> str:
    raise "Explicit type annotations not supported for type Select"


def _format_union_type(annotation):
    args = get_args(annotation)
    all_arg_len = len(args)
    main_args = [a for a in args if a is not None and a is not type(None)]
    main_args_len = len(main_args)
    if all_arg_len != 2 or main_args_len != 1:
        raise ValueError(
            "Invalid type annotation for SQL function generation: Union types must be a primitive sql compatible type unioned with None")
    t = format_type(main_args[0]) + ' null'
    return t


def _format_primitive_type(annotation):
    return {
        str: 'text',
        bool: 'boolean',
        int: 'bigint',
        float: 'double precision',
        Decimal: 'decimal',
        datetime: 'datetime',
        date: 'date',
        time: 'time',
        uuid.UUID: 'uuid',
        sqlalchemy.Integer: 'integer',
        sqlalchemy.Float: 'real',
        sqlalchemy.BigInteger: 'bigint',
        sqlalchemy.SmallInteger: 'smallint',
        sqlalchemy.Double: 'double precision',
        sqlalchemy.Numeric: 'numeric',
        sqlalchemy.UUID: 'uuid',
        sqlalchemy.dialects.postgresql.UUID: 'uuid',
        sqlalchemy.Date: 'date',
        sqlalchemy.Time: 'time',
        sqlalchemy.Boolean: 'boolean',
        sqlalchemy.Text: 'text',
        sqlalchemy.VARCHAR: 'varchar',
        sqlalchemy.NVARCHAR: 'varchar',
        sqlalchemy.CHAR: 'character',
        sqlalchemy.NCHAR: 'character',
        sqlalchemy.JSON: 'JSON',
        sqlalchemy.dialects.postgresql.JSONB: 'JSONB',
        sqlalchemy.dialects.postgresql.JSON: 'JSON',
        sqlalchemy.dialects.postgresql.MONEY: 'MONEY',
        sqlalchemy.dialects.postgresql.BIT: 'bit',
        sqlalchemy.dialects.postgresql.DATERANGE: 'daterange',
        None: 'void',
        type(None): 'void'
    }[annotation]
Union

class ReturnTypedExpression(Generic[T]):
    def __init__(self, expression: Select|BinaryExpression|None):
        self.expression = expression

    @classmethod
    def get_sql_type(cls):
        return format_type(get_args(cls)[0])
