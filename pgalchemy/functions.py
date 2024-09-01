from typing import Callable, Iterable, get_type_hints, get_origin

from alembic_utils.pg_function import PGFunction
import inspect

from sqlalchemy import BinaryExpression, Select

from pgalchemy.expressions import _try_get_sql_from_path, _try_get_sql_from_returned_expression
from pgalchemy.types import format_type, ReturnTypedExpression


def sql_function(path: str | None=None, schema=None):
    def wrapper(func: Callable) -> Callable:
        Function(
            schema=schema,
            path=path,
            sql=_try_get_sql_from_returned_expression(func),
            returns=_return_type_from_annotation(func) or _return_type_from_result(func),
            parameters=params(func)
        )
        return func


class Function:
    def __init__(self, name, path: str | None=None, sql: str | None = None, schema=None, parameters: Iterable[inspect.Parameter], returns: type):
        sig_params = ', '.join([_format_arg(arg) for arg in parameters])
        sql = sql or _try_get_sql_from_path(path)
        name = name or path.split('/')[-1].split('.')[0]
        PGFunction(
            schema=schema,
            signature=f"{name}({sig_params})",
            definition=f"""
                RETURNS {returns_for_type(returns)} AS $$
                BEGIN
                    {sql}
                END;
                $$ language 'plpgsql'
            """
        )



def _format_arg(arg):
    main = f'{arg.name} {format_type(arg.annotation)}'
    if arg.default:
        return f'{main} default {arg.default}'
    else:
        return main


def params(func):
    sig = inspect.signature(func)
    return sig.parameters.values()


def returns_for_type(annotation):
    if annotation and get_origin(annotation) is ReturnTypedExpression:
        return annotation.get_sql_type()
    elif annotation:
        return format_type(annotation)
    else:
        return None


def _return_type_from_annotation(func):
    return get_type_hints(func)['return']


def _return_type_from_result(func):
    result = func()
    if result and (isinstance(result, Select) or isinstance(result, BinaryExpression)):
        raise ValueError("Please wrap your returned expression with a ReturnTypedExpression() so that we have type information to generate your function")
    if result and isinstance(result, ReturnTypedExpression) and result.expression:
        return type(result)
    return None