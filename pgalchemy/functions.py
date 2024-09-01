from typing import Callable

from alembic_utils.pg_function import PGFunction
import inspect

from pgalchemy.expressions import _try_get_sql_from_path, _try_get_sql_from_returned_expression, \
    _try_get_return_from_annotation, _try_get_type_from_returned_expression
from pgalchemy.types import format_type


def sql_function(path: str | None=None, schema=None):
    def wrapper(func: Callable) -> Callable:
        sig_params = _get_signature_params_sql(func)
        sql = _try_get_sql_from_returned_expression(func) or _try_get_sql_from_path(path)
        returns = _try_get_return_from_annotation(func) or _try_get_type_from_returned_expression(func)
        PGFunction(
            schema=schema,
            signature=f"{func.__name__}({sig_params})",
            definition=f"""
                RETURNS {returns} AS $$
                BEGIN
                    {sql}
                END;
                $$ language 'plpgsql'
            """
        )
        return func

def _format_arg(arg):
    main = f'{arg.name} {format_type(arg.annotation)}'
    if arg.default:
        return f'{main} default {arg.default}'
    else:
        return main


def _get_signature_params_sql(func):
    sig = inspect.signature(func)
    args = sig.parameters.values()
    return ', '.join([_format_arg(arg) for arg in args])