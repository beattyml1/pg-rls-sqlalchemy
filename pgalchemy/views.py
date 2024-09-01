from typing import Callable

from alembic_utils.pg_function import PGFunction
import inspect

from alembic_utils.pg_materialized_view import PGMaterializedView
from alembic_utils.pg_view import PGView

from pgalchemy.expressions import _try_get_sql_from_path, _try_get_sql_from_returned_expression


def sql_view(schema=None, materialized: bool = False):
    def wrapper(func: Callable) -> Callable:
        sql = _try_get_sql_from_returned_expression(func)
        View(func.__name__, sql, materialized=materialized, schema=schema, sql=sql)
    return wrapper


class View:
    def __init__(self, name: str, schema, materialized: bool = False, path: str| None = None, sql: str | None = None):
        sql = sql or _try_get_sql_from_path(path)
        name = name or path.split('/')[-1].split('.')[0]
        if materialized:
            PGMaterializedView(schema=schema, signature=f"{name}", definition=sql)
        else:
            PGView(schema=schema, signature=f"{name}", definition=sql)


