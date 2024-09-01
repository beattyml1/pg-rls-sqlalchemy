import os
import pathlib
from typing import get_type_hints

from sqlalchemy import Select, BinaryExpression

from pgalchemy.types import ReturnTypedExpression, format_type


def _try_get_sql_from_path(path):
    if path:
        if path.endswith('.sql') or path.endswith('.psql'):
            try:
                pwd = pathlib.Path(__file__).parent.resolve()
                full_path = os.path.normpath(os.path.join(pwd, path))
            except Exception as e:
                raise ValueError(f"Cannot construct a fully qualified path from {path} in directory {pwd}")

            try:
                with open(full_path, 'r') as f:
                    return f.read()
            except FileNotFoundError as e:
                raise ValueError(f"File {full_path} does not exist")
        else:
            raise ValueError("Path parameter must be a path to a .sql or .psql file")
    return None


def _try_get_sql_from_returned_expression(func):
    result = func()

    if result and (isinstance(result, Select) or isinstance(result, BinaryExpression)):
        return str(result)
    if result and isinstance(result, ReturnTypedExpression):
        return str(result.expression)
    return None


def _try_get_return_from_annotation(func):
    annotation = get_type_hints(func)['return']
    if annotation and annotation is ReturnTypedExpression:
        return annotation.get_sql_type()
    elif annotation:
        return format_type(annotation)
    else:
        return None


def _try_get_type_from_returned_expression(func):
    result = func()

    if result and (isinstance(result, Select) or isinstance(result, BinaryExpression)):
        raise ValueError("Please wrap your returned expression with a ReturnTypedExpression() so that we have type information to generate your function")
    if result and isinstance(result, ReturnTypedExpression) and result.expression:
        return type(result).get_sql_type()
    return None
