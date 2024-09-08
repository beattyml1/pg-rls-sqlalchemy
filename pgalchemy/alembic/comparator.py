from alembic.autogenerate import comparators
from sqlalchemy import Table, Column

from .column_privilege import ColumnPrivilege
from .operations import EnableRlsOp, DisableRlsOp
from .operations.cls import ColGrantOp
from ..cls import column_is_managed
from ..rls import RlsData


@comparators.dispatch_for("table")
def compare_rls(autogen_context, modify_ops, schemaname, tablename, conn_table, metadata_table: Table):
    rls: RlsData = metadata_table.info.get('rls')

    db_table, rls_enabled_db = get_table_rls_data(autogen_context, schemaname, tablename)
    if db_table is None:
        return

    compare_rls_enabled(modify_ops, rls, rls_enabled_db, schemaname, tablename)


def compare_rls_enabled(modify_ops, rls, rls_enabled_db, schemaname, tablename):
    if rls.active is True and rls_enabled_db is False:
        modify_ops.ops.append(
            EnableRlsOp(tablename, schema=schemaname)
        )
    if rls.active is False and rls_enabled_db is True:
        modify_ops.ops.append(
            DisableRlsOp(tablename, schema=schemaname)
        )


def get_table_rls_data(autogen_context, schemaname, tablename):
    results = autogen_context.connection.execute(
        'select relname, relrowsecurity, relforcerowsecurity from pg_class where  relnamespace = %s and relname = %s;',
        (schemaname, tablename)
    )
    db_table = results.fetchone()
    rls_enabled_db = db_table['relrowsecurity'] if db_table is not None else None
    return db_table, rls_enabled_db



@comparators.dispatch_for("column")
def compare_cls(autogen_context, modify_ops, schemaname, tablename, colname, conn_table, metadata_column: Column):
    GrantRevoke, Op, Role = str, str, str
    code_privileges: list[(GrantRevoke, Op, Role)] = metadata_column.info.get('cls')
    db_privileges = ColumnPrivilege.get_for_column(autogen_context.connection, schemaname, tablename, colname)

    compare_cls(modify_ops, code_privileges, db_privileges, schemaname, tablename, colname, metadata_column)


def compare_cls_enabled(modify_ops, code_privileges: list[(str, str, str)], db_privileges: list[ColumnPrivilege], schemaname, tablename, colname, metadata_column):
    for (grant_revoke, op, role) in code_privileges:
        exists = any(x.is_same(op, role) for x in db_privileges)
        managed = column_is_managed(role, column=metadata_column)
        if managed:
            if grant_revoke == 'GRANT' and not exists:
                modify_ops.append(ColGrantOp(table_name=tablename, operation=op, column=colname, schema=schemaname))
            elif grant_revoke == 'REVOKE' and exists:
                modify_ops.append(ColGrantOp(table_name=tablename, operation=op, column=colname, schema=schemaname))
