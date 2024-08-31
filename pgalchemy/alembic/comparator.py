from alembic.autogenerate import comparators
from alembic.operations import Operations, MigrateOperation
from sqlalchemy import Table

from pg_rls.sqlalchemy import RlsData
from .operations import EnableRlsOp, DisableRlsOp
from .. import Policy


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
