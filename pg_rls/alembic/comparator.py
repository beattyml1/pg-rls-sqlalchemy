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

    policy_results = get_policies_db(autogen_context, schemaname, tablename)
    policies_to_compare = setup_policy_comparison_list(policy_results, rls)
    for policy in policies_to_compare:
        compare_policy(autogen_context, modify_ops, schemaname, tablename, *policy)


def compare_policy(autogen_context, modify_ops, schemaname, tablename, policy_db, policy_metadata):
    pass


def setup_policy_comparison_list(policy_results, rls):
    policy_names_db = {p.name['policname'] for p in policy_results}
    policy_names_meta = {p.name for p in rls.policies}
    policy_names = {*policy_names_meta, *policy_names_db}
    policies_to_compare = {p: [None, None] for p in policy_names}
    for policy in rls.policies:
        policies_to_compare[policy.name][1] = policy
    for policy in policy_results:
        policies_to_compare[policy.name][0] = Policy.from_pg_policy(policy)

    return policies_to_compare


def get_policies_db(autogen_context, schemaname, tablename):
    policy_query = "select * from pg_policies where tablename = %s and schemaname = %s;"
    return autogen_context.connection.execute(policy_query, (schemaname, tablename))


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
