from alembic.operations import MigrateOperation, Operations
from sqlalchemy import Table

from pg_rls import Policy
from pg_rls.alembic.operations.policy_sql import PolicySql


@Operations.register_operation("create_policy")
class CreatePolicyOp(MigrateOperation):
    """Create a POLICY."""

    def __init__(self, policy_name, for_, as_, using, with_check, tablename, schema=None) -> None:
        self.with_check = with_check
        self.using = using
        self.as_ = as_
        self.for_ = for_
        self.policy_name = policy_name
        self.tablename = tablename
        self.schema = schema

    @classmethod
    def create_policy(cls, operations, policy_name, **kw):
        """Issue a "CREATE SEQUENCE" instruction."""

        op = CreatePolicyOp(policy_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return DropPolicyOp(
            self.policy_name,
            self.for_,
            self.as_,
            self.using,
            self.with_check,
            tablename=self.tablename,
            schema=self.schema
        )


@Operations.register_operation("drop_policy")
class DropPolicyOp(MigrateOperation):
    """DROP a POLICY."""

    def __init__(self, policy_name, for_, as_, using, with_check, tablename, schema=None) -> None:
        self.with_check = with_check
        self.using = using
        self.as_ = as_
        self.for_ = for_
        self.policy_name = policy_name
        self.tablename = tablename
        self.schema = schema


    @classmethod
    def drop_policy(cls, operations, policy_name, **kw):
        """Issue a "DROP SEQUENCE" instruction."""

        op = DropPolicyOp(policy_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return CreatePolicyOp(
            self.policy_name,
            self.for_,
            self.as_,
            self.using,
            self.with_check,
            tablename=self.tablename,
            schema=self.schema
        )


@Operations.register_operation("alter_policy")
class AlterPolicyOp(MigrateOperation):
    """Alter a POLICY."""

    def __init__(self, policy_name, for_, as_, using, with_check,  tablename, schema=None) -> None:
        self.with_check = with_check
        self.using = using
        self.as_ = as_
        self.for_ = for_
        self.policy_name = policy_name
        self.tablename = tablename
        self.schema = schema


    @classmethod
    def alter_policy(cls, operations, policy_name, **kw):
        """Issue a "DROP SEQUENCE" instruction."""

        op = AlterPolicyOp(policy_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return AlterPolicyOp(
            self.policy_name,
            self.for_,
            self.as_,
            self.using,
            self.with_check,
            tablename=self.tablename,
            schema=self.schema
        )


@Operations.register_operation("rename_policy")
class RenamePolicyOp(MigrateOperation):
    """Rename a POLICY."""

    def __init__(self, policy_name, for_, as_, using, with_check,  tablename, old_name, schema=None) -> None:
        self.with_check = with_check
        self.using = using
        self.as_ = as_
        self.for_ = for_
        self.policy_name = policy_name
        self.tablename = tablename
        self.schema = schema
        self.old_name = old_name

    @classmethod
    def rename_policy(cls, operations, policy_name, **kw):
        """Issue a "DROP SEQUENCE" instruction."""

        op = RenamePolicyOp(policy_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return RenamePolicyOp(
            self.policy_name,
            self.for_,
            self.as_,
            self.using,
            self.with_check,
            tablename=self.tablename,
            schema=self.schema,
            old_name=self.old_name
        )

def policy_sql_for_operation(operation):
    return PolicySql(
        Policy(
            operation.policy_name,
            operation.as_,
            operation.for_,
            operation.using,
            operation.with_check,
        ),
        table_name=operation.tablename,
        schema=operation.schema,
    )

@Operations.implementation_for(CreatePolicyOp)
def create_policy(operations, operation: CreatePolicyOp):
    operations.execute(policy_sql_for_operation(operation).create_sql())


@Operations.implementation_for(DropPolicyOp)
def drop_policy(operations, operation):
    operations.execute(policy_sql_for_operation(operation).drop_sql())


@Operations.implementation_for(AlterPolicyOp)
def alter_policy(operations, operation):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.sequence_name)
    else:
        name = operation.sequence_name
    operations.execute(policy_sql_for_operation(operation).alter_sql())


@Operations.implementation_for(RenamePolicyOp)
def rename_policy(operations, operation):
    operations.execute(policy_sql_for_operation(operation).rename_sql(operation.old_name))