from alembic.operations import MigrateOperation, Operations


@Operations.register_operation("create_policy")
class CreatePolicyOp(MigrateOperation):
    """Create a SEQUENCE."""

    def __init__(self, policy_name, schema=None):
        self.policy_name = policy_name
        self.schema = schema

    @classmethod
    def create_policy(cls, operations, policy_name, **kw):
        """Issue a "CREATE SEQUENCE" instruction."""

        op = CreatePolicyOp(policy_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return DropPolicyOp(self.policy_name, schema=self.schema)


@Operations.register_operation("drop_policy")
class DropPolicyOp(MigrateOperation):
    """Drop a SEQUENCE."""

    def __init__(self, policy_name, schema=None):
        self.policy_name = policy_name
        self.schema = schema

    @classmethod
    def drop_policy(cls, operations, policy_name, **kw):
        """Issue a "DROP SEQUENCE" instruction."""

        op = DropPolicyOp(policy_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return CreatePolicyOp(self.policy_name, schema=self.schema)


@Operations.register_operation("alter_policy")
class AlterPolicyOp(MigrateOperation):
    """Drop a SEQUENCE."""

    def __init__(self, policy_name, schema=None):
        self.policy_name = policy_name
        self.schema = schema

    @classmethod
    def alter_policy(cls, operations, policy_name, **kw):
        """Issue a "DROP SEQUENCE" instruction."""

        op = AlterPolicyOp(policy_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return AlterPolicyOp(self.policy_name, schema=self.schema)


@Operations.register_operation("rename_policy")
class RenamePolicyOp(MigrateOperation):
    """Drop a SEQUENCE."""

    def __init__(self, policy_name, schema=None):
        self.policy_name = policy_name
        self.schema = schema

    @classmethod
    def rename_policy(cls, operations, policy_name, **kw):
        """Issue a "DROP SEQUENCE" instruction."""

        op = RenamePolicyOp(policy_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return RenamePolicyOp(self.policy_name, schema=self.schema)


@Operations.implementation_for(CreatePolicyOp)
def create_policy(operations, operation):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.sequence_name)
    else:
        name = operation.sequence_name
    operations.execute("CREATE SEQUENCE %s" % name)


@Operations.implementation_for(DropPolicyOp)
def drop_policy(operations, operation):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.sequence_name)
    else:
        name = operation.sequence_name
    operations.execute("DROP SEQUENCE %s" % name)


@Operations.implementation_for(AlterPolicyOp)
def alter_policy(operations, operation):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.sequence_name)
    else:
        name = operation.sequence_name
    operations.execute("CREATE SEQUENCE %s" % name)


@Operations.implementation_for(RenamePolicyOp)
def rename_policy(operations, operation):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.sequence_name)
    else:
        name = operation.sequence_name
    operations.execute("CREATE SEQUENCE %s" % name)