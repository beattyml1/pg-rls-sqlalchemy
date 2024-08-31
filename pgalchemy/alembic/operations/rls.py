from alembic.operations import MigrateOperation, Operations


@Operations.register_operation("enable_rls")
class EnableRlsOp(MigrateOperation):
    """Enable RLS on a table."""

    def __init__(self, table_name, schema=None):
        self.table_name = table_name
        self.schema = schema

    @classmethod
    def enable_rls(cls, operations, table_name, **kw):
        """Issue a "ALTER TABLE ENABLE ROW LEVEL SECURITY" instruction."""

        op = EnableRlsOp(table_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return DisableRlsOp(self.table_name, schema=self.schema)


@Operations.register_operation("disable_rls")
class DisableRlsOp(MigrateOperation):
    """Disable RLS on table."""

    def __init__(self, table_name, schema=None):
        self.table_name = table_name
        self.schema = schema

    @classmethod
    def disable_rls(cls, operations, sequence_name, **kw):
        """Issue a "ALTER TABLE DISABLE ROW LEVEL SECURITY" instruction."""

        op = DisableRlsOp(sequence_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return EnableRlsOp(self.sequence_name, schema=self.schema)

@Operations.implementation_for(EnableRlsOp)
def enable_rls(operations, operation: EnableRlsOp):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.table_name)
    else:
        name = operation.table_name
    operations.execute("ALTER TABLE %s ENABLE ROW LEVEL SECURITY;" % name)

@Operations.implementation_for(DisableRlsOp)
def disable_rls(operations, operation: DisableRlsOp):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.table_name)
    else:
        name = operation.table_name
    operations.execute("ALTER TABLE %s DISABLE ROW LEVEL SECURITY;" % name)