from alembic.operations import MigrateOperation, Operations


@Operations.register_operation("grant_column")
class ColGrantOp(MigrateOperation):
    """Enable RLS on a table."""

    def __init__(self, table_name, operation, column, role, schema=None):
        self.role = role
        self.column = column
        self.operation = operation
        self.table_name = table_name
        self.schema = schema

    @classmethod
    def grant_column(cls, operations, table_name, **kw):
        """Issue a "ALTER TABLE ENABLE ROW LEVEL SECURITY" instruction."""

        op = ColRevokeOp(table_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return ColRevokeOp(self.table_name, self.operation, self.column, self.role, schema=self.schema)


@Operations.register_operation("revoke_column")
class ColRevokeOp(MigrateOperation):
    """Disable RLS on table."""

    def __init__(self, table_name, operation, column, role, schema=None):
        self.role = role
        self.column = column
        self.operation = operation
        self.table_name = table_name
        self.schema = schema

    @classmethod
    def revoke_column(cls, operations, sequence_name, **kw):
        """Issue a "ALTER TABLE DISABLE ROW LEVEL SECURITY" instruction."""

        op = ColGrantOp(sequence_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return ColGrantOp(self.table_name, self.operation, self.column, self.role, schema=self.schema)

@Operations.implementation_for(ColGrantOp)
def grant_column(operations, operation: ColGrantOp):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.table_name)
    else:
        name = operation.table_name
    operations.execute("REVOKE %s (%s) on %s;" % operation.operation, operation.column, name, operation.role)

@Operations.implementation_for(ColRevokeOp)
def revoke_column(operations, operation: ColRevokeOp):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.table_name)
    else:
        name = operation.table_name
    operations.execute("REVOKE %s (%s) on %s to %s;" % operation.operation, operation.column, name, operation.role)