from enum import Enum

from sqlalchemy import Row, Connection

sql_identifier = str


class YesOrNo(Enum):
    YES = 'YES'
    NO = 'NO'


class ColumnPrivilegeType(Enum):
    SELECT = 'SELECT' 
    INSERT = 'INSERT' 
    UPDATE = 'UPDATE'
    REFERENCES = 'REFERENCES'


class ColumnPrivilege:
    def __init__(
            self,
            grantor: sql_identifier, # Name of the role that granted the privilege
            grantee: sql_identifier,  # Name of the role that the privilege was granted to
            table_catalog: sql_identifier,  # Name of the database that contains the table that contains the column (always the current database)
            table_schema: sql_identifier,  # Name of the schema that contains the table that contains the column
            table_name: sql_identifier,  # Name of the table that contains the column
            column_name: sql_identifier,  # Name of the column
            privilege_type: ColumnPrivilegeType,  # Type of the privilege: SELECT, INSERT, UPDATE, or REFERENCES
            is_grantable: YesOrNo  # YES if the privilege is grantable, NO if not
    ):
        self.is_grantable = is_grantable
        self.privilege_type = privilege_type
        self.column_name = column_name
        self.table_name = table_name
        self.table_schema = table_schema
        self.table_catalog = table_catalog
        self.grantee = grantee
        self.grantor = grantor

    @staticmethod
    def from_row(row: Row):
        return ColumnPrivilege(**row._asdict())

    def get_for_table(self, connection: Connection, schema, table_name):
        results = connection.execute(
            "select * from information_schema.column_privileges where table_name='{}' and table_schema={}"
            .format(table_name, schema)
        )
        return [ColumnPrivilege.from_row(r) for r in results]

    def get_for_column(self, connection: Connection, schema, table_name, column_name):
        results = connection.execute(
            "select * from information_schema.column_privileges where table_name='{}' and table_schema={} and column_name={}"
            .format(table_name, schema, column_name)
        )
        return [ColumnPrivilege.from_row(r) for r in results]

    def get_all(self, connection: Connection):
        results = connection.execute("select * from information_schema.column_privileges")
        return [ColumnPrivilege.from_row(r) for r in results]

    def is_same(self, privilege_type, grantee):
        return self.privilege_type == privilege_type and self.grantee == grantee
