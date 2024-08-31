from sqlalchemy import Table

from pg_rls import Policy


class PolicySql:
    def __init__(self, policy: Policy, table_name: str, schema: str):
        self.policy = policy
        self.table_name = table_name
        self.schema = schema
    
    def qualify(self, name):
        return f"{self.schema}.{name}"
    
    @property
    def tab(self):
        return self.qualify(self.table_name)
    
    @property
    def pol(self):
        return self.qualify(self.policy.name)

    # Full
    def rename_sql(self, old_name):
        return f"alter policy {old_name} on {self.tab} rename to {self.pol}"

    def drop_sql(self):
        return f"drop policy {self.pol} on {self.tab}"

    def create_sql(self):
        return self._create_fragment() + self._as_fragment() + self._from_fragment() + self._using_fragment() + self._with_check_fragment()

    def alter_sql(self):
        return self._alter_fragment() + self._using_fragment() + self._with_check_fragment()

    # Fragments
    def _create_fragment(self):
        return f"create policy {self.pol} on {self.tab}\n"

    def _alter_fragment(self):
        return f"alter policy {self.pol} on {self.tab}\n"

    def _as_fragment(self):
        return f"as {self.policy.as_}\n" if self.policy.as_ else ""

    def _from_fragment(self):
        return f"for {self.policy.for_}\n" if self.policy.for_ else ""

    def _with_check_fragment(self):
        return f"with check {self.policy.with_check}\n" if self.policy.with_check else ""

    def _using_fragment(self):
        return f"using {self.policy.using}\n" if self.policy.using else ""
