# pg-rls-sqlalchemy

Work in progress. Support for Postgres Row Level Security (RLS) include Postgres Policy in SQL Alchemy and Alembic.

## Installation

```shell
pip install pg-rls-sqlalchemy
```

OR 

```shell
poetry add pg-rls-sqlalchemy
```

## Usage

### Using RLS BaseModel
Recommended most projects. This is for projects with majority of tables using RLS which will also be almost all new projects using this library.

```python

from sqlalchemy.orm import declarative_base
from pg_rls import rls_base, policy, Policy, PolicyType, PolicyCommands

BaseModel = rls_base(declarative_base())


@policy(Policy("pol_my_models_select_primary", as_=PolicyType.PERMISSIVE, for_=PolicyCommands.SELECT, using="user_id == auth.uid()"))
@policy(Policy("pol_my_models_delete_primary", as_=PolicyType.PERMISSIVE, for_=PolicyCommands.DELETE, using="user_id == auth.uid()"))
@policy(Policy("pol_my_models_update_primary", as_=PolicyType.PERMISSIVE, for_=PolicyCommands.UPDATE, using="user_id == auth.uid()", with_check="user_id == auth.uid()"))
@policy(Policy("pol_my_models_update_primary", as_=PolicyType.PERMISSIVE, for_=PolicyCommands.INSERT, with_check="user_id == auth.uid()"))
# Equivalent to:
# @policy(Policy("pol_my_models_primary", as_=PolicyType.PERMISSIVE, for_=PolicyCommands.ALL, using="user_id == auth.uid()", with_check="user_id == auth.uid()"))
class MyModel(BaseModel):
    ...
```

### Using RLS Decorator
Only intended for projects with majority of tables without RLS enabled. Usually only for existing projects with most tables not protected using RLS that are only using RLS for a niche use case

This is not recommended for other use cases as it makes it easy for a developer to forget to enable RLS and expose a security vulnerability.
```python

from sqlalchemy.orm import declarative_base
from pg_rls import rls, policy, Policy, PolicyType, PolicyCommands

BaseModel = declarative_base()

@rls()
@policy(Policy("pol_my_models_select_primary", as_=PolicyType.PERMISSIVE, for_=PolicyCommands.SELECT, using="user_id == auth.uid()"))
@policy(Policy("pol_my_models_delete_primary", as_=PolicyType.PERMISSIVE, for_=PolicyCommands.DELETE, using="user_id == auth.uid()"))
@policy(Policy("pol_my_models_update_primary", as_=PolicyType.PERMISSIVE, for_=PolicyCommands.UPDATE, using="user_id == auth.uid()", with_check="user_id == auth.uid()"))
@policy(Policy("pol_my_models_update_primary", as_=PolicyType.PERMISSIVE, for_=PolicyCommands.INSERT, with_check="user_id == auth.uid()"))
# Equivalent to:
# @policy(Policy("pol_my_models_primary", as_=PolicyType.PERMISSIVE, for_=PolicyCommands.ALL, using="user_id == auth.uid()", with_check="user_id == auth.uid()"))
class MyModel(BaseModel):
    ...
```

