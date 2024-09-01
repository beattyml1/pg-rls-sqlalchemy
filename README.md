# pg-rls-sqlalchemy

Work in progress. 

SQLAlchemy and Alembic support for Postgres features like:
- Row Level Security (RLS)
- Policies

Built on top of alembic_utils but provides a more usable interface and a few missing features

## Installation

```shell
pip install pg-rls-sqlalchemy
```

OR 

```shell
poetry add pgalchemy
```

## Policy and Row Level Security

### Using RLS BaseModel
Recommended most projects. This is for projects with majority of tables using RLS which will also be almost all new projects using this library.

```python

from sqlalchemy.orm import declarative_base
from pgalchemy import Policy, PolicyType, PolicyCommands, rls_base

BaseModel = rls_base(declarative_base())

class MyModel(BaseModel):
    ...
Policy("pol_my_models_select_primary", on=MyModel, as_=PolicyType.PERMISSIVE, for_=PolicyCommands.SELECT, using="user_id == auth.uid()")
Policy("pol_my_models_delete_primary", on=MyModel,as_=PolicyType.PERMISSIVE, for_=PolicyCommands.DELETE, using="user_id == auth.uid()")
Policy("pol_my_models_update_primary", on=MyModel,as_=PolicyType.PERMISSIVE, for_=PolicyCommands.UPDATE, using="user_id == auth.uid()", with_check="user_id == auth.uid()")
Policy("pol_my_models_update_primary", on=MyModel,as_=PolicyType.PERMISSIVE, for_=PolicyCommands.INSERT, with_check="user_id == auth.uid()")
```

### Using RLS Decorator
Only intended for projects with majority of tables without RLS enabled. Usually only for existing projects with most tables not protected using RLS that are only using RLS for a niche use case

This is not recommended for other use cases as it makes it easy for a developer to forget to enable RLS and expose a security vulnerability.
```python

from sqlalchemy.orm import declarative_base
from pgalchemy import , rls_base, Policy, PolicyType, PolicyCommands

BaseModel = declarative_base()

@rls()
# Equivalent to:
# @policy(Policy("pol_my_models_primary", as_=PolicyType.PERMISSIVE, for_=PolicyCommands.ALL, using="user_id == auth.uid()", with_check="user_id == auth.uid()"))
class MyModel(BaseModel):
    ...
Policy("pol_my_models_select_primary", on=MyModel, as_=PolicyType.PERMISSIVE, for_=PolicyCommands.SELECT, using="user_id == auth.uid()")
Policy("pol_my_models_delete_primary", on=MyModel,as_=PolicyType.PERMISSIVE, for_=PolicyCommands.DELETE, using="user_id == auth.uid()")
Policy("pol_my_models_update_primary", on=MyModel,as_=PolicyType.PERMISSIVE, for_=PolicyCommands.UPDATE, using="user_id == auth.uid()", with_check="user_id == auth.uid()")
Policy("pol_my_models_update_primary", on=MyModel,as_=PolicyType.PERMISSIVE, for_=PolicyCommands.INSERT, with_check="user_id == auth.uid()")
```

## Functions

### Using Python Functions
```python
from pgalchemy.functions import sql_function
from pgalchemy.types import ReturnTypedExpression
@sql_function(schema='test')
def get_thing(id: int):
    return ReturnTypedExpression[MyModel](
        select(MyModel).where(MyModel.id == id)
    )

```

### Using SQL File with empty python function
```python
from pgalchemy.functions import sql_function
@sql_function(schema='test', path='../functions/get_thing.sql')
def get_thing(id: int) -> MyModel:
    pass

```
### Using SQL File with metadata

```python
import inspect
from pgalchemy.functions import Function

Function(
    schema='test', 
    path='../functions/get_thing.sql', 
    returns=MyModel,
    parameters=[inspect.Parameter(name='id', annotation=int)]
)
```

## Views

### Using Python Functions
```python
from pgalchemy.views import sql_view
from pgalchemy.types import ReturnTypedExpression
@sql_view(schema='test')
def my_view():
    return select(MyModel).where(MyModel.id == id)

```

### Using SQL File

```python
import inspect
from pgalchemy.views import View

View(
    schema='test', 
    path='../views/my_view.sql', 
)
```

