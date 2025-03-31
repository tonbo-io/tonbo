# Tonbo Python Binding


## `@Record`

Tonbo provides ORM-like macro for ease of use, you can use `@Record` to define schema of column family.
```py
@Record
class User:
   id = Column(DataType.Int64, name="id", primary_key=True)
   age = Column(DataType.Int16, name="age", nullable=True)
   name = Column(DataType.String, name="name", nullable=False)
```

<div class="warning">

This is a bad thing that you should pay attention to.

Warning blocks should be used sparingly in documentation, to avoid "warning
fatigue," where people are trained to ignore them because they usually don't
matter for what they're doing.

</div>


## Configuration

## Example

```python
from tonbo import DbOption, Column, DataType, Record, TonboDB, Bound
from tonbo.fs import from_filesystem_path
import asyncio

@Record
class User:
   id = Column(DataType.Int64, name="id", primary_key=True)
   age = Column(DataType.Int16, name="age", nullable=True)
   name = Column(DataType.String, name="name", nullable=False)

async def main():
    db = TonboDB(DbOption(from_filesystem_path("db_path/user")), User())
    await db.insert(User(id=18, age=175, name="Alice"))
    record = await db.get(18)
    print(record)

    # use transcaction
    txn = await db.transaction()
    result = await txn.get(18)
    scan = await txn.scan(Bound.Included(18), None, limit=10, projection=["id", "name"])

    async for record in scan:
        print(record)

asyncio.run(main())
````
