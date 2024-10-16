# Tonbo Python Binding

This package intends to build a native python binding for [Tonbo](https://github.com/tonbo-io/tonbo).

Tonbo's Python bindings can be used to build data-intensive applications, including other types of databases.



## Example

```py
from tonbo import DbOption, Column, DataType, Record, TonboDB, Bound
import asyncio

@Record
class User:
    age = Column(DataType.INT8, name="age", primary_key=True)
    height = Column(DataType.INT16, name="height", nullable=True)
    weight = Column(DataType.INT8, name="weight", nullable=False)

async def main():

    db = TonboDB(DbOption("./db_path/users"), User())

    await db.insert(User(age=18, height=175, weight=60))
    record = await db.get(18)
    assert record == {"age": 18, "height": 175, "weight": 60}

    txn = await db.transaction()
    txn.insert(User(age=19, height=195, weight=75))
    result = await txn.get(19)
    assert result == {"age": 19, "height": 195, "weight": 75}

    # commit transaction
    await txn.commit()

    txn = await db.transaction()
    # range scan, supports pushing down and limit
    scan = await txn.scan(
        Bound.Excluded(18), None, limit=100, projection=["age", "weight"]
    )
    async for record in scan:
        print(record)

asyncio.run(main())
```

See [examples](example/README.md) for more information.

## Development
This assumes that you have Rust and cargo installed. We use the [pyo3](https://github.com/PyO3/pyo3) to generate a native Python module and use [maturin](https://github.com/PyO3/maturin) to build Rust-based Python packages.

First, follow the commands below to build a new Python virtualenv, and install maturin into the virtualenv using Python's package manager, pip:
```bash
# setup virtualenv
python -m venv .env
# activate venv
source .env/bin/activate

# install maturin
pip install maturin
# build bindings
maturin develop
```
Whenever Rust code changes run:
```bash
maturin develop
```

Run tests:
```bash
maturin develop -E test
python -m pytest
```
