from tonbo import DbOption, Column, DataType, Record, TonboDB, Bound
import asyncio
import tempfile

@Record
class User:
    id = Column(DataType.Int64, name="id", primary_key=True)
    age = Column(DataType.Int16, name="age", nullable=True)
    name = Column(DataType.String, name="name", nullable=False)
    email = Column(DataType.String, name="email", nullable=True)
    data = Column(DataType.Bytes, name="data", nullable=True)
    grade = Column(DataType.Float, name="grade", nullable=True)


async def main():
    temp_dir = tempfile.TemporaryDirectory()

    db = TonboDB(DbOption(temp_dir.name), User())
    await db.insert(User(id=18, age=175, name="Alice", grade = 1.23))

    record = await db.get(18)
    assert record == {
        "id": 18,
        "age": 175,
        "name": "Alice",
        "email": None,
        "data": None,
        "grade": 1.23,
    }

    txn = await db.transaction()
    result = await txn.get(18)
    assert result == {
        "id": 18,
        "age": 175,
        "name": "Alice",
        "email": None,
        "data": None,
        "grade": 1.23,
    }

    txn.insert(
        User(
            id=19,
            age=195,
            name="Bob",
            data=b"Hello Tonbo!",
            email="contact@tonbo.io",
            grade = 2.23,
        )
    )
    result = await txn.get(19)
    assert result == {
        "id": 19,
        "age": 195,
        "name": "Bob",
        "email": "contact@tonbo.io",
        "data": b"Hello Tonbo!",
        "grade": 2.23,
    }

    await txn.commit()
    txn = await db.transaction()
    scan = await txn.scan(
        Bound.Excluded(18),
        None,
        limit=100,
        projection=["id", "email", "data", "grade"],
    )
    async for record in scan:
        assert record["age"] is None
        print(record)
    await txn.commit()


asyncio.run(main())
