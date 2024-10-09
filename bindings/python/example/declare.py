from tonbo import DbOption, Column, DataType, Record, TonboDB, Bound
import asyncio
import tempfile


@Record
class User:
    age = Column(DataType.Int8, name="age", primary_key=True)
    height = Column(DataType.Int16, name="height", nullable=True)
    weight = Column(DataType.Int32, name="weight", nullable=False)


async def main():
    temp_dir = tempfile.TemporaryDirectory()

    db = TonboDB(DbOption(temp_dir.name), User())
    await db.insert(User(age=18, height=175, weight=60))

    record = await db.get(18)
    assert record == {"age": 18, "height": 175, "weight": 60}

    txn = await db.transaction()
    result = await txn.get(18)
    assert result == {"age": 18, "height": 175, "weight": 60}

    txn.insert(User(age=19, height=195, weight=75))
    result = await txn.get(19)
    assert result == {"age": 19, "height": 195, "weight": 75}

    await txn.commit()
    txn = await db.transaction()
    scan = await txn.scan(
        Bound.Excluded(18), None, limit=100, projection=["age", "weight"]
    )
    async for record in scan:
        assert record["height"] is None
        print(record)
    await txn.commit()


asyncio.run(main())
