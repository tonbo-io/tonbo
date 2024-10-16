from tonbo import DbOption, Column, DataType, Record, TonboDB, Bound
import asyncio
import tempfile


@Record
class User:
    id = Column(DataType.Int64, name="id", primary_key=True)
    height = Column(DataType.Int16, name="height", nullable=True)
    name = Column(DataType.String, name="name", nullable=False)
    email = Column(DataType.String, name="email", nullable=True)
    data = Column(DataType.Bytes, name="data", nullable=True)


async def main():
    temp_dir = tempfile.TemporaryDirectory()

    db = TonboDB(DbOption(temp_dir.name), User())
    # create a new transaction
    txn = await db.transaction()

    # insert with class
    txn.insert(
        User(
            id=19,
            height=195,
            name="Bob",
            data=b"Hello Tonbo!",
            email="contact@tonbo.io",
        )
    )
    result = await txn.get(19)
    assert result == {
        "id": 19,
        "height": 195,
        "name": "Bob",
        "email": "contact@tonbo.io",
        "data": b"Hello Tonbo!",
    }

    # commit a transaction
    await txn.commit()

    txn = await db.transaction()
    # support push down limit, filter and projection
    scan = await txn.scan(
        Bound.Excluded(18),
        None,
        limit=100,
        projection=["id", "email", "data"],
    )
    async for record in scan:
        assert record["height"] is None
        print(record)
    await txn.commit()


asyncio.run(main())
