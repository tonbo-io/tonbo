import asyncio
import tempfile

from tonbo import Column, DataType, TonboDB, RecordBatch, DbOption, Record
from tonbo.fs import from_filesystem_path


@Record
class User:
    id = Column(DataType.Int64, name="id", primary_key=True)
    age = Column(datatype=DataType.UInt8, name="age", nullable=True)
    name = Column(DataType.String, name="name")
    email = Column(DataType.String, name="email", nullable=True)
    data = Column(DataType.Bytes, name="data", nullable=True)


async def main():
    temp_dir = tempfile.TemporaryDirectory()
    db = TonboDB(DbOption(from_filesystem_path(temp_dir.name)), User())
    batch = RecordBatch()
    for i in range(0, 100):
        # you must add record to `RecordBatch` one by one
        batch.append(User(id= i, age=i, name=str(i * 20)))

    await db.insert_batch(batch)

    for i in range(0, 100):
        user = await db.get(i)
        assert user == { "id": i, "age": i, "name": str(i * 20), "email": None, "data": None }


asyncio.run(main())
