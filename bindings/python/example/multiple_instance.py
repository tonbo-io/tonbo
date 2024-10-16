import asyncio
import tempfile

from tonbo import Column, DataType, TonboDB, RecordBatch, DbOption, Record
from tonbo.fs import from_filesystem_path


@Record
class User:
    id = Column(DataType.UInt64, name="id", primary_key=True)
    age = Column(datatype=DataType.UInt8, name="age", nullable=True)
    name = Column(DataType.String, name="name")
    email = Column(DataType.String, name="email")


@Record
class Pet:
    id = Column(DataType.UInt64, name="id", primary_key=True)
    type = Column(DataType.String, name="type")
    name = Column(DataType.String, name="name")
    master = Column(datatype=DataType.UInt64, name="master", nullable=True)


@Record
class FoundRecord:
    user_id = Column(DataType.UInt64, name="user_id", primary_key=True)
    pet_id = Column(DataType.UInt64, name="pet_id")
    email = Column(DataType.String, name="email")
    status = Column(DataType.Boolean, name="status")


async def main():
    user_temp_dir = tempfile.TemporaryDirectory()
    pet_temp_dir = tempfile.TemporaryDirectory()
    found_temp_dir = tempfile.TemporaryDirectory()
    user_db = TonboDB(DbOption(from_filesystem_path(user_temp_dir.name)), User())
    course_db = TonboDB(DbOption(from_filesystem_path(pet_temp_dir.name)), Pet())
    found_db = TonboDB(DbOption(from_filesystem_path(found_temp_dir.name)), FoundRecord())

    user_txn = await user_db.transaction()
    for i in range(0, 100):
        user_txn.insert(User(id=i, age=i, name=str(i * 20), email=str(i * 100)))
    user_txn.commit()

    pet_txn = await course_db.transaction()
    pet_txn.insert(Pet(id=1, type="dog", name="Hachi", master=10))
    pet_txn.insert(Pet(id=2, type="cat", name="Kitty", master=21))
    pet_txn.insert(Pet(id=3, type="dragonfly", name="Tonbo", master=11))

    await pet_txn.commit()

    user_txn = await user_db.transaction()
    found_txn = await found_db.transaction()
    user_scan = await user_txn.scan(lower=None, high=None, projection=["id", "name", "email"])

    async for user in user_scan:
        pet_txn = await course_db.transaction()
        pet_scan = await pet_txn.scan(lower=None, high=None, projection=["master", "name"])
        async for pet in pet_scan:
            if user["id"] == pet["master"]:
                found_txn.insert(FoundRecord(user_id=user["id"], pet_id=pet["id"], email=user["email"], status=False))

    await found_txn.commit()

    found_txn = await found_db.transaction()
    found_scan = await found_txn.scan(lower=None, high=None)
    async for found in found_scan:
        print(found)


asyncio.run(main())
