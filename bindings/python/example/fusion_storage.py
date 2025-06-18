import asyncio
import os

from tonbo import DbOption, Column, DataType, Record, TonboDB
from tonbo.fs import from_filesystem_path, FsOptions


@Record
class User:
    id = Column(DataType.Int64, name="id", primary_key=True)
    name = Column(DataType.String, name="name")
    email = Column(DataType.String, name="email", nullable=True)
    age = Column(DataType.UInt8, name="age")
    data = Column(DataType.Bytes, name="data")


async def main():
    if not os.path.exists("db_path/user/l0"):
        os.makedirs("db_path/user/l0")
    if not os.path.exists("db_path/user/l1"):
        os.makedirs("db_path/user/l1")

    option = DbOption(from_filesystem_path("db_path/user"))
    option.level_path(0, from_filesystem_path("db_path/user/l0"), FsOptions.Local(), False)
    option.level_path(1, from_filesystem_path("db_path/user/l1"), FsOptions.Local(), False)

    option.immutable_chunk_num = 1
    option.major_threshold_with_sst_size = 3
    option.level_sst_magnification = 1
    option.max_sst_file_size = 1 * 1024

    db = TonboDB(option, User())
    for i in range(0, 1000):
        if i % 50 == 0:
            await db.flush()
        await db.insert(
            User(
                id=i,
                age=i % 128,
                name=str(i * 10),
                email=str(i * 20),
                data=b"Hello Tonbo!",
            )
        )

    for i in range(0, 1000):
        user = await db.get(i)
        assert user == {
            "id": i,
            "name": str(i * 10),
            "email": str(i * 20),
            "age": i % 128,
            "data": b"Hello Tonbo!",
        }


asyncio.run(main())
