import pytest
import tempfile
from tonbo import DbOption, Column, DataType, Record, TonboDB


@Record
class User:
    id = Column(DataType.Int64, name="id", primary_key=True)
    name = Column(DataType.String, name="name")
    email = Column(DataType.String, name="email", nullable=True)
    age = Column(DataType.Int8, name="age")
    data = Column(DataType.Bytes, name="data")


@pytest.mark.asyncio
async def test_flush():
    temp_dir = tempfile.TemporaryDirectory()
    option = DbOption(temp_dir.name)
    option.immutable_chunk_num = 1
    option.major_threshold_with_sst_size = 3
    option.level_sst_magnification = 1
    option.max_sst_file_size = 1 * 1024
    db = TonboDB(option, User())
    # db = build_db()
    for i in range(0, 1000):
        if i % 100 == 0:
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
