import pytest
import tempfile
from tonbo import DbOption, Column, DataType, Record, RecordBatch, TonboDB


@Record
class User:
    age = Column(DataType.Int8, name="age", primary_key=True)
    height = Column(DataType.Int16, name="height", nullable=True)
    weight = Column(DataType.Int32, name="weight", nullable=False)


def build_db():
    temp_dir = tempfile.TemporaryDirectory()
    return TonboDB(DbOption(temp_dir.name), User())


@pytest.mark.asyncio
async def test_db_write():
    db = build_db()
    for i in range(0, 100):
        await db.insert(User(age=i, height=i * 10, weight=i * 20))

@pytest.mark.asyncio
async def test_db_write_none_value():
    db = build_db()
    await db.insert(User(age=1, height=100, weight=20))
    await db.insert(User(age=2, weight=30))
    user1 = await db.get(1)
    user2 = await db.get(2)
    assert user1 == {"age": 1, "height": 100, "weight": 20}
    assert user2 == {"age": 2, "height": None, "weight": 30}

@pytest.mark.asyncio
async def test_db_read():
    db = build_db()
    for i in range(0, 100):
        await db.insert(User(age=i, height=i * 10, weight=i * 20))

    for i in range(0, 100):
        user = await db.get(i)
        assert user == {"age": i, "height": i * 10, "weight": i * 20}


@pytest.mark.asyncio
async def test_db_write_batch():
    db = build_db()
    batch = RecordBatch()
    for i in range(0, 100):
        batch.append(User(age=i, height=i * 10, weight=i * 20))
    await db.insert_batch(batch)

    for i in range(0, 100):
        user = await db.get(i)
        assert user == {"age": i, "height": i * 10, "weight": i * 20}


@pytest.mark.asyncio
async def test_db_remove():
    db = build_db()
    for i in range(0, 100):
        if i % 2 == 0:
            await db.insert(User(age=i, height=i * 10, weight=i * 20))
        else:
            await db.remove(i)

    for i in range(0, 100):
        user = await db.get(i)
        if i % 2 == 0:
            assert user == {"age": i, "height": i * 10, "weight": i * 20}
        else:
            assert user is None


@pytest.mark.asyncio
async def test_db_recover():
    temp_dir = tempfile.TemporaryDirectory()
    db = TonboDB(DbOption(temp_dir.name), User())
    for i in range(0, 100):
        await db.insert(User(age=i, height=i * 10, weight=i * 20))

    for i in range(0, 100):
        user = await db.get(i)
        assert user == {"age": i, "height": i * 10, "weight": i * 20}

    await db.flush_wal()

    db = TonboDB(DbOption(temp_dir.name), User())
    for i in range(0, 100):
        user = await db.get(i)
        assert user == {"age": i, "height": i * 10, "weight": i * 20}
