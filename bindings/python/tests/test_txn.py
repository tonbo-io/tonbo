import pytest
import tempfile
from tonbo import DbOption, Column, DataType, Record, TonboDB, Bound
from tonbo.error import RepeatedCommitError, WriteConflictError

@Record
class User:
    age = Column(DataType.Int8, name="age", primary_key=True)
    height = Column(DataType.Int16, name="height", nullable=True)
    weight = Column(DataType.Int32, name="weight", nullable=False)


def build_db():
    temp_dir = tempfile.TemporaryDirectory()
    return TonboDB(DbOption(temp_dir.name), User())


@pytest.mark.asyncio
async def test_txn_write():
    db = build_db()
    txn = await db.transaction()
    for i in range(0, 100):
        txn.insert(User(age=i, height=i * 10, weight=i * 20))


@pytest.mark.asyncio
async def test_txn_read_write():
    db = build_db()
    txn = await db.transaction()
    for i in range(0, 100):
        txn.insert(User(age=i, height=i * 10, weight=i * 20))
    for i in range(0, 100):
        txn.insert(User(age=i, height=i * 11, weight=i * 22))

    for i in range(0, 100):
        user = await txn.get(i)
        assert user == {"age": i, "height": i * 11, "weight": i * 22}


@pytest.mark.asyncio
async def test_txn_remove():
    db = build_db()
    txn = await db.transaction()
    for i in range(0, 100):
        if i % 2 == 0:
            txn.insert(User(age=i, height=i * 10, weight=i * 20))
        else:
            txn.remove(i)

    for i in range(0, 100):
        user = await txn.get(i)
        if i % 2 == 0:
            assert user == {"age": i, "height": i * 10, "weight": i * 20}
        else:
            assert user is None


@pytest.mark.asyncio
async def test_txn_scan():
    db = build_db()
    txn = await db.transaction()
    for i in range(0, 100):
        txn.insert(User(age=i, height=i * 10, weight=i * 20))

    scan = await txn.scan(Bound.Included(10), Bound.Excluded(75))
    i = 10
    async for user in scan:
        assert user == {"age": i, "height": i * 10, "weight": i * 20}
        i += 1
    assert i == 75


@pytest.mark.asyncio
async def test_txn_projection():
    db = build_db()
    txn = await db.transaction()
    for i in range(0, 100):
        txn.insert(User(age=i, height=i * 10, weight=i * 20))

    scan = await txn.scan(None, Bound.Excluded(75), projection=["age", "height"])
    i = 0
    async for user in scan:
        assert user == {"age": i, "height": i * 10, "weight": None}
        i += 1


@pytest.mark.asyncio
async def test_txn_limit():
    db = build_db()
    txn = await db.transaction()
    for i in range(0, 100):
        txn.insert(User(age=i, height=i * 10, weight=i * 20))

    scan = await txn.scan(None, Bound.Excluded(75), limit=10)
    i = 0
    async for user in scan:
        assert user == {"age": i, "height": i * 10, "weight": i * 20}
        i += 1
    assert i == 10


@pytest.mark.asyncio
async def test_repeated_commit():
    db = build_db()
    txn = await db.transaction()
    await txn.commit()

    with pytest.raises(RepeatedCommitError):
        await txn.commit()


@pytest.mark.asyncio
async def test_txn_write_conflict():
    db = build_db()
    txn = await db.transaction()
    txn2 = await db.transaction()
    for i in range(0, 10):
        txn.insert(User(age=i, height=i * 10, weight=i * 20))

    for i in range(0, 10):
        txn2.insert(User(age=i, height=i * 10, weight=i * 20))

    await txn2.commit()
    with pytest.raises(WriteConflictError):
        await txn.commit()
