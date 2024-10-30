import asyncio
import os
import tempfile
import duckdb
import sqlite3
import pytest

from conftest import gen_int, gen_string, gen_bytes
from tonbo import Record, Column, DataType, TonboDB, DbOption
from tonbo.fs import from_filesystem_path

WRITE_TIME = 500000


@Record
class User:
    id = Column(DataType.Int64, name="id", primary_key=True)
    name = Column(DataType.String, name="name")
    email = Column(DataType.String, name="email", nullable=True)
    age = Column(DataType.UInt16, name="age")
    data = Column(DataType.Bytes, name="data")


async def duckdb_write(threads: int):
    con = duckdb.connect(config={"threads": threads})
    con.sql(
        "CREATE TABLE user (id INTEGER, name VARCHAR(20), email VARCHAR(20), age INTEGER, data VARCHAR(200))"
    )

    async def insert_task(start: int, end: int):
        txn = con.begin()
        for j in range(start, end):
            txn.execute(
                "INSERT INTO user VALUES (?, ?, ?, ?, ?)",
                [i, gen_string(20), gen_string(20), gen_int(0, 0xffff), gen_bytes(200)],
            )
        txn.commit()

    tasks = []
    for i in range(0, threads):
        tasks.append(insert_task(i * WRITE_TIME // threads, (i + 1) * WRITE_TIME // threads))

    await asyncio.gather(*tasks)
    con.commit()
    con.close()


async def tonbo_write(threads: int):
    temp_dir = tempfile.TemporaryDirectory()

    option = DbOption(from_filesystem_path(temp_dir.name))

    db = TonboDB(option, User())
    tasks = []

    async def insert_task(start: int, end: int):
        txn = await db.transaction()
        for j in range(start, end):
            txn.insert(
                User(
                    id=j,
                    age=gen_int(0, 0xffff),
                    name=gen_string(20),
                    email=gen_string(20),
                    data=gen_bytes(200),
                )
            )
        await txn.commit()

    for i in range(0, threads):
        tasks.append(insert_task(i * WRITE_TIME // threads, (i + 1) * WRITE_TIME // threads))

    await asyncio.gather(*tasks)
    await db.flush_wal()


async def tonbo_write_disable_wal(threads: int):
    temp_dir = tempfile.TemporaryDirectory()

    option = DbOption(from_filesystem_path(temp_dir.name))
    option.use_wal = False

    db = TonboDB(option, User())
    tasks = []

    async def insert_task(start: int, end: int):
        txn = await db.transaction()
        for j in range(start, end):
            txn.insert(
                User(
                    id=j,
                    age=gen_int(0, 0xffff),
                    name=gen_string(20),
                    email=gen_string(20),
                    data=gen_bytes(200),
                )
            )
        await txn.commit()

    for i in range(0, threads):
        tasks.append(insert_task(i * WRITE_TIME // threads, (i + 1) * WRITE_TIME // threads))

    await asyncio.gather(*tasks)


async def sqlite_write(threads: int):
    file = tempfile.NamedTemporaryFile()
    con = sqlite3.connect(file.name, check_same_thread=False)
    con.execute(
        "CREATE TABLE user (id INTEGER, name VARCHAR(20), email VARCHAR(20), age INTEGER, data VARCHAR(200))"
    )

    async def insert_task(start: int, end: int):
        for j in range(start, end):
            con.execute(
                "INSERT INTO user VALUES (?, ?, ?, ?, ?)",
                [j, gen_string(20), gen_string(20), gen_int(0, 0xffff), gen_bytes(200)],
            )
        con.commit()

    tasks = []
    for i in range(0, threads):
        tasks.append(insert_task(i * WRITE_TIME // threads, (i + 1) * WRITE_TIME // threads))
    await asyncio.gather(*tasks)
    con.commit()
    con.close()


@pytest.mark.parametrize("threads", [1])
@pytest.mark.benchmark(group="1 async task")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_duckdb_one_task(aio_benchmark, threads):
    aio_benchmark(duckdb_write, threads)


@pytest.mark.parametrize("threads", [1])
@pytest.mark.benchmark(group="1 async task")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_tonbo_one_task(aio_benchmark, threads):
    aio_benchmark(tonbo_write, threads)


@pytest.mark.parametrize("threads", [1])
@pytest.mark.benchmark(group="1 async task")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_tonbo_disable_wal_one_task(aio_benchmark, threads):
    aio_benchmark(tonbo_write_disable_wal, threads)


@pytest.mark.parametrize("threads", [1])
@pytest.mark.benchmark(group="1 async task")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_sqlite_one_task(aio_benchmark, threads):
    aio_benchmark(sqlite_write, threads)


@pytest.mark.parametrize("threads", [4])
@pytest.mark.benchmark(group="4 async task")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_duckdb_four_task(aio_benchmark, threads):
    aio_benchmark(duckdb_write, threads)


@pytest.mark.parametrize("threads", [4])
@pytest.mark.benchmark(group="4 async task")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_tonbo_four_task(aio_benchmark, threads):
    aio_benchmark(tonbo_write, threads)


@pytest.mark.parametrize("threads", [4])
@pytest.mark.benchmark(group="4 async task")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_tonbo_disable_wal_four_task(aio_benchmark, threads):
    aio_benchmark(tonbo_write_disable_wal, threads)


@pytest.mark.parametrize("threads", [4])
@pytest.mark.benchmark(group="4 async task")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_sqlite_four_task(aio_benchmark, threads):
    aio_benchmark(sqlite_write, threads)


@pytest.mark.parametrize("threads", [8])
@pytest.mark.benchmark(group="8 async task")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_duckdb_eight_task(aio_benchmark, threads):
    aio_benchmark(duckdb_write, threads)


@pytest.mark.parametrize("threads", [8])
@pytest.mark.benchmark(group="8 async task")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_tonbo_eight_task(aio_benchmark, threads):
    aio_benchmark(tonbo_write, threads)


@pytest.mark.parametrize("threads", [8])
@pytest.mark.benchmark(group="8 async task")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_tonbo_disable_wal_eight_task(aio_benchmark, threads):
    aio_benchmark(tonbo_write_disable_wal, threads)


@pytest.mark.parametrize("threads", [8])
@pytest.mark.benchmark(group="8 async task")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_sqlite_eight_task(aio_benchmark, threads):
    aio_benchmark(sqlite_write, threads)
