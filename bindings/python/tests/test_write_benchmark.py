import os
import random
import tempfile
import duckdb
import sqlite3
import pytest

from tonbo import Record, Column, DataType, TonboDB, DbOption
from tonbo.fs import from_filesystem_path

WRITE_TIMEs = 500000


@Record
class User:
    id = Column(DataType.Int64, name="id", primary_key=True)
    name = Column(DataType.String, name="name")
    email = Column(DataType.String, name="email", nullable=True)
    age = Column(DataType.UInt16, name="age")
    data = Column(DataType.Bytes, name="data")


def gen_string(max_size):
    size = gen_int(0, max_size)
    charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return ''.join(random.choices(charset, k=size))


def gen_bytes(max_size):
    return gen_string(max_size).encode("utf-8")


def gen_int(lower, high):
    return random.randint(lower, high)


def duckdb_write():
    con = duckdb.connect()
    con.sql(
        "CREATE TABLE user (id INTEGER, name VARCHAR(20), email VARCHAR(20), age INTEGER, data VARCHAR(200))"
    )
    for i in range(0, WRITE_TIMEs):
        con.execute(
            "INSERT INTO user VALUES (?, ?, ?, ?, ?)",
            [i, gen_string(20), gen_string(20), gen_int(0, 0xffff), gen_bytes(200)],
        )
    con.close()


async def tonbo_write():
    temp_dir = tempfile.TemporaryDirectory()

    option = DbOption(from_filesystem_path(temp_dir.name))

    db = TonboDB(option, User())
    txn = await db.transaction()
    for i in range(0, WRITE_TIMEs):
        txn.insert(
            User(
                id=i,
                age=gen_int(0, 0xffff),
                name=gen_string(20),
                email=gen_string(20),
                data=gen_bytes(200),
            )
        )
    await txn.commit()
    await db.flush_wal()


def sqlite_write():
    file = tempfile.NamedTemporaryFile()
    con = sqlite3.connect(file.name)
    con.execute(
        "CREATE TABLE user (id INTEGER, name VARCHAR(20), email VARCHAR(20), age INTEGER, data VARCHAR(200))"
    )
    for i in range(0, WRITE_TIMEs):
        con.execute(
            "INSERT INTO user VALUES (?, ?, ?, ?, ?)",
            [i, gen_string(20), gen_string(20), gen_int(0, 0xffff), gen_bytes(200)],
        )


@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_duckdb(benchmark):
    benchmark(duckdb_write)


@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_tonbo(aio_benchmark):
    aio_benchmark(tonbo_write)


@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_sqlite(benchmark):
    benchmark(sqlite_write)
