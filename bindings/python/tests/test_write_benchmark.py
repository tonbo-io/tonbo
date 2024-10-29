import os
import tempfile
import duckdb
import sqlite3
import pytest

from conftest import gen_string, gen_int, gen_bytes
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


def duckdb_write(auto_commit: bool):
    con = duckdb.connect()
    con.sql(
        "CREATE TABLE user (id INTEGER, name VARCHAR(20), email VARCHAR(20), age INTEGER, data VARCHAR(200))"
    )
    if not auto_commit:
        con.begin()
    for i in range(0, WRITE_TIME):
        con.execute(
            "INSERT INTO user VALUES (?, ?, ?, ?, ?)",
            [i, gen_string(20), gen_string(20), gen_int(0, 0xffff), gen_bytes(200)],
        )
    if not auto_commit:
        con.commit()
    con.close()


async def tonbo_write(auto_commit: bool):
    temp_dir = tempfile.TemporaryDirectory()

    option = DbOption(from_filesystem_path(temp_dir.name))

    db = TonboDB(option, User())
    if auto_commit:
        for i in range(0, WRITE_TIME):
            await db.insert(User(
                id=i,
                age=gen_int(0, 0xffff),
                name=gen_string(20),
                email=gen_string(20),
                data=gen_bytes(200),
            ))
    else:
        txn = await db.transaction()
        for i in range(0, WRITE_TIME):
            txn.insert(User(
                id=i,
                age=gen_int(0, 0xffff),
                name=gen_string(20),
                email=gen_string(20),
                data=gen_bytes(200),
            ))
        await txn.commit()

    await db.flush_wal()


def sqlite_write(auto_commit: bool):
    file = tempfile.NamedTemporaryFile()
    con = sqlite3.connect(file.name, autocommit=auto_commit)
    con.execute(
        "CREATE TABLE user (id INTEGER, name VARCHAR(20), email VARCHAR(20), age INTEGER, data VARCHAR(200))"
    )
    for i in range(0, WRITE_TIME):
        con.execute(
            "INSERT INTO user VALUES (?, ?, ?, ?, ?)",
            [i, gen_string(20), gen_string(20), gen_int(0, 0xffff), gen_bytes(200)],
        )
    con.commit()
    con.close()


@pytest.mark.parametrize("auto_commit", [True])
@pytest.mark.benchmark(group="autocommit")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_duckdb_autocommit(benchmark, auto_commit):
    benchmark(duckdb_write, auto_commit)


@pytest.mark.parametrize("auto_commit", [False])
@pytest.mark.benchmark(group="txn")
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
def test_duckdb(benchmark, auto_commit):
    benchmark(duckdb_write, auto_commit)


@pytest.mark.parametrize("auto_commit", [False])
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
@pytest.mark.benchmark(group="txn")
def test_tonbo(aio_benchmark, auto_commit):
    aio_benchmark(tonbo_write, auto_commit)


@pytest.mark.parametrize("auto_commit", [True])
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
@pytest.mark.benchmark(group="autocommit")
def test_tonbo_no_txn(aio_benchmark, auto_commit):
    aio_benchmark(tonbo_write, auto_commit)


@pytest.mark.parametrize("auto_commit", [True])
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
@pytest.mark.benchmark(group="autocommit")
def test_sqlite_autocommit(benchmark, auto_commit):
    benchmark(sqlite_write, auto_commit)


@pytest.mark.parametrize("auto_commit", [False])
@pytest.mark.skipif("BENCH" not in os.environ, reason="benchmark")
@pytest.mark.benchmark(group="txn")
def test_sqlite(benchmark, auto_commit):
    benchmark(sqlite_write, auto_commit)
