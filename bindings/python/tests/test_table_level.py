import tempfile
import pytest
from tonbo import TonboDB, DbOption, Column, DataType, Record
from tonbo.fs import from_filesystem_path, parse, from_url_path, FsOptions
from tonbo.error import PathParseError


@Record
class User:
    id = Column(DataType.Int64, name="id", primary_key=True)
    name = Column(DataType.String, name="name")
    email = Column(DataType.String, name="email", nullable=True)
    age = Column(DataType.Int8, name="age")
    data = Column(DataType.Bytes, name="data")


def test_parse():
    assert parse("/foo/bar") == "foo/bar"
    with pytest.raises(PathParseError):
        parse("//foo/bar")


def test_from_url_path():
    assert from_url_path("foo%20bar") == "foo bar"
    assert from_url_path("foo%2F%252E%252E%2Fbar") == "foo/%2E%2E/bar"
    assert from_url_path("foo/%252E%252E/bar") == "foo/%2E%2E/bar"
    assert from_url_path("%48%45%4C%4C%4F") == "HELLO"
    with pytest.raises(PathParseError):
        from_url_path("foo/%2E%2E/bar")


@pytest.mark.asyncio
async def test_table_level_local():
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir0 = tempfile.TemporaryDirectory()
    temp_dir1 = tempfile.TemporaryDirectory()

    option = DbOption(from_filesystem_path(temp_dir.name))
    option.level_path(0, from_filesystem_path(temp_dir0.name), FsOptions.Local())
    option.level_path(1, from_filesystem_path(temp_dir1.name), FsOptions.Local())

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
