import os

import pytest
import tempfile
from tonbo import DbOption, Column, DataType, Record, TonboDB
from tonbo.fs import FsOptions, AwsCredential, from_url_path


@Record
class User:
    id = Column(DataType.Int64, name="id", primary_key=True)
    name = Column(DataType.String, name="name")
    email = Column(DataType.String, name="email", nullable=True)
    age = Column(DataType.UInt8, name="age")
    data = Column(DataType.Bytes, name="data")


@pytest.mark.asyncio
@pytest.mark.skipif("S3" not in os.environ, reason="s3")
async def test_s3_read_write():

    temp_dir = tempfile.TemporaryDirectory()

    key_id = os.environ['AWS_ACCESS_KEY_ID']
    secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
    credential = AwsCredential(key_id, secret_key)
    fs_option = FsOptions.S3("wasm-data", credential,"ap-southeast-2",None, None, None)

    option = DbOption(temp_dir.name)
    option.level_path(0, from_url_path("l0"), fs_option, False)
    option.level_path(1, from_url_path("l1"), fs_option, False)
    option.level_path(2, from_url_path("l2"), fs_option, False)

    option.immutable_chunk_num = 1
    option.major_threshold_with_sst_size = 3
    option.level_sst_magnification = 1
    option.max_sst_file_size = 1 * 1024

    db = TonboDB(option, User())
    for i in range(0, 500):
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
    user = await db.get(10)
    assert user == {
        "id": 10,
        "name": str(10 * 10),
        "email": str(10 * 20),
        "age": 10,
        "data": b"Hello Tonbo!",
    }




