from typing import Any, AsyncIterable, Optional, final
from enum import Enum, auto
from tonbo import error as error

@final
class Record: ...

@final
class Bound(Enum):
    Included = auto()
    Excluded = auto()

@final
class DataType(Enum):
    Int8 = auto()
    Int16 = auto()
    Int32 = auto()
    Int64 = auto()
    String = auto()
    Boolean = auto()
    Bytes = auto()

@final
class Column:
    name: str
    datatype: DataType
    nullable: bool
    primary_key: bool
    def __init__(
        self,
        datatype: DataType,
        name: str,
        nullable: bool = False,
        primary_key: bool = False,
    ): ...

@final
class RecordBatch:
    def __init__(self): ...
    def append(self, record: object): ...

class DbOption:
    clean_channel_buffer: int
    immutable_chunk_num: int
    level_sst_magnification: int
    major_default_oldest_table_num: int
    major_threshold_with_sst_size: int
    max_sst_file_size: int
    version_log_snapshot_threshold: int
    use_wal: bool
    path: str

    def __init__(self, path: str): ...

@final
class TonboDB:
    def __init__(self, option: DbOption, schema: Any): ...
    async def get(self, key: Any) -> Optional[dict[str, Any]]: ...
    async def insert(self, record: object) -> None: ...
    async def insert_batch(self, record_batch: RecordBatch) -> None: ...
    async def remove(self, key: Any) -> None: ...
    async def transaction(self) -> Transaction: ...
    async def flush(self) -> None: ...

@final
class Transaction:
    async def get(
        self, key: Any, projection: list[str] = ["*"]
    ) -> Optional[dict[str, Any]]: ...
    def insert(self, record: object) -> None: ...
    def remove(self, key: Any) -> None: ...
    async def scan(
        self,
        lower: Optional[Bound],
        higher: Optional[Bound],
        limit: Optional[int],
        projection: list[str] = ["*"],
    ) -> AsyncIterable[dict[str, Any]]: ...
    async def commit(self) -> None: ...
