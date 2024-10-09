from typing import Any, AsyncIterable, Optional, final
from enum import Enum, auto

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

@final
class Column:
    name: str
    datatype: DataType
    nullable: bool
    primary_key: bool
    def __init__(
        self,
        name: str,
        datatype: DataType,
        nullable: bool = False,
        primary_key: bool = False,
    ): ...

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
    def __init__(self, option: DbOption, schema: object): ...
    async def get(self, key: Any) -> Optional[dict[str, Any]]: ...
    async def insert(self, record: object) -> None: ...
    async def remove(self, key: Any) -> None: ...
    async def transaction(self) -> Transaction: ...

@final
class Transaction:
    async def get(
        self, key: Any, projection: Optional[list[str]]
    ) -> Optional[dict[str, Any]]: ...
    def insert(self, record: object) -> None: ...
    def remove(self, key: Any) -> None: ...
    def scan(
        self,
        lower: Optional[Bound],
        higher: Optional[Bound],
        limit: Optional[int],
        projection: list[str],
    ) -> AsyncIterable[dict[str, Any]]: ...
    def commit(self) -> None: ...
