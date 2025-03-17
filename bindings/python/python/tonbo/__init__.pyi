from typing import Any, AsyncIterable, final
from enum import Enum, auto
from tonbo import error as error
from tonbo.fs import FsOptions

@final
class Record:
    def __call__(self) -> None: ...

@final
class Bound(Enum):
    """Tonbo range for scan. None for unbounded"""

    @staticmethod
    def Included(key: Any) -> Bound: ...
    @staticmethod
    def Excluded(key: Any) -> Bound: ...

@final
class DataType(Enum):
    """Tonbo data type."""

    UInt8 = auto()
    UInt16 = auto()
    UInt32 = auto()
    UInt64 = auto()
    Int8 = auto()
    Int16 = auto()
    Int32 = auto()
    Int64 = auto()
    String = auto()
    Boolean = auto()
    Bytes = auto()

@final
class Column:
    """Column represents properties of a field."""

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
    ) -> None: ...

@final
class RecordBatch:
    """Column represents properties of a field."""
    def __init__(self) -> None: ...
    def append(self, record: object) -> None: ...

class DbOption:
    """Tonbo configurations."""

    clean_channel_buffer: int
    immutable_chunk_num: int
    level_sst_magnification: int
    major_default_oldest_table_num: int
    major_threshold_with_sst_size: int
    max_sst_file_size: int
    version_log_snapshot_threshold: int
    use_wal: bool
    wal_buffer_size: int
    path: str

    def __init__(self, path: str) -> None:
        """Create a new :py:class:`DbOption` with the given path. Note: the path must exist"""
        ...

    def level_path(self, level: int, path: str, fs_options: FsOptions) -> None:
        """Set path for assigned level

        Args:
            level: Level for output.
            path: Path for output
            fs_options: Local or S3
        """
        ...

@final
class Transaction:
    """Tonbo transaction."""

    async def get(
        self, key: Any, projection: list[str] = ["*"]
    ) -> dict[str, Any] | None:
        """Get record from db.

        Args:
            key: Primary key of record.
            projection: fields to projection
        """
        ...
    def insert(self, record: object) -> None:
        """Insert record to db."""
        ...
    def remove(self, key: Any) -> None:
        """Remove record from db.
        Args:
            key: Primary key of record.
        """
        ...
    async def scan(
        self,
        lower: Bound | None,
        high: Bound | None,
        limit: int | None,
        projection: list[str] = ["*"],
    ) -> AsyncIterable[dict[str, Any]]:
        """Create an async stream for scanning.

        Args:
            lower: Lower bound of range. Use None represent unbounded.
            high: High bound of range. Use None represent unbounded.
            limit: max number records to scan
            projection: fields to projection
        """
        ...
    async def commit(self) -> None:
        """Commit :py:class:`Transaction`."""
        ...

@final
class TonboDB:
    def __init__(self, option: DbOption, schema: Any) -> None:
        """Create a new :py:class:`TonboDB` with the given configuration options.

        Args:
            option: Configuration options.
            schema: Schema of record.
        """
        ...
    async def get(self, key: Any) -> dict[str, Any] | None:
        """Get record from db.

        Args:
            key: Primary key of record.
        """
        ...
    async def insert(self, record: object) -> None:
        """Insert a record to db."""
        ...
    async def insert_batch(self, record_batch: RecordBatch) -> None:
        """Insert :py:class:`RecordBatch` to db."""
        ...
    async def remove(self, key: Any) -> None:
        """Remove record from db.

        Args:
            key: Primary key of record.
        """
        ...
    async def transaction(self) -> Transaction:
        """Create a new :py:class:`Transaction`."""
        ...
    async def flush(self) -> None:
        """Try to execute compaction."""
        ...
    async def flush_wal(self) -> None:
        """Flush wal manually."""
        ...
