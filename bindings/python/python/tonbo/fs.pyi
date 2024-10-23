from typing import final

@final
class AwsCredential:
    key_id: str
    secret_key: str
    token: str | None

    def __init__(
        self, key_id: str, secret_key: str, token: str | None = None
    ) -> None: ...

@final
class FsOptions:
    class Local: ...

    class S3:
        bucket: str
        credential: AwsCredential | None
        region: str | None
        sign_payload: bool | None
        checksum: bool | None

        def __init__(
            self,
            bucket: str,
            credential: AwsCredential | None = None,
            region: str | None = None,
            sign_payload: bool | None = None,
            checksum: bool | None = None,
        ) -> None: ...

def parse(path: str) -> str: ...

def from_filesystem_path(path: str) -> str:
    """Parse path to a filesystem relative path"""
    ...
def from_absolute_path(path: str) -> str:
    """Parse path to a filesystem absolute path"""
    ...
def from_url_path(path: str) -> str:
    """Parse an url path"""
    ...
