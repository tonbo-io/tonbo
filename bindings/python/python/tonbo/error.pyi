class CommitError(Exception):
    """Commit error"""
    pass

class DbError(Exception):
    """DB error"""
    pass

class DecodeError(Exception):
    """Decode error"""
    pass

class RecoverError(Exception):
    """Recover error"""
    pass

class ExceedsMaxLevelError(Exception):
    """Exceeds max level"""
    pass

class WriteConflictError(Exception):
    """Write conflict"""
    pass

class RepeatedCommitError(Exception):
    """Repeated commit the same transaction"""
    pass

class PathParseError(Exception):
    """Parse path error"""
    pass