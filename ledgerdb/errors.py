"""Domain errors exposed by LedgerDB."""


class StorageCorruptionError(RuntimeError):
    """Raised when a persisted record is malformed or fails integrity checks."""
