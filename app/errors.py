"""Domain exceptions mapped to HTTP responses in :mod:`app.main`."""
from __future__ import annotations


class StorageError(Exception):
    """Base class for expected storage-layer failures."""

    status_code = 400


class CsvValidationError(StorageError):
    status_code = 400


class MergeConflictError(StorageError):
    """Another merge for the same station/day is already running."""

    status_code = 409


class MergeFailedError(StorageError):
    """A merge was attempted but could not be completed atomically."""

    status_code = 409


class NotFoundError(StorageError):
    status_code = 404


class CursorError(StorageError):
    """A pull cursor is malformed, belongs to another station, or is out of range."""

    status_code = 400


class PullReadError(StorageError):
    """Published files could not be read consistently; the same cursor may be retried."""

    status_code = 503
