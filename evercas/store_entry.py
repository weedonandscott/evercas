from __future__ import annotations

import pathlib
from dataclasses import dataclass


@dataclass(frozen=True)
class StoreEntry:
    """File address containing file's path on disk and it's content checksum.

    Attributes:
        checksum: Hexdigest of file contents.
        path: File path **relative** to `Store.root`.
        is_duplicate: Whether the newly returned StoreEntry represents a duplicate
        of an existing file. Can only be `True` after a put operation.
    """

    def set_path(self, path: str):
        if pathlib.Path(path).is_absolute():
            raise ValueError("Entry path must be relative to its store's root")
        self.__dict__["path"] = path

    def get_path(self) -> str:
        return str(self.__dict__.get("path"))

    checksum: str
    path: str = property(get_path, set_path)  # type: ignore
    is_duplicate: bool = False

    del set_path, get_path
