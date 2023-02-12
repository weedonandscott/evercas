from __future__ import annotations

import pathlib
from dataclasses import dataclass


@dataclass(frozen=True)
class StoreEntry:
    """File address containing file's path on disk and it's content checksum.

    Attributes:
        checksum: Hexdigest of file contents.
        path: File path **relative** to `Store.root`.
        store_root: The [`Store`][evercas.store.Store] which this is an entry of
        is_duplicate: Whether the newly returned StoreEntry represents a duplicate
        of an existing file. Can only be `True` after a put operation.
    """

    def set_path(self, path: str):
        if pathlib.Path(path).is_absolute():
            raise ValueError("Entry's path must be a relative path")
        self.__dict__["path"] = path

    def get_path(self) -> str:
        return str(self.__dict__.get("path"))

    def set_store_root(self, store_root: str):
        if not pathlib.Path(store_root).is_absolute():
            raise ValueError("Entry's store_root must be an absolute path")
        self.__dict__["store_root"] = store_root

    def get_store_root(self) -> str:
        return str(self.__dict__.get("store_root"))

    checksum: str
    path: str = property(get_path, set_path)  # type: ignore
    store_root: str = property(get_store_root, set_store_root)  # type: ignore
    is_duplicate: bool = False

    del set_path, get_path, set_store_root, get_store_root
