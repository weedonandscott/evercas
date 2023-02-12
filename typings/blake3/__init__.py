# Lorenzo L. Ancora - 2022
# SPDX-License-Identifier: EUPL-1.2

from array import array
from ctypes import _CData  # type:ignore
from mmap import mmap
from typing import Any, Optional, TypeVar, Union

ReadableBuffer = Union[bytes, bytearray, memoryview, array[Any], mmap, _CData]
# ^ Derived from typeshed/stdlib/typeshed/__init__.py.

_Self = TypeVar("_Self")


class blake3:
    AUTO: int = -1
    block_size: int = 64
    digest_size: int = 32
    key_size: int = 32
    name: str = "blake3"

    def __init__(
        self,
        data: Optional[ReadableBuffer] = ...,
        key: Optional[ReadableBuffer] = ...,
        derive_key_context: Optional[str] = ...,
        max_threads: int = ...,
        usedforsecurity: bool = ...,
    ) -> None:
        ...

    def copy(self: _Self) -> _Self:
        ...

    def reset(self: _Self) -> _Self:
        ...

    def digest(self, length: int = ..., *, seek: int = ...) -> bytes:
        ...

    def hexdigest(self, length: int = ..., *, seek: int = ...) -> str:
        ...

    def update(self, data: ReadableBuffer) -> None:
        ...
