from __future__ import annotations

import os
import tempfile
from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Callable

import anyio


def compact(items: list[Any]):
    """Return only truthy elements of `items`."""
    return [item for item in items if item]


def shard(checksum: str, prefix_depth: int, prefix_width: int) -> list[str]:
    # This creates a list of `prefix_depth` number of tokens with width
    # `prefix_width` from the first part of the checksum plus the remainder.
    if len(checksum) <= prefix_depth * prefix_width:
        raise ValueError("checksum must be larger prefix_depth * prefix_width")

    return compact(
        [
            checksum[i * prefix_width : prefix_width * (i + 1)]
            for i in range(prefix_depth)
        ]
        + [checksum[prefix_depth * prefix_width :]]
    )


async def find_files(
    path: anyio.Path, recursive: bool = False
) -> AsyncGenerator[anyio.Path, None]:
    if recursive:
        async for sub_path in path.glob("**"):
            if sub_path.is_file():
                yield sub_path
    else:
        async for sub_path in path.iterdir():
            if sub_path.is_file():
                yield sub_path


class BaseAsyncFileReader(ABC):
    @property
    @abstractmethod
    def file_path(self) -> anyio.Path:
        pass

    @abstractmethod
    async def read(self, size: int = -1) -> AsyncGenerator[bytes, None]:
        pass


class AsyncFileReader:
    def __init__(self, source: anyio.Path | AsyncFileReader) -> None:
        self._source = source
        # self._file_path = file_path

    @property
    def source_path(self) -> anyio.Path:
        if isinstance(self._source, anyio.Path):
            return self._source
        return self._source.source_path

    async def read(self, size: int = -1) -> AsyncGenerator[bytes, None]:
        if isinstance(self._source, anyio.Path):
            async with await self.source_path.open("rb") as file:
                while True:
                    data = await file.read(size)
                    if not data:
                        break
                    yield data
        else:
            async for data in self._source.read(size):
                yield data


class TeeAsyncFileReader(AsyncFileReader):
    def __init__(self, source: anyio.Path | AsyncFileReader, dest_path: anyio.Path):
        super().__init__(source)
        self._destination_path = dest_path

    @property
    def destination_path(self) -> anyio.Path:
        return self._destination_path

    async def read(self, size: int = -1) -> AsyncGenerator[bytes, None]:
        await self._destination_path.parent.mkdir(parents=True, exist_ok=True)
        temp_file = tempfile.NamedTemporaryFile(
            dir=str(self._destination_path.parent), delete=False
        )
        with temp_file:
            async_temp_file = anyio.wrap_file(temp_file)
            async for data in super().read(size):
                await async_temp_file.write(data)
                yield data
        os.rename(os.path.realpath(temp_file.name), str(self._destination_path))


ProgressCallback = Callable[[str, tuple[int, int | None]], Any]


class ProgressAsyncFileReader(AsyncFileReader):
    def __init__(
        self,
        source: anyio.Path | AsyncFileReader,
        progress_callback: ProgressCallback | None,
    ):
        super().__init__(source)
        self._progress_callback = progress_callback

    async def read(self, size: int = -1) -> AsyncGenerator[bytes, None]:
        total_bytes = None

        if self._progress_callback is not None:
            try:
                stat = await self.source_path.stat()
                total_bytes = stat.st_size
            except BaseException:
                # DON'T CAUSE CRASH
                pass

        curr_chunk = 0
        async for data in super().read(size):
            if self._progress_callback is not None:
                curr_chunk = curr_chunk + 1
                curr_bytes = curr_chunk * size
                self._progress_callback(
                    str(self.source_path), (curr_bytes, total_bytes)
                )
            yield data
