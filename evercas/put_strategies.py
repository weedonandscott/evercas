from __future__ import annotations

from enum import Enum
import os
from typing import Awaitable, Callable
from uuid import uuid4

import anyio

from evercas.store_entry import StoreEntry
from ._utils import (
    AsyncFileReader,
    ProgressAsyncFileReader,
    ProgressCallback,
    TeeAsyncFileReader,
)

Checksummer = Callable[[AsyncFileReader], Awaitable[str]]
PathBuilder = Callable[[str], anyio.Path]


class PutStrategy(str, Enum):
    """Available PutStrategies used as input for
    [`PutStrategiesRunner`][evercas.put_strategies.PutStrategiesRunner]

    This Enum's members' names are equivalent to the put methods in
       [`PutStrategiesRunner`][evercas.put_strategies.PutStrategiesRunner]
    """

    EARLY_ATOMIC_RENAME = "EARLY_ATOMIC_RENAME"
    LATE_ATOMIC_RENAME = "LATE_ATOMIC_RENAME"
    COPY = "COPY"


class PutStrategiesRunner:
    """A class responsible for defining and running the different available
    `PutStrategies`.

    Warning: Important information about PutStrategy
        The selected [`PutStrategy`][evercas.put_strategies.PutStrategiesRunner] has
        a significant effect on the user experience and resulting data integrity.
        Take you time choosing and considering the different trade-offs.

    A put strategy is responsible for getting a source file from its original location
    to inside the store, located under in a checksum-dependant location computed by
    the using [`Store`][evercas.evercas.Store].

    Different strategies have different advantages and disadvantages pertaining both to
    user experience and to the integrity of the data in the store. Read the docs of the
    individual strategies for their specific considerations.

        Args:
            checksummer: Function that checksums a file
            dest_path_builder: Function that builds a destination path based
                on the checksum
            scratch_dir: Path to use for temporary files. Different strategies
                may have different requirements as to being on the same or different
                file systems relative to the source and/or destination paths.
            fmode: Permissions to set on the new file
            dmode: Permissions to set on created directories, if any

    """

    def __init__(
        self,
        checksummer: Checksummer,
        dest_path_builder: PathBuilder,
        scratch_dir: anyio.Path,
        fmode: int,
        dmode: int,
    ) -> None:
        self._checksummer = checksummer
        self._dest_path_builder = dest_path_builder
        self._scratch_dir = scratch_dir
        self._fmode = fmode
        self._dmode = dmode

    def run(
        self,
        put_strategy: PutStrategy,
        source_path: anyio.Path,
        progress_callback: ProgressCallback | None = None,
    ):

        if not source_path.is_file():
            raise ValueError("{source_path} must be a file")

        # Ruff doesn't support match
        # https://github.com/charliermarsh/ruff/issues/282
        match put_strategy:  # noqa: E999
            case PutStrategy.EARLY_ATOMIC_RENAME:
                return self.early_atomic_rename(source_path, progress_callback)
            case PutStrategy.LATE_ATOMIC_RENAME:
                return self.late_atomic_rename(source_path, progress_callback)
            case PutStrategy.COPY:
                return self.copy(source_path, progress_callback)

    async def early_atomic_rename(
        self,
        source_path: anyio.Path,
        progress_callback: ProgressCallback | None,
    ) -> StoreEntry:
        """Early atomic rename first moves the file object to a
        temporary location, checksums it, then moves it into place.

        This strategy is the most resilient to corruption caused by a file remaining
        open for writing, although nothing is 100% corruption proof.

        The downside is possible poor user experience as the file disappears from the
        source directory and can't be checked back in until after checksum computation.

        `scratch_dir`, `source_path` and the resulting destination need to be on the
        same file system.

        Args:
            source_path: file to move
            progress_callback: Callback to receive progress
        """

        scratch_path = self._scratch_dir.joinpath(f"{uuid4().hex}_{source_path.name}")

        # this is the atomic part
        os.rename(source_path, scratch_path)

        async_reader = ProgressAsyncFileReader(scratch_path, progress_callback)

        checksum = await self._checksummer(async_reader)
        dest_path = self._dest_path_builder(checksum)

        await dest_path.parent.mkdir(
            parents=True,
            mode=self._dmode,
            exist_ok=True,
        )

        is_duplicate = await anyio.Path(dest_path).is_file()

        # this is atomic too, but not the point of this method
        os.rename(scratch_path, dest_path)
        os.chmod(dest_path, self._fmode)

        return StoreEntry(checksum, str(dest_path), is_duplicate)

    async def late_atomic_rename(
        self,
        source_path: anyio.Path,
        progress_callback: ProgressCallback | None,
    ) -> StoreEntry:
        """Late atomic rename checksums the file in its source location,
        and only then moves it into place.

        Note that this strategy is prone to corruption if `src_path` is
        written to before the put completes. This may cause a mismatch
        between the computed and actual checksum of the stored file.

        The chances of the file itself being corrupt depend heavily on the
        manner the offending writes are done.

        This strategy is implemented in cases where safer options either lead
        to bad user experience (some `early_atomic_rename` uses), are impossible
        to achieve cross platform (like with file locking), or are limited by the
        user setup (`early_atomic_rename` does not function across filesystems).

        `source_path` and the resulting destination need to be on the same file system.

        Args:
            source_path: file to move
            progress_callback: Callback to receive progress
        """

        async_reader = ProgressAsyncFileReader(source_path, progress_callback)

        checksum = await self._checksummer(async_reader)
        dest_path = self._dest_path_builder(checksum)

        # this is the atomic part
        await dest_path.parent.mkdir(
            parents=True,
            mode=self._dmode,
            exist_ok=True,
        )

        is_duplicate = await anyio.Path(dest_path).is_file()

        os.rename(source_path, dest_path)
        os.chmod(dest_path, self._fmode)

        return StoreEntry(checksum, str(dest_path), is_duplicate)

    async def copy(
        self,
        source_path: anyio.Path,
        progress_callback: ProgressCallback | None,
    ) -> StoreEntry:
        """Copy checksums the file as it's being copied to a temporary
        location, and then moves the temporary file into place.

        Note that this strategy is prone to corruption if `src_path` is
        written to before the put completes. While there will be not mismatch
        between the computed and actual checksum of the stored file, the file
        itself may become corrupted.

        This strategy may make sense in some cases, for example if atomic rename
        is unsupported on the user's setup.

        `scratch_dir`, and the resulting destination need to be on the same file system.

        Args:
            source_path: file to move
            progress_callback: Callback to receive progress
        """

        scratch_path = self._scratch_dir.joinpath(f"{uuid4().hex}_{source_path.name}")

        async_reader = ProgressAsyncFileReader(
            TeeAsyncFileReader(source_path, destination_path=scratch_path),
            progress_callback,
        )

        checksum = await self._checksummer(async_reader)
        dest_path = self._dest_path_builder(checksum)

        await dest_path.parent.mkdir(
            parents=True,
            mode=self._dmode,
            exist_ok=True,
        )

        is_duplicate = await anyio.Path(dest_path).is_file()

        os.rename(scratch_path, dest_path)
        os.chmod(dest_path, self._fmode)

        return StoreEntry(checksum, str(dest_path), is_duplicate)
