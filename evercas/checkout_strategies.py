from __future__ import annotations

from enum import Enum
import os

import anyio
from evercas.put_strategies import Checksummer

from evercas.store_entry import StoreEntry
from ._utils import (
    ProgressAsyncFileReader,
    ProgressCallback,
    TeeAsyncFileReader,
)


class CheckoutStrategy(str, Enum):
    """Available CheckoutStrategies used as input for
    [`CheckoutStrategiesRunner`][evercas.checkout_strategies.CheckoutStrategiesRunner]

    This Enum's members' names are equivalent to the put methods in
       [`CheckoutStrategiesRunner`][evercas.checkout_strategies.CheckoutStrategiesRunner]
    """

    SYMBOLIC_LINK = "SYMBOLIC_LINK"
    COPY = "COPY"


class CheckoutStrategiesRunner:
    """A class responsible for defining and running the different available
    `CheckoutStrategies`.

    A checkout strategy is responsible for extracting a file from the store to an
    external path.

    Read the docs of the individual strategies for their specific considerations.

    Args:
        checksummer: Function that checksums a file, for integrity verification
        fmode: Permissions to set on the new file
        dmode: Permissions to set on created directories, if any

    """

    def __init__(
        self,
        checksummer: Checksummer,
        fmode: int,
        dmode: int,
    ) -> None:
        self._checksummer = checksummer
        self._fmode = fmode
        self._dmode = dmode

    async def run(
        self,
        checkout_strategy: CheckoutStrategy,
        source_entry: StoreEntry,
        dest_path: anyio.Path,
        progress_callback: ProgressCallback | None = None,
        dry_run: bool = False,
    ) -> str | None:
        """Run said checkout strategy

        Read the docs of the individual strategies for their specific considerations.

        Args:
            checkout_strategy: The checkout strategy to use
            source_path: Path to file in store
            dest_path: Location of the symlink
            progress_callback: Callback to receive progress
            dry_run: If `True`, returns checked-out file checksum (or `None` if
            irrelevant) without checking it out

        Returns:
            checksum: Checksum of the checked-out version, or `None` if irrelevant
            (such as with symbolic links)

        Raises:
            RuntimeError: In case of mismatch between source and destination checksums

        """
        source_path = anyio.Path(source_entry.store_root)

        if not source_path.is_file():
            raise ValueError("{source_path} must be a file")

        output_checksum = None

        # Ruff doesn't support match
        # https://github.com/charliermarsh/ruff/issues/282
        match checkout_strategy:  # noqa: E999
            case CheckoutStrategy.SYMBOLIC_LINK:
                output_checksum = await self.symbolic_link(
                    source_path, dest_path, progress_callback, dry_run
                )
            case CheckoutStrategy.COPY:
                output_checksum = await self.copy(
                    source_path, dest_path, progress_callback, dry_run
                )

        if output_checksum is not None and output_checksum != source_entry.checksum:
            raise RuntimeError("Source and checked out checksums do not match")

        return output_checksum

    async def symbolic_link(
        self,
        source_path: anyio.Path,
        dest_path: anyio.Path,
        progress_callback: ProgressCallback | None,
        dry_run: bool = False,
    ) -> None:
        """Symbolic link creates a symbolic link to the stored file at `dest_path`.
        Windows natively supports symbolic links since Windows 10, thus this is the
        recommended way to checkout files as read only.

        Args:
            source_path: Path to file in store
            dest_path: Location of the symlink
            progress_callback: Callback to receive progress
            dry_run: If `True`, returns `None` without creating the link

        Returns:
            checksum (None): Checksum check is irrelevant for symbolic links
        """

        if dry_run:
            if progress_callback:
                progress_callback(str(dest_path), (1, 1))

            return None

        if progress_callback:
            progress_callback(str(dest_path), (0, 1))

        await dest_path.parent.mkdir(
            parents=True,
            mode=self._dmode,
            exist_ok=True,
        )

        await dest_path.symlink_to(source_path)

        if progress_callback:
            progress_callback(str(dest_path), (1, 1))

        return None

    async def copy(
        self,
        source_path: anyio.Path,
        dest_path: anyio.Path,
        progress_callback: ProgressCallback | None,
        dry_run: bool = False,
    ) -> str:
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
            source_path: File to move
            dest_path: Location of the copied file
            progress_callback: Callback to receive progress
            dry_run: If `True`, returns the checksum of the would-be copy without
            actually copying it

        Returns:
            checksum: Checksum of the checked-out version
        """

        if dry_run:
            return await self._checksummer(
                ProgressAsyncFileReader(
                    source_path,
                    progress_callback,
                )
            )

        await dest_path.parent.mkdir(
            parents=True,
            mode=self._dmode,
            exist_ok=True,
        )

        async_reader = ProgressAsyncFileReader(
            TeeAsyncFileReader(source_path, dest_path=dest_path),
            progress_callback,
        )

        checksum = await self._checksummer(async_reader)

        os.chmod(dest_path, self._fmode)

        return checksum
