from __future__ import annotations

import json
import os
import pathlib
from typing import AsyncGenerator, Literal

import anyio
from blake3 import blake3

from evercas.put_strategies import PutStrategiesRunner, PutStrategy
from evercas.store_entry import StoreEntry

from ._utils import (
    AsyncFileReader,
    ProgressAsyncFileReader,
    ProgressCallback,
    find_files,
    shard,
)

PathLikeArg = str | os.PathLike[str]

"""
    Attributes:
"""


class Store:
    """Manages CRUD actions on stored files.

    If a store was already initialized in `root`, its config will be loaded
    from a file that was saved on init.

    Otherwise, a call to [`init()`][evercas.evercas.Store.init] is required to
    initialize the store.

    Unless otherwise indicated, `EverCas` APIs ***DON'T*** handle exceptions that may
    be raised as part of normal operation.

    Attributes:
        root: Directory path used as root of storage space
        is_initialized: Whether `root` points to an initialized store
        prefix_depth: Count of subdirectories file is hosted in,
        prefix_width: Length of each subdirectory name as taken from the file checksum,
        fmode: store File permissions,
        dmode: store Directory permissions,
        default_put_strategy: Default
            [`PutStrategy`][evercas.put_strategies.PutStrategiesRunner] to use.

    Parameters:
        root: **Absolute** directory path used as root of storage space
    """

    def __init__(self, root: PathLikeArg):
        sync_root = pathlib.Path(root)
        if not sync_root.is_absolute():
            raise ValueError("Store root must be an absolute path")
        sync_root = sync_root.resolve()
        self._root = anyio.Path(sync_root)

        self._is_initialized = False

        try:
            self._import_config()
            self._is_initialized = True
        except FileNotFoundError:
            # config not found is fine, user will need to init a new store
            pass

    def init(
        self,
        prefix_depth: int = 1,
        prefix_width: int = 2,
        fmode: int = 0o400,
        dmode: int = 0o700,
        default_put_strategy: PutStrategy = PutStrategy.EARLY_ATOMIC_RENAME,
    ) -> None:
        """Initialize a new repository in `root`, which must either be an empty
        or non-existent directory.

        Warning: Important information about PutStrategy
            The selected [`PutStrategy`][evercas.put_strategies.PutStrategiesRunner] has
            a significant effect on the user experience and resulting data integrity.
            Take you time choosing and considering the different trade-offs.

        Parameters:
            prefix_depth: Number of prefix folder to create when saving a
                file.
            prefix_width: Width of each prefix folder to create when saving
                a file.
            fmode: File mode permission to set when adding files to
                directory. It is strongly recommended to keep the default `0o400`
                which allows only owner to only read the file, thus avoiding accidental
                loss of data (e.g `echo oops > file`)
            dmode: Directory mode permission to set for
                subdirectories. Defaults to `0o700` which allows owner read, write and
                execute.
            default_put_strategy: Default `PutStrategy` for `put()` and `put_dir()`.

            See documentation for
            [`PutStrategiesRunner`][evercas.put_strategies.PutStrategiesRunner] for the
            various options.
        """
        sync_root = pathlib.Path(self._root)
        sync_root.mkdir(parents=True, exist_ok=True)

        for _ in sync_root.iterdir():
            raise FileExistsError(
                "Store directory must be empty for new repo initialization"
            )

        pathlib.Path(self._scratch_path).mkdir(parents=True)

        self._set_config(
            prefix_depth=prefix_depth,
            prefix_width=prefix_width,
            fmode=fmode,
            dmode=dmode,
            default_put_strategy=default_put_strategy,
        )

        self._is_initialized = True

    @property
    def root(self) -> str:
        """The store's root directory path"""
        return str(self._root)

    @property
    def _scratch_path(self) -> anyio.Path:
        return self._root.joinpath(".scratch")

    @property
    def is_initialized(self) -> bool:
        """`True` if points to an initialized store"""
        return self._is_initialized

    @property
    def _config_file(self) -> pathlib.Path:
        # initialization is sync
        return pathlib.Path(self._root.joinpath(".evercas_conf.json"))

    def _import_config(self) -> None:
        if not self._config_file.is_file():
            raise FileNotFoundError("No config file found")

        config = json.loads(self._config_file.read_text())
        self._set_config(
            prefix_depth=config["prefix_depth"],
            prefix_width=config["prefix_width"],
            fmode=config["fmode"],
            dmode=config["dmode"],
            default_put_strategy=config["default_put_strategy"],
        )

    @property
    def prefix_depth(self) -> int:
        """Quantity of subfolders in which a file is stored"""
        return self._prefix_depth

    @property
    def prefix_width(self) -> int:
        """Portion of the checksum consumed by each subfolder"""
        return self._prefix_width

    @property
    def fmode(self) -> int:
        """The mode set on *new* files in the store"""
        return self._fmode

    @property
    def dmode(self) -> int:
        """The mode set on *new* directories in the store"""
        return self._dmode

    def _set_config(
        self,
        prefix_depth: int,
        prefix_width: int,
        fmode: int,
        dmode: int,
        default_put_strategy: PutStrategy,
    ) -> None:
        if self._config_file.exists():
            raise FileExistsError("Overwriting existing config may cause loss of data")

        if not self._root.is_dir():
            raise FileNotFoundError(f"{self._root} is not a directory")

        self._prefix_depth = prefix_depth
        self._prefix_width = prefix_width
        self._fmode = fmode
        self._dmode = dmode
        self._default_put_strategy = default_put_strategy

        self._put_strategy_runner = PutStrategiesRunner(
            self.compute_checksum,
            self._checksum_to_path,
            self._scratch_path,
            self._fmode,
            self._dmode,
        )

        json_config = json.dumps(
            {
                "prefix_depth": self._prefix_depth,
                "prefix_width": self._prefix_width,
                "fmode": self._fmode,
                "dmode": self._dmode,
                "default_put_strategy": self._default_put_strategy,
            }
        )
        self._config_file.write_text(json_config)

    async def put(
        self,
        pathlike: PathLikeArg,
        progress_callback: ProgressCallback | None = None,
        put_strategy: PutStrategy | None = None,
        dry_run: bool = False,
    ) -> StoreEntry:
        """Store contents of `pathlike` using its content hash for the path.

        Warning: Important information about PutStrategy
            The selected [`PutStrategy`][evercas.put_strategies.PutStrategiesRunner] has
            a significant effect on the user experience and resulting data integrity.
            Take you time choosing and considering the different trade-offs.

        Parameters:
            pathlike: **Absolute** path to file.
            progress_callback: optional callback to receive `put` progress
            put_strategy: The strategy to use for putting the file into the store.
                If `None`, uses store's default strategy.
                See the [`PutStrategy`][evercas.put_strategies.PutStrategiesRunner]
                class for the available options.
            dry_run: Return the `StoreEntry` of the
                file that would be appended but don't store it.

        Returns:
            StoreEntry: File's store entry.
        """
        source_path = anyio.Path(pathlike)

        if not source_path.is_absolute():
            raise ValueError("`pathlike` to put must be absolute")

        if dry_run:
            checksum = await self.compute_checksum(
                ProgressAsyncFileReader(
                    source_path,
                    progress_callback,
                )
            )
            checksum_path = self._checksum_to_path(checksum)
            return StoreEntry(checksum, str(checksum_path), self.exists(checksum))

        created_entry = await self._put_strategy_runner.run(
            put_strategy or self._default_put_strategy,
            source_path,
            progress_callback,
        )

        got_entry = self.get(created_entry.checksum)

        if got_entry is None:
            raise RuntimeError("Unknown error occurred")

        return created_entry

    async def put_dir(
        self,
        pathlike: PathLikeArg,
        recursive: bool = False,
        progress_callback: ProgressCallback | None = None,
        put_strategy: PutStrategy | None = None,
        dry_run: bool = False,
    ) -> AsyncGenerator[tuple[str, StoreEntry], None]:
        """Put all files from a directory.

        Warning: Important information about PutStrategy
            The selected [`PutStrategy`][evercas.put_strategies.PutStrategiesRunner] has
            a significant effect on the user experience and resulting data integrity.
            Take you time choosing and considering the different trade-offs.

        Parameters:
            root: Path to the directory to add.
            recursive: Find files recursively in `root`.
                Defaults to `False`.
            progress_callback: optional callback to receive `put` progress. Called
            separately for each file.
            put_strategy: The strategy to use for putting the files into the store.
                If `None`, uses store's default strategy.
                See the [`PutStrategy`][evercas.put_strategies.PutStrategiesRunner]
                class for the available options.
            dry_run: Return the `StoreEntry` of the
                files that would be appended but don't store any.

        Yields:
            StoreEntry(StoreEntry): For each inserted file
        """

        async for source_file in find_files(anyio.Path(pathlike), recursive=recursive):
            entry = await self.put(
                source_file,
                put_strategy=put_strategy,
                progress_callback=progress_callback,
                dry_run=dry_run,
            )
            yield (str(source_file), entry)

    def get(self, checksum: str) -> StoreEntry | None:
        """Return `StoreEntry` from given checksum or path. If `file` does not
        refer to a valid file, then `None` is returned.

        Parameters:
            file (str): Checksum or path of file.

        Returns:
            StoreEntry: File's hash entry.
        """

        # Check for sharded path.
        filepath = self._checksum_to_path(checksum)
        if filepath.is_file():
            return StoreEntry(checksum, str(filepath))

        # Could not determine a match.
        return None

    async def get_all(self) -> AsyncGenerator[StoreEntry, None]:
        """Return async generator that yields all store entries

        Yields:
            entry (StoreEntry):
        """
        async for file in find_files(self._root, recursive=True):
            entry = self.get(self._path_to_checksum(file))
            if entry is not None:
                yield entry

    def open(
        self, checksum: str, mode: Literal["rb"] | Literal["r"] | Literal["rt"] = "rb"
    ):
        """Return open buffer object from given checksum.
        Only `rb`, `r`, `rt` modes allowed.

        Parameters:
            checksum (str): Checksum of file.

        Returns:
            Buffer: An `io` buffer dependent on the `mode`.

        Raises:
            IOError: If file doesn't exist.
            ValueError: If given forbidden mode.
        """
        if mode not in ["rb", "r", "rt"]:
            raise ValueError(f"Forbidden mode {mode}. Only `rb`, `r`, `rt` allowed.")

        entry = self.get(checksum)
        if entry is None:
            raise IOError(f"Could not locate checksum: {checksum}")

        return self._root.joinpath(entry.path).open(mode)

    async def delete(self, checksum: str):
        """Delete file using checksum. Remove any empty directories after deleting.
        No exception is raised if file doesn't exist.

        Parameters:
            file (str): Checksum or path of file.
        """
        entry = self.get(checksum)
        if entry is None:
            return None

        await self._root.joinpath(entry.path).unlink(missing_ok=True)

    async def count(self) -> int:
        """Return count of the number of files in the `root` directory."""
        count = 0
        async for _ in self.get_all():
            count += 1
        return count

    async def size(self) -> int:
        """Return the total size in bytes of all files in the `root`
        directory.
        """
        total = 0

        async for entry in self.get_all():
            total += (await self._root.joinpath(entry.path).stat()).st_size

        return total

    def exists(self, checksum: str) -> bool:
        """Check whether a given file checksum exists on disk."""
        return self.get(checksum) is not None

    async def compute_checksum(
        self, file: AsyncFileReader | PathLikeArg | anyio.Path
    ) -> str:
        """Compute checksum of file.

        file: File to checksum
        """

        if not isinstance(file, AsyncFileReader):
            file = AsyncFileReader(anyio.Path(file))

        blksize = 4096
        file_size = None

        try:
            file_stat = await file.source_path.stat()
            blksize = file_stat.st_blksize or 4096
            file_size = file_stat.st_size
        except BaseException:
            # if stat fails we just try to move on with file access
            # using the default values for block, file size
            pass

        if not file_size or file_size > 1.5 * 1024 * 1024:  # > 1.5 MiB
            # block-aligned size closest to 32MiB, a benchmark sweet-spot
            chunk_size = (32 * 1024 * 1024 // blksize) * blksize
            max_threads = blake3.AUTO
        else:
            chunk_size = blksize
            max_threads = 4

        # TODO: benchmark and tweak accordingly
        hasher = blake3(max_threads=max_threads)
        async for data in file.read(chunk_size):
            hasher.update(data)

        return hasher.hexdigest()

    def _checksum_to_path(
        self,
        checksum: str,
    ) -> anyio.Path:
        """Build the file path for a given checksum."""
        path_parts = shard(checksum, self._prefix_depth, self._prefix_width)
        return anyio.Path("").joinpath(*path_parts)

    def _path_to_checksum(self, path: anyio.Path) -> str:
        """Unshard path to determine checksum."""
        if path.is_absolute():
            raise ValueError(f"Path {path} must be relative to store root")

        if not self._root.joinpath(path).is_file():
            raise ValueError(
                f"Cannot unshard path. The path {path}"
                f"is not a file contained in the store"
            )

        return "".join(path.parts)

    async def corrupted(
        self, trust_file_path: bool = False
    ) -> AsyncGenerator[tuple[str, StoreEntry], None]:
        """Return generator that yields entries as `(corrupt_path, expected_entry)`
        where `corrupt_path` is the string path of the mis-located file and
        `expected_entry` is the `StoreEntry` of the expected location.

        Parameters:
            trust_file_path: If `True`, gets checksum for each file by un-sharding
            the file's path in the store. If `False`, computes the checksum from
            file content.
        """
        async for entry in self.get_all():

            if trust_file_path:
                checksum = entry.checksum
            else:
                checksum = await self.compute_checksum(
                    self._root.joinpath(anyio.Path(entry.path))
                )

            expected_path = self._checksum_to_path(checksum)

            if expected_path != entry.path:
                yield (
                    entry.path,
                    StoreEntry(checksum, str(expected_path)),
                )

    def __contains__(self, file: str) -> bool:
        """Return whether a given file checksum or path is contained in the
        `root` directory.
        """
        return self.exists(file)

    def __aiter__(self) -> AsyncGenerator[StoreEntry, None]:
        """Iterate over all entries in the store."""
        return self.get_all()



