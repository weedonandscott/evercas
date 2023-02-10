from __future__ import annotations

import errno
import json
import os
import pathlib
import shutil
from dataclasses import dataclass
from tempfile import NamedTemporaryFile
from typing import AsyncGenerator, Callable, Literal

import anyio
from blake3 import blake3

from .utils import shard

PathLikeArg = str | os.PathLike[str]

"""
    Attributes:
"""


class Store:
    """Manages CRUD actions on stored files.

    If a store was already initialized in `root`, its config will be loaded
    from a file that was saved on init.

    Otherwise, a call to `init()` is required to initialize the store.

    Attributes:
        root: Directory path used as root of storage space
        is_initialized: Whether `root` points to an initialized store
        prefix_depth: Count of subdirectories file is hosted in,
        prefix_width: Length of each subdirectory name as taken from the file checksum,
        fmode: store File permissions,
        dmode: store Directory permissions,
        put_strategy: Selected `put_strategy`

    Parameters:
        root: **Absolute** directory path used as root of storage space
    """

    def __init__(
        self,
        root: PathLikeArg,
    ):
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
        put_strategy: str | None = None,
    ) -> None:
        """Initialize a new repository in `root`, which must either be an empty
        or non-existent directory.

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
            put_strategy: Default `put_strategy` for
                `put` method. See `put` for more information. Defaults
                to `PutStrategies.copy`.
        """
        sync_root = pathlib.Path(self._root)
        for _ in sync_root.iterdir():
            raise FileExistsError(
                "Store directory must be empty for new repo initialization"
            )

        if not sync_root.is_dir():
            sync_root.mkdir(parents=True)

        self._set_config(
            prefix_depth=prefix_depth,
            prefix_width=prefix_width,
            fmode=fmode,
            dmode=dmode,
            put_strategy=put_strategy,
        )

        self._is_initialized = True

    @property
    def root(self) -> str:
        return str(self.root)

    @property
    def is_initialized(self) -> bool:
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
            put_strategy=config["put_strategy"],
        )

    @property
    def prefix_depth(self) -> int:
        return self._prefix_depth

    @property
    def prefix_width(self) -> int:
        return self._prefix_width

    @property
    def fmode(self) -> int:
        return self._fmode

    @property
    def dmode(self) -> int:
        return self._dmode

    @property
    def put_strategy(self):
        return self._put_strategy

    def _set_config(
        self,
        prefix_depth: int,
        prefix_width: int,
        fmode: int,
        dmode: int,
        put_strategy: str | None,
    ) -> None:
        if self._config_file.exists():
            raise FileExistsError("Overwriting existing config may cause loss of data")

        if not self._root.is_dir():
            raise FileNotFoundError(f"Store root directory not found at {self._root}")

        self._prefix_depth = prefix_depth
        self._prefix_width = prefix_width
        self._fmode = fmode
        self._dmode = dmode
        self._put_strategy = PutStrategies.get(put_strategy) or PutStrategies.copy

        json_config = json.dumps(
            {
                "prefix_depth": self._prefix_depth,
                "prefix_width": self._prefix_width,
                "fmode": self._fmode,
                "dmode": self._dmode,
                "put_strategy": self._put_strategy,
            }
        )
        self._config_file.write_text(json_config)

    async def put(
        self,
        pathlike: PathLikeArg,
        put_strategy: str | None = None,
        dry_run: bool = False,
    ) -> StoreEntry:
        """Store contents of `pathlike` using its content hash for the path.

        Parameters:
            pathlike: **Absolute** path to file.
            put_strategy: The strategy to use for adding
                files; may be a function or the string name of one of the
                built-in put strategies declared in `PutStrategies`
                class. Defaults to `PutStrategies.copy`.
            dry_run: Return the `StoreEntry` of the
                file that would be appended but don't do anything.

        Put strategies are functions `(store, source_path, dest_path)` where
        `store` is the `Store` instance from which `put` was
        called; `source_path` is the `anyio.Path` object representing the
        data to add; and `dest_path` is the string absolute file path inside
        the Store where it needs to be saved. The put strategy function should
        create the path `dest_path` containing the data in `source_path`.

        There are currently two built-in put strategies: "copy" (the default)
        and "link". "link" attempts to hard link the file into the Store if
        the platform and underlying filesystem support it, and falls back to
        "copy" behavior.

        Returns:
            StoreEntry: File's store entry.
        """
        source_path = anyio.Path(pathlike)

        if not source_path.is_absolute():
            raise ValueError("`pathlike` to put must be absolute")

        checksum = await self.compute_checksum(source_path)
        checksum_path = self.checksum_path(checksum)

        # Only move file if it doesn't already exist.
        if not os.path.isfile(checksum_path):
            is_duplicate = False
            if not dry_run:
                self.makepath(os.path.dirname(checksum_path))
                put_strategy_callable = (
                    PutStrategies.get(put_strategy)
                    or self._put_strategy
                    or PutStrategies.copy
                )
                put_strategy_callable(self, source_path, checksum_path)
        else:
            is_duplicate = True

        return StoreEntry(checksum, str(self.relpath(checksum_path)), is_duplicate)

    async def putdir(
        self,
        pathlike: PathLikeArg,
        recursive: bool = False,
        put_strategy: str | None = None,
        dry_run: bool = False,
    ) -> AsyncGenerator[tuple[anyio.Path, StoreEntry], None]:
        """Put all files from a directory.

        Parameters:
            root: Path to the directory to add.
            recursive: Find files recursively in `root`.
                Defaults to `False`.
            put_strategy: same as `put`.
            dry_run: same as `put`.

        Yields:
            StoreEntry(StoreEntry): For each put file
        """
        async for file in find_files(anyio.Path(pathlike), recursive=recursive):
            entry = await self.put(file, put_strategy=put_strategy, dry_run=dry_run)
            yield (file, entry)

    async def mktempfile(self, source_file_path: anyio.Path):
        """Create a named temporary file from a `anyio.Path` object and
        return its filename.
        """
        tmp = NamedTemporaryFile(delete=False)

        oldmask = os.umask(0)

        try:
            os.chmod(tmp.name, self._fmode)
        finally:
            os.umask(oldmask)

        chunk_size = (await source_file_path.stat()).st_blksize
        async with await source_file_path.open("rb") as source_file:
            while True:
                data = await source_file.read(chunk_size)
                if not data:
                    break
                tmp.write(to_bytes(data))

        tmp.close()

        return tmp.name

    def get(self, checksum: str) -> StoreEntry | None:
        """Return `StoreEntry` from given checksum or path. If `file` does not
        refer to a valid file, then `None` is returned.

        Parameters:
            file (str): Checksum or path of file.

        Returns:
            StoreEntry: File's hash entry.
        """

        # Check for sharded path.
        filepath = self.checksum_path(checksum)
        if filepath.is_file():
            return StoreEntry(checksum, str(self.relpath(filepath)))

        # Could not determine a match.
        return None

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

        return self.abspath(entry.path).open(mode)

    async def delete(self, checksum: str):
        """Delete file using checksum. Remove any empty directories after deleting.
        No exception is raised if file doesn't exist.

        Parameters:
            file (str): Checksum or path of file.
        """
        entry = self.get(checksum)
        if entry is None:
            return None

        await self.abspath(entry.path).unlink(missing_ok=True)

    async def files(self):
        """Return generator that yields all files in the `root`
        directory.
        """
        async for async_path in find_files(self._root, recursive=True):
            yield async_path

    def folders(self):
        """Return generator that yields all folders in the `root`
        directory that contain files.
        """
        for folder, _, files in os.walk(self._root):
            if files:
                yield folder

    async def count(self):
        """Return count of the number of files in the `root` directory."""
        count = 0
        async for _ in self:
            count += 1
        return count

    async def size(self):
        """Return the total size in bytes of all files in the `root`
        directory.
        """
        total = 0

        async for path in self.files():
            total += (await path.stat()).st_size

        return total

    def exists(self, checksum: str):
        """Check whether a given file checksum exists on disk."""
        return self.get(checksum) is not None

    def haspath(self, pathlike: PathLikeArg):
        """Return whether `pathlike` is a subdirectory of the `root`
        directory.
        """
        return self._root in anyio.Path(pathlike).parents

    def makepath(self, path: str):
        """Physically create the folder path on disk."""
        try:
            os.makedirs(path, self._dmode)
        except FileExistsError:
            assert os.path.isdir(path), "expected {} to be a directory".format(path)

    def relpath(self, path: anyio.Path) -> anyio.Path:
        """Return `path` relative to the `root` directory."""
        return path.relative_to(self._root)

    def abspath(self, path: str) -> anyio.Path:
        """Return absolute version of `path` in `root` directory."""
        return self._root.joinpath(path)

    def checksum_path(
        self,
        checksum: str,
    ) -> anyio.Path:
        """Build the file path for a given checksum."""
        paths = self.shard(checksum)

        return self._root.joinpath(*paths)

    async def compute_checksum(self, file: anyio.Path):
        """Compute checksum of file."""

        file_stat = await file.stat()
        blksize = file_stat.st_blksize
        file_size = file_stat.st_size

        if file_size > 1.5 * 1024 * 1024:  # > 1.5 MiB
            # block-aligned size closest to 32MiB, a benchmark sweet-spot
            chunk_size = (32 * 1024 * 1024 // blksize) * blksize
            max_threads = blake3.AUTO
        else:
            chunk_size = blksize
            max_threads = 4

        # TODO: benchmark and tweak accordingly
        hasher = blake3(max_threads=max_threads)
        async with await file.open("rb") as f:
            while True:
                data = await f.read(chunk_size)
                if not data:
                    break
                hasher.update(data)

        return hasher.hexdigest()

    def shard(self, checksum: str) -> list[str]:
        """Shard checksum into subfolders."""
        return shard(checksum, self._prefix_depth, self._prefix_width)

    def unshard(self, path: anyio.Path) -> str:
        """Unshard path to determine checksum."""
        if not self.haspath(path):
            raise ValueError(
                f"Cannot unshard path. The path {path} is not "
                f"a subdirectory of the root directory {self._root}"
            )

        return "".join(path.parts)

    async def corrupted(self) -> AsyncGenerator[tuple[str, StoreEntry], None]:
        """Return generator that yields entries as `(corrupt_path, expected_entry)`
        where `corrupt_path` is the string path of the mis-located file and
        `expected_entry` is the `StoreEntry` of the expected location.
        """
        async for path in self.files():
            checksum = await self.compute_checksum(path)

            expected_path = self.checksum_path(checksum)

            if expected_path != path:
                yield (
                    str(path),
                    StoreEntry(checksum, str(self.relpath(expected_path))),
                )

    def __contains__(self, file: str) -> bool:
        """Return whether a given file checksum or path is contained in the
        `root` directory.
        """
        return self.exists(file)

    def __aiter__(self) -> AsyncGenerator[anyio.Path, None]:
        """Iterate over all files in the `root` directory."""
        return self.files()


async def find_files(path: anyio.Path, recursive: bool = False):
    if recursive:
        async for sub_path in path.glob("**"):
            if sub_path.is_file():
                yield sub_path
    else:
        async for sub_path in path.iterdir():
            if sub_path.is_file():
                yield sub_path




def to_bytes(text: bytes | str):
    if not isinstance(text, bytes):
        text = bytes(text, "utf8")
    return text


@dataclass
class StoreEntry:
    """File address containing file's path on disk and it's content checksum.

    Attributes:
        checksum (str): Hexdigest of file contents.
        path (str): Relative path location to `Store.root`.
        is_duplicate (boolean, optional): Whether the newly returned StoreEntry
        represents a duplicate of an existing file. Can only be `True` after
        a put operation. Defaults to `False`.
    """

    checksum: str
    path: str
    is_duplicate: bool = False


class PutStrategies:
    """Namespace for built-in put strategies.

    Should not be instantiated. Use the `get` static method to look up a
    strategy by name, or directly reference one of the included class methods.
    """

    @classmethod
    def get(
        cls, method: str | None
    ) -> Callable[[Store, anyio.Path, anyio.Path], None] | None:
        """Look up a strategy by name string. You can also pass a function
        which will be returned as is."""
        if method:
            if method == "get":
                raise ValueError("invalid put strategy name, 'get'")
            if callable(method):
                return method
            elif callable(getattr(cls, method)):
                return getattr(cls, method)

    @staticmethod
    async def copy(store: Store, src_path: anyio.Path, dest_path: anyio.Path) -> None:
        """The default copy put strategy, writes the file object to a
        temporary file on disk and then moves it into place."""
        tmp_path = await store.mktempfile(src_path)
        shutil.move(tmp_path, dest_path)

    @classmethod
    async def link(
        cls, store: Store, src_path: anyio.Path, dest_path: anyio.Path
    ) -> None:
        """Use os.link if available to create a hard link to the original
        file if the store and the original file reside on the same
        filesystem and the filesystem supports hard links."""

        if not hasattr(os, "link"):
            return await cls.copy(store, src_path, dest_path)

        # No path available because e.g. a StringIO was used
        if not src_path:
            # Just copy
            return await cls.copy(store, src_path, dest_path)

        try:
            # Try to create the hard link
            os.link(src_path, dest_path)
        except EnvironmentError as e:
            # These are link specific errors. If any of these 3 are raised
            # we try to copy instead
            # EMLINK - src already has the maximum number of links to it
            # EXDEV - invalid cross-device link
            # EPERM - the dst filesystem does not support hard links
            # (note EPERM could also be another permissions error; these
            # will be raised again when we try to copy)
            if e.errno not in (errno.EMLINK, errno.EXDEV, errno.EPERM):
                raise
            return await cls.copy(store, src_path, dest_path)
        else:
            # After creating the hard link, make sure it has the correct
            # file permissions
            os.chmod(dest_path, store.fmode)
