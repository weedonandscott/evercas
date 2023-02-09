"""Module for EverCas class.
"""

from __future__ import annotations

import errno
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


class EverCas(object):
    """Content addressable file manager.

    Attributes:
        root (str | os.PathLike[str]): Directory path used as root of storage space.
        prefix_depth (int, optional): Number of prefix folder to create when saving a
            file.
        prefix_width (int, optional): Width of each prefix folder to create when saving
            a file.
        fmode (int, optional): File mode permission to set when adding files to
            directory. It is strongly recommended to keep the default ``0o400`` which
            allows only owner to only read the file, thus avoiding accidental loss of
            data (e.g ``echo oops > file``)
        dmode (int, optional): Directory mode permission to set for
            subdirectories. Defaults to ``0o700`` which allows owner read, write and
            execute.
        put_strategy (mixed, optional): Default ``put_strategy`` for
            :meth:`put` method. See :meth:`put` for more information. Defaults
            to :attr:`PutStrategies.copy`.
    """

    def __init__(
        self,
        root: PathLikeArg,
        prefix_depth: int = 1,
        prefix_width: int = 2,
        fmode: int = 0o400,
        dmode: int = 0o700,
        put_strategy: str | None = None,
    ):
        # use `pathlib` for the sync `resolve()`
        temp_path = pathlib.Path(root)
        if not self.root.is_absolute():
            raise ValueError("Store root must be an absolute path")
        temp_path = temp_path.resolve()
        self.root = anyio.Path(temp_path)

        self.prefix_depth = prefix_depth
        self.prefix_width = prefix_width
        self.fmode = fmode
        self.dmode = dmode
        self.put_strategy = PutStrategies.get(put_strategy) or PutStrategies.copy

    async def put(
        self,
        file: PathLikeArg,
        put_strategy: str | None = None,
        dry_run: bool = False,
    ) -> HashAddress:
        """Store contents of `file` on disk using its content hash for the
        address.

        Args:
            file (mixed): Readable object or path to file.
            put_strategy (mixed, optional): The strategy to use for adding
                files; may be a function or the string name of one of the
                built-in put strategies declared in :class:`PutStrategies`
                class. Defaults to :attr:`PutStrategies.copy`.
            dry_run (bool, optional): Return the :class:`HashAddress` of the
                file that would be appended but don't do anything.

        Put strategies are functions ``(evercas, source_path, dest_path)`` where
        ``evercas`` is the :class:`EverCas` instance from which :meth:`put` was
        called; ``path`` is the :class:`anyio.Path` object representing the
        data to add; and ``dest_path`` is the string absolute file path inside
        the EverCas where it needs to be saved. The put strategy function should
        create the path ``dest_path`` containing the data in ``source_path``.

        There are currently two built-in put strategies: "copy" (the default)
        and "link". "link" attempts to hard link the file into the EverCas if
        the platform and underlying filesystem support it, and falls back to
        "copy" behavior.

        Returns:
            HashAddress: File's hash address.
        """
        source_path = anyio.Path(file)

        checksum = await self.compute_checksum(source_path)
        checksum_path = self.checksum_path(checksum)

        # Only move file if it doesn't already exist.
        if not os.path.isfile(checksum_path):
            is_duplicate = False
            if not dry_run:
                self.makepath(os.path.dirname(checksum_path))
                put_strategy_callable = (
                    PutStrategies.get(put_strategy)
                    or self.put_strategy
                    or PutStrategies.copy
                )
                put_strategy_callable(self, source_path, checksum_path)
        else:
            is_duplicate = True

        return HashAddress(checksum, str(self.relpath(checksum_path)), is_duplicate)

    async def putdir(
        self,
        root: PathLikeArg,
        recursive: bool = False,
        put_strategy: str | None = None,
        dry_run: bool = False,
    ) -> AsyncGenerator[tuple[anyio.Path, HashAddress], None]:
        """Put all files from a directory.

        Args:
            root (str): Path to the directory to add.
            recursive (bool, optional): Find files recursively in ``root``.
                Defaults to ``False``.
            put_strategy (mixed, optional): same as :meth:`put`.
            dry_run (boo, optional): same as :meth:`put`.

        Yields :class:`HashAddress`es for all added files.
        """
        async for file in find_files(anyio.Path(root), recursive=recursive):
            address = await self.put(file, put_strategy=put_strategy, dry_run=dry_run)
            yield (file, address)

    async def mktempfile(self, source_file_path: anyio.Path):
        """Create a named temporary file from a :class:`anyio.Path` object and
        return its filename.
        """
        tmp = NamedTemporaryFile(delete=False)

        oldmask = os.umask(0)

        try:
            os.chmod(tmp.name, self.fmode)
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

    def get(self, checksum: str) -> HashAddress | None:
        """Return :class:`HashAddress` from given checksum or path. If `file` does not
        refer to a valid file, then ``None`` is returned.

        Args:
            file (str): Checksum or path of file.

        Returns:
            HashAddress: File's hash address.
        """

        # Check for sharded path.
        filepath = self.checksum_path(checksum)
        if filepath.is_file():
            return HashAddress(checksum, str(self.relpath(filepath)))

        # Could not determine a match.
        return None

    def open(
        self, checksum: str, mode: Literal["rb"] | Literal["r"] | Literal["rt"] = "rb"
    ):
        """Return open buffer object from given checksum.
        Only `rb`, `r`, `rt` modes allowed.

        Args:
            checksum (str): Checksum of file.

        Returns:
            Buffer: An ``io`` buffer dependent on the `mode`.

        Raises:
            IOError: If file doesn't exist.
            ValueError: If given forbidden mode.
        """
        if mode not in ["rb", "r", "rt"]:
            raise ValueError(f"Forbidden mode {mode}. Only `rb`, `r`, `rt` allowed.")

        address = self.get(checksum)
        if address is None:
            raise IOError(f"Could not locate checksum: {checksum}")

        return self.abspath(address.path).open(mode)

    async def delete(self, checksum: str):
        """Delete file using checksum. Remove any empty directories after deleting.
        No exception is raised if file doesn't exist.

        Args:
            file (str): Checksum or path of file.
        """
        address = self.get(checksum)
        if address is None:
            return None

        await self.abspath(address.path).unlink(missing_ok=True)

        await self.remove_empty(anyio.Path(address.path))

    async def remove_empty(self, subpath: anyio.Path) -> None:
        """Successively remove all empty folders starting with `subpath` and
        proceeding "up" through directory tree until reaching the :attr:`root`
        folder.
        """
        # Don't attempt to remove any folders if subpath is not a
        # subdirectory of the root directory.
        if not self.haspath(subpath):
            return

        normalized_path = await subpath.resolve()

        is_dir = await normalized_path.is_dir()
        is_symlink = await normalized_path.is_symlink()

        if not is_dir or is_symlink:
            return

        async for _ in normalized_path.iterdir():
            # `subpath` not empty
            return

        parent = normalized_path.parent
        await normalized_path.rmdir()
        await self.remove_empty(parent)

    async def files(self):
        """Return generator that yields all files in the :attr:`root`
        directory.
        """
        async for async_path in find_files(self.root, recursive=True):
            yield async_path

    def folders(self):
        """Return generator that yields all folders in the :attr:`root`
        directory that contain files.
        """
        for folder, _, files in os.walk(self.root):
            if files:
                yield folder

    async def count(self):
        """Return count of the number of files in the :attr:`root` directory."""
        count = 0
        async for _ in self:
            count += 1
        return count

    async def size(self):
        """Return the total size in bytes of all files in the :attr:`root`
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
        """Return whether `pathlike` is a subdirectory of the :attr:`root`
        directory.
        """
        return self.root in anyio.Path(pathlike).parents

    def makepath(self, path: str):
        """Physically create the folder path on disk."""
        try:
            os.makedirs(path, self.dmode)
        except FileExistsError:
            assert os.path.isdir(path), "expected {} to be a directory".format(path)

    def relpath(self, path: anyio.Path) -> anyio.Path:
        """Return `path` relative to the :attr:`root` directory."""
        return path.relative_to(self.root)

    def abspath(self, path: str) -> anyio.Path:
        """Return absolute version of `path` in :attr:`root` directory."""
        return self.root.joinpath(path)

    def checksum_path(
        self,
        checksum: str,
    ) -> anyio.Path:
        """Build the file path for a given checksum."""
        paths = self.shard(checksum)

        return self.root.joinpath(*paths)

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
        return shard(checksum, self.prefix_depth, self.prefix_width)

    def unshard(self, path: anyio.Path) -> str:
        """Unshard path to determine checksum."""
        if not self.haspath(path):
            raise ValueError(
                f"Cannot unshard path. The path {path} is not "
                f"a subdirectory of the root directory {self.root}"
            )

        return "".join(path.parts)

    async def repair(self) -> list[tuple[str, HashAddress]]:
        """Repair any file locations whose content address doesn't match it's
        file path.
        """
        repaired: list[tuple[str, HashAddress]] = []
        oldmask = os.umask(0)

        try:
            async for corrupt_path, expected_address in self.corrupted():
                expected_abspath = self.abspath(expected_address.path)
                if os.path.isfile(expected_abspath):
                    # File already exists so just delete corrupted path.
                    os.remove(corrupt_path)
                else:
                    # File doesn't exists so move it.
                    self.makepath(os.path.dirname(expected_abspath))
                    shutil.move(corrupt_path, expected_abspath)

                os.chmod(expected_abspath, self.fmode)
                repaired.append((str(corrupt_path), expected_address))
        finally:
            os.umask(oldmask)

        return repaired

    async def corrupted(self) -> AsyncGenerator[tuple[anyio.Path, HashAddress], None]:
        """Return generator that yields corrupted files as ``(path, address)``
        where ``path`` is the path of the corrupted file and ``address`` is
        the :class:`HashAddress` of the expected location.
        """
        async for path in self.files():
            checksum = await self.compute_checksum(path)

            expected_path = self.checksum_path(checksum)

            if expected_path != path:
                yield (
                    path,
                    HashAddress(checksum, str(self.relpath(expected_path))),
                )

    def __contains__(self, file: str) -> bool:
        """Return whether a given file checksum or path is contained in the
        :attr:`root` directory.
        """
        return self.exists(file)

    def __aiter__(self) -> AsyncGenerator[anyio.Path, None]:
        """Iterate over all files in the :attr:`root` directory."""
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
class HashAddress:
    """File address containing file's path on disk and it's content checksum.

    Attributes:
        checksum (str): Hexdigest of file contents.
        path (str): Relative path location to :attr:`EverCas.root`.
        is_duplicate (boolean, optional): Whether the hash address created was
            a duplicate of a previously existing file. Can only be ``True``
            after a put operation. Defaults to ``False``.
    """

    checksum: str
    path: str
    is_duplicate: bool = False


class PutStrategies:
    """Namespace for built-in put strategies.

    Should not be instantiated. Use the :meth:`get` static method to look up a
    strategy by name, or directly reference one of the included class methods.
    """

    @classmethod
    def get(
        cls, method: str | None
    ) -> Callable[[EverCas, anyio.Path, anyio.Path], None] | None:
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
    async def copy(
        evercas: EverCas, src_path: anyio.Path, dest_path: anyio.Path
    ) -> None:
        """The default copy put strategy, writes the file object to a
        temporary file on disk and then moves it into place."""
        tmp_path = await evercas.mktempfile(src_path)
        shutil.move(tmp_path, dest_path)

    @classmethod
    async def link(
        cls, evercas: EverCas, src_path: anyio.Path, dest_path: anyio.Path
    ) -> None:
        """Use os.link if available to create a hard link to the original
        file if the EverCas and the original file reside on the same
        filesystem and the filesystem supports hard links."""

        if not hasattr(os, "link"):
            return await cls.copy(evercas, src_path, dest_path)

        # No path available because e.g. a StringIO was used
        if not src_path:
            # Just copy
            return await cls.copy(evercas, src_path, dest_path)

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
            return await cls.copy(evercas, src_path, dest_path)
        else:
            # After creating the hard link, make sure it has the correct
            # file permissions
            os.chmod(dest_path, evercas.fmode)
