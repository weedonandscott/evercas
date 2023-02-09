"""Module for EverCas class.
"""

from __future__ import annotations

import errno
import glob
import io
import os
import shutil
from contextlib import closing
from dataclasses import dataclass
from tempfile import NamedTemporaryFile
from typing import BinaryIO, Callable

from blake3 import blake3

from .utils import issubdir, shard


class EverCas(object):
    """Content addressable file manager.

    Attributes:
        root (str): Directory path used as root of storage space.
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
        lowercase_extensions (bool, optional): Normalize all file extensions
            to lower case when adding files. Defaults to ``False``.
    """

    def __init__(
        self,
        root: str,
        prefix_depth: int = 1,
        prefix_width: int = 2,
        fmode: int = 0o400,
        dmode: int = 0o700,
        put_strategy: str | None = None,
        lowercase_extensions: bool = False,
    ):
        self.root = os.path.realpath(root)
        self.prefix_depth = prefix_depth
        self.prefix_width = prefix_width
        self.fmode = fmode
        self.dmode = dmode
        self.put_strategy = PutStrategies.get(put_strategy) or PutStrategies.copy
        self.lowercase_extensions = lowercase_extensions

    def put(
        self,
        file: BinaryIO | str,
        extension: str | None = None,
        put_strategy: str | None = None,
        simulate: bool = False,
    ):
        """Store contents of `file` on disk using its content hash for the
        address.

        Args:
            file (mixed): Readable object or path to file.
            extension (str, optional): Optional extension to append to file
                when saving.
            put_strategy (mixed, optional): The strategy to use for adding
                files; may be a function or the string name of one of the
                built-in put strategies declared in :class:`PutStrategies`
                class. Defaults to :attr:`PutStrategies.copy`.
            simulate (bool, optional): Return the :class:`HashAddress` of the
                file that would be appended but don't do anything.

        Put strategies are functions ``(evercas, stream, filepath)`` where
        ``evercas`` is the :class:`EverCas` instance from which :meth:`put` was
        called; ``stream`` is the :class:`Stream` object representing the
        data to add; and ``filepath`` is the string absolute file path inside
        the EverCas where it needs to be saved. The put strategy function should
        create the path ``filepath`` containing the data in ``stream``.

        There are currently two built-in put strategies: "copy" (the default)
        and "link". "link" attempts to hard link the file into the EverCas if
        the platform and underlying filesystem support it, and falls back to
        "copy" behavior.

        Returns:
            HashAddress: File's hash address.
        """
        stream = Stream(file)

        if extension and self.lowercase_extensions:
            extension = extension.lower()

        with closing(stream):
            id = self.computehash(stream)
            filepath = self.idpath(id, extension)

            # Only move file if it doesn't already exist.
            if not os.path.isfile(filepath):
                is_duplicate = False
                if not simulate:
                    self.makepath(os.path.dirname(filepath))
                    put_strategy_callable = (
                        PutStrategies.get(put_strategy)
                        or self.put_strategy
                        or PutStrategies.copy
                    )
                    put_strategy_callable(self, stream, filepath)
            else:
                is_duplicate = True

        return HashAddress(id, self.relpath(filepath), filepath, is_duplicate)

    def putdir(
        self,
        root: str,
        extensions: bool = True,
        recursive: bool = False,
        put_strategy: str | None = None,
        simulate: bool = False,
    ):
        """Put all files from a directory.

        Args:
            root (str): Path to the directory to add.
            extensions (bool, optional): Whether to add extensions when
                saving (extension will be taken from input file). Defaults to
                ``True``.
            recursive (bool, optional): Find files recursively in ``root``.
                Defaults to ``False``.
            put_strategy (mixed, optional): same as :meth:`put`.
            simulate (boo, optional): same as :meth:`put`.

        Yields :class:`HashAddress`es for all added files.
        """
        for file in find_files(root, recursive=recursive):
            extension = os.path.splitext(file)[1] if extensions else None
            address = self.put(
                file, extension=extension, put_strategy=put_strategy, simulate=simulate
            )
            yield (file, address)

    def mktempfile(self, stream: Stream):
        """Create a named temporary file from a :class:`Stream` object and
        return its filename.
        """
        tmp = NamedTemporaryFile(delete=False)

        oldmask = os.umask(0)

        try:
            os.chmod(tmp.name, self.fmode)
        finally:
            os.umask(oldmask)

        for data in stream:
            tmp.write(to_bytes(data))

        tmp.close()

        return tmp.name

    def get(self, file: str):
        """Return :class:`HashAddress` from given id or path. If `file` does not
        refer to a valid file, then ``None`` is returned.

        Args:
            file (str): Address ID or path of file.

        Returns:
            HashAddress: File's hash address.
        """
        realpath = self.realpath(file)

        if realpath is None:
            return None
        else:
            return HashAddress(self.unshard(realpath), self.relpath(realpath), realpath)

    def open(self, file: str, mode: str = "rb"):
        """Return open buffer object from given id or path.

        Args:
            file (str): Address ID or path of file.
            mode (str, optional): Mode to open file in. Defaults to ``'rb'``.

        Returns:
            Buffer: An ``io`` buffer dependent on the `mode`.

        Raises:
            IOError: If file doesn't exist.
        """
        realpath = self.realpath(file)
        if realpath is None:
            raise IOError("Could not locate file: {0}".format(file))

        return io.open(realpath, mode)

    def delete(self, file: str):
        """Delete file using id or path. Remove any empty directories after
        deleting. No exception is raised if file doesn't exist.

        Args:
            file (str): Address ID or path of file.
        """
        realpath = self.realpath(file)
        if realpath is None:
            return

        try:
            os.remove(realpath)
        except OSError:  # pragma: no cover
            pass
        else:
            self.remove_empty(os.path.dirname(realpath))

    def remove_empty(self, subpath: str):
        """Successively remove all empty folders starting with `subpath` and
        proceeding "up" through directory tree until reaching the :attr:`root`
        folder.
        """
        # Don't attempt to remove any folders if subpath is not a
        # subdirectory of the root directory.
        if not self.haspath(subpath):
            return

        while subpath != self.root:
            if len(os.listdir(subpath)) > 0 or os.path.islink(subpath):
                break
            os.rmdir(subpath)
            subpath = os.path.dirname(subpath)

    def files(self):
        """Return generator that yields all files in the :attr:`root`
        directory.
        """
        for file in find_files(self.root, recursive=True):
            yield os.path.abspath(file)

    def folders(self):
        """Return generator that yields all folders in the :attr:`root`
        directory that contain files.
        """
        for folder, _, files in os.walk(self.root):
            if files:
                yield folder

    def count(self):
        """Return count of the number of files in the :attr:`root` directory."""
        count = 0
        for _ in self:
            count += 1
        return count

    def size(self):
        """Return the total size in bytes of all files in the :attr:`root`
        directory.
        """
        total = 0

        for path in self.files():
            total += os.path.getsize(path)

        return total

    def exists(self, file: str):
        """Check whether a given file id or path exists on disk."""
        return bool(self.realpath(file))

    def haspath(self, path: str):
        """Return whether `path` is a subdirectory of the :attr:`root`
        directory.
        """
        return issubdir(path, self.root)

    def makepath(self, path: str):
        """Physically create the folder path on disk."""
        try:
            os.makedirs(path, self.dmode)
        except FileExistsError:
            assert os.path.isdir(path), "expected {} to be a directory".format(path)

    def relpath(self, path: str):
        """Return `path` relative to the :attr:`root` directory."""
        return os.path.relpath(path, self.root)

    def realpath(self, file: str):
        """Attempt to determine the real path of a file id or path through
        successive checking of candidate paths. If the real path is stored with
        an extension, the path is considered a match if the basename matches
        the expected file path of the id.
        """

        # Check for absolute path.
        if os.path.isfile(file):
            return file

        # Check for relative path.
        relpath = os.path.join(self.root, file)
        if os.path.isfile(relpath):
            return relpath

        # Check for sharded path.
        filepath = self.idpath(file)
        if os.path.isfile(filepath):
            return filepath

        # Check for sharded path with any extension.
        paths = glob.glob("{0}.*".format(filepath))
        if paths:
            return paths[0]

        # Could not determine a match.
        return None

    def idpath(self, id: str, extension: str | None = ""):
        """Build the file path for a given hash id. Optionally, append a
        file extension.
        """
        paths = self.shard(id)

        if extension and not extension.startswith(os.extsep):
            extension = os.extsep + extension
        elif not extension:
            extension = ""

        return os.path.join(self.root, *paths) + extension

    def computehash(self, stream: Stream):
        """Compute hash of file."""
        # TODO: benchmark and tweak accordingly
        hasher = blake3(max_threads=blake3.AUTO)
        for data in stream:
            hasher.update(to_bytes(data))
        return hasher.hexdigest()

    def shard(self, id: str):
        """Shard content ID into subfolders."""
        return shard(id, self.prefix_depth, self.prefix_width)

    def unshard(self, path: str):
        """Unshard path to determine hash value."""
        if not self.haspath(path):
            raise ValueError(
                "Cannot unshard path. The path {0!r} is not "
                "a subdirectory of the root directory {1!r}".format(path, self.root)
            )

        return os.path.splitext(self.relpath(path))[0].replace(os.sep, "")

    def repair(self, extensions: bool = True):
        """Repair any file locations whose content address doesn't match it's
        file path.
        """
        repaired: list[tuple[str, HashAddress]] = []
        corrupted = tuple(self.corrupted(extensions=extensions))
        oldmask = os.umask(0)

        try:
            for path, address in corrupted:
                if os.path.isfile(address.abspath):
                    # File already exists so just delete corrupted path.
                    os.remove(path)
                else:
                    # File doesn't exists so move it.
                    self.makepath(os.path.dirname(address.abspath))
                    shutil.move(path, address.abspath)

                os.chmod(address.abspath, self.fmode)
                repaired.append((path, address))
        finally:
            os.umask(oldmask)

        return repaired

    def corrupted(self, extensions: bool = True):
        """Return generator that yields corrupted files as ``(path, address)``
        where ``path`` is the path of the corrupted file and ``address`` is
        the :class:`HashAddress` of the expected location.
        """
        for path in self.files():
            stream = Stream(path)

            with closing(stream):
                id = self.computehash(stream)

            extension = os.path.splitext(path)[1] if extensions else None
            expected_path = self.idpath(id, extension)

            if expected_path != path:
                yield (
                    path,
                    HashAddress(id, self.relpath(expected_path), expected_path),
                )

    def __contains__(self, file: str):
        """Return whether a given file id or path is contained in the
        :attr:`root` directory.
        """
        return self.exists(file)

    def __iter__(self):
        """Iterate over all files in the :attr:`root` directory."""
        return self.files()

    def __len__(self):
        """Return count of the number of files in the :attr:`root` directory."""
        return self.count()


def find_files(path: str, recursive: bool = False):
    if recursive:
        for folder, _, files in os.walk(path):
            for file in files:
                yield os.path.join(folder, file)
    else:
        for file in list_dir_files(path):
            yield file


def list_dir_files(path: str):
    it = os.scandir(path)
    try:
        for file in it:
            if file.is_file():
                yield file.path
    finally:
        try:
            it.close()
        except AttributeError:
            pass


def to_bytes(text: bytes | str):
    if not isinstance(text, bytes):
        text = bytes(text, "utf8")
    return text


@dataclass
class HashAddress:
    """File address containing file's path on disk and it's content hash ID.

    Attributes:
        id (str): Hash ID (hexdigest) of file contents.
        relpath (str): Relative path location to :attr:`EverCas.root`.
        abspath (str): Absolute path location of file on disk.
        is_duplicate (boolean, optional): Whether the hash address created was
            a duplicate of a previously existing file. Can only be ``True``
            after a put operation. Defaults to ``False``.
    """

    id: str
    relpath: str
    abspath: str
    is_duplicate: bool = False


class Stream(object):
    """Common interface for file-like objects.

    The input `obj` can be a file-like object or a path to a file. If `obj` is
    a path to a file, then it will be opened until :meth:`close` is called.
    If `obj` is a file-like object, then it's original position will be
    restored when :meth:`close` is called instead of closing the object
    automatically. Closing of the stream is deferred to whatever process passed
    the stream in.

    Successive readings of the stream is supported without having to manually
    set it's position back to ``0``.
    """

    def __init__(self, obj: BinaryIO | str):
        if isinstance(obj, str) and os.path.isfile(obj):
            obj = io.open(obj, "rb")
            pos = None
        elif isinstance(obj, BinaryIO):
            pos = obj.tell()
        else:
            raise ValueError("Object must be a valid file path or a BinaryIO object")

        try:
            file_stat = os.stat(obj.name)
            buffer_size = file_stat.st_blksize
        except Exception:
            buffer_size = 8192

        try:
            # Expose the original file path if available.
            # This allows put strategies to use OS functions, working with
            # paths, instead of being limited to the API provided by Python
            # file-like objects
            # name property can also hold int fd, so we make it None in that
            # case
            self.name: str | None = None if isinstance(obj.name, int) else obj.name
        except AttributeError:
            self.name = None

        self._obj = obj
        self._pos = pos
        self._buffer_size = buffer_size

    def __iter__(self):
        """Read underlying IO object and yield results. Return object to
        original position if we didn't open it originally.
        """
        self._obj.seek(0)

        while True:
            data = self._obj.read(self._buffer_size)

            if not data:
                break

            yield data

        if self._pos is not None:
            self._obj.seek(self._pos)

    def close(self):
        """Close underlying IO object if we opened it, else return it to
        original position.
        """
        if self._pos is None:
            self._obj.close()
        else:
            self._obj.seek(self._pos)


class PutStrategies:
    """Namespace for built-in put strategies.

    Should not be instantiated. Use the :meth:`get` static method to look up a
    strategy by name, or directly reference one of the included class methods.
    """

    @classmethod
    def get(cls, method: str | None) -> Callable[[EverCas, Stream, str], None] | None:
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
    def copy(evercas: EverCas, src_stream: Stream, dst_path: str) -> None:
        """The default copy put strategy, writes the file object to a
        temporary file on disk and then moves it into place."""
        shutil.move(evercas.mktempfile(src_stream), dst_path)

    @classmethod
    def link(cls, evercas: EverCas, src_stream: Stream, dst_path: str) -> None:
        """Use os.link if available to create a hard link to the original
        file if the EverCas and the original file reside on the same
        filesystem and the filesystem supports hard links."""

        if not hasattr(os, "link"):
            return PutStrategies.copy(evercas, src_stream, dst_path)

        # Get the original file path exposed by the Stream instance
        src_path = src_stream.name
        # No path available because e.g. a StringIO was used
        if not src_path:
            # Just copy
            return cls.copy(evercas, src_stream, dst_path)

        try:
            # Try to create the hard link
            os.link(src_path, dst_path)
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
            return cls.copy(evercas, src_stream, dst_path)
        else:
            # After creating the hard link, make sure it has the correct
            # file permissions
            os.chmod(dst_path, evercas.fmode)
