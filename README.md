# EverCas

EverCas is a content-addressable file management system. What does that
mean? Simply, that EverCas manages a directory where files are saved
based on the file\'s hash.

Typical use cases for this kind of system are ones where:

-   Files are written once and never change (e.g. image storage).
-   It\'s desirable to have no duplicate files (e.g. user uploads).
-   File metadata is stored elsewhere (e.g. in a database).

## Features

-   Files are stored once and never duplicated.
-   Uses an efficient folder structure optimized for a large number of
    files. File paths are based on the content hash and are nested based
    on the first `n` number of characters.
-   Can save files from local file paths or readable objects (open file
    handlers, IO buffers, etc).
-   Pluggable put strategies, allowing fine-grained control of how files
    are added.
-   Able to repair the root folder by reindexing all files. Useful if
    the hashing algorithm or folder structure options change or to
    initialize existing files.
-   Supports any hashing algorithm available via `hashlib.new`.
-   Python 3.10+ compatible.
-   Support for hard-linking files into the EverCas-managed directory on
    compatible filesystems

## Links

-   Project: <https://github.com/weedonandscott/evercas>
-   Documentation: <https://weedonandscott.github.io/evercas/>
-   PyPI: <https://pypi.python.org/pypi/evercas/>

## Quickstart

Install using pip:

    pip install evercas

### Initialization

``` python
from evercas import EverCas
```

Designate a root folder for `EverCas`. If the folder doesn\'t already
exist, it will be created.

``` python
# Set the `depth` to the number of subfolders the file's hash should be split when saving.
# Set the `width` to the desired width of each subfolder.
fs = EverCas('temp_evercas', depth=4, width=1, algorithm='sha256')

# With depth=4 and width=1, files will be saved in the following pattern:
# temp_evercas/a/b/c/d/efghijklmnopqrstuvwxyz

# With depth=3 and width=2, files will be saved in the following pattern:
# temp_evercas/ab/cd/ef/ghijklmnopqrstuvwxyz
```

**NOTE:** The `algorithm` value should be a valid string argument to
`hashlib.new()`.

## Basic Usage

`EverCas` supports basic file storage, retrieval, and removal as well as
some more advanced features like file repair.

### Storing Content

Add content to the folder using either readable objects (e.g.
`StringIO`) or file paths (e.g. `'a/path/to/some/file'`).

``` python
from io import StringIO

some_content = StringIO('some content')

address = fs.put(some_content)

# Or if you'd like to save the file with an extension...
address = fs.put(some_content, '.txt')

# Put all files in a directory
for srcpath, address in fs.putdir("dir"):
    #...

# Put all files in a directory tree recursively
for srcpath, address in fs.putdir("dir", recursive=True):
    #...

# Put all files in a directory tree using same extensions
for srcpath, address in fs.putdir("dir", extensions=True):
    # address.abspath will have same file extension as srcpath

# The id of the file (i.e. the hexdigest of its contents).
address.id

# The absolute path where the file was saved.
address.abspath

# The path relative to fs.root.
address.relpath

# Whether the file previously existed.
address.is_duplicate
```

### Retrieving File Address

Get a file\'s `HashAddress` by address ID or path. This address would be
identical to the address returned by `put()`.

``` python
assert fs.get(address.id) == address
assert fs.get(address.relpath) == address
assert fs.get(address.abspath) == address
assert fs.get('invalid') is None
```

### Retrieving Content

Get a `BufferedReader` handler for an existing file by address ID or
path.

``` python
fileio = fs.open(address.id)

# Or using the full path...
fileio = fs.open(address.abspath)

# Or using a path relative to fs.root
fileio = fs.open(address.relpath)
```

**NOTE:** When getting a file that was saved with an extension, it\'s
not necessary to supply the extension. Extensions are ignored when
looking for a file based on the ID or path.

### Removing Content

Delete a file by address ID or path.

``` python
fs.delete(address.id)
fs.delete(address.abspath)
fs.delete(address.relpath)
```

**NOTE:** When a file is deleted, any parent directories above the file
will also be deleted if they are empty directories.

## Advanced Usage

Below are some of the more advanced features of `EverCas`.

### Repairing Files

The `EverCas` files may not always be in sync with it\'s `depth`,
`width`, or `algorithm` settings (e.g. if `EverCas` takes ownership of a
directory that wasn\'t previously stored using content hashes or if the
`EverCas` settings change). These files can be easily reindexed using
`repair()`.

``` python
repaired = fs.repair()

# Or if you want to drop file extensions...
repaired = fs.repair(extensions=False)
```

**WARNING:** It\'s recommended that a backup of the directory be made
before repairing just in case something goes wrong.

### Walking Corrupted Files

Instead of actually repairing the files, you can iterate over them for
custom processing.

``` python
for corrupted_path, expected_address in fs.corrupted():
    # do something
```

**WARNING:** `EverCas.corrupted()` is a generator so be aware that
modifying the file system while iterating could have unexpected results.

### Walking All Files

Iterate over files.

``` python
for file in fs.files():
    # do something

# Or using the class' iter method...
for file in fs:
    # do something
```

Iterate over folders that contain files (i.e. ignore the nested
subfolders that only contain folders).

``` python
for folder in fs.folders():
    # do something
```

### Computing Size

Compute the size in bytes of all files in the `root` directory.

``` python
total_bytes = fs.size()
```

Count the total number of files.

``` python
total_files = fs.count()

# Or via len()...
total_files = len(fs)
```

### Hard-linking files

You can use the built-in \"link\" put strategy to hard-link files into
the EverCas directory if the platform and filesystem support it. This
will automatically and silently fall back to copying if a hard-link
can\'t be made, e.g. because the source is on a different device, the
EverCas directory is on a filesystem that does not support hard links or
the source file already has the operating system\'s maximum allowed
number of hard links to it.

``` python
newpath = fs.put("file/path", put_strategy="link").abspath
assert os.path.samefile("file/path", newpath)
```

### Custom Put Strategy

Fine-grained control over how each file or file-like object is stored in
the underlying filesytem.

``` python
# Implement your own put strategy
def my_put_strategy(evercas, src_stream, dst_path):
    # src_stream is the source data to insert
    # it is a EverCas.Stream object, which is a Python file-like object
    # Stream objects also expose the filesystem path of the underlying
    # file via the src_stream.name property

    # dst_path is the path generated by EverCas, based on the hash of the
    # source data

    # src_stream.name will be None if there is not an underlying file path
    # available (e.g. a StringIO was passed or some other non-file
    # file-like)
    # Its recommended to check name property is available before using
    if src_stream.name:
        # Example: rename files instead of copying
        # (be careful with underlying file paths, make sure to test your
        # implementation before using it).
        os.rename(src_stream.name, dst_path)
        # You can also access properties and methods of the EverCas instance
        # using the evercas parameter
        os.chmod(dst_path, EverCas.fmode)
    else:
        # The default put strategy is available for use as
        # PutStrategies.copy
        # You can manually call other strategies if you want fallbacks
        # (recommended)
        PutStrategies.copy(EverCas, src_stream, dst_path)

# And use it like:
fs.put("myfile", put_strategy=my_put_strategy)
```

For more details, please see the full documentation at
<https://weedonandscott.github.io/evercas/>.

### Acknowledgements

This software is based on HashFS, made by @dgilland with @x11x contributions, and inspired by parts of dud, by @kevin-hanselman.
