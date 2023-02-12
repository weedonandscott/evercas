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
-   Uses the performant `blake3` hash with multithreading enabled.
-   Python 3.10+ compatible.
-   Support various put strategies to insert file into the store, each with a different trade-off

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

store = Store('/absolute/path/to/store/root')

# if this store was created before, all done!

# otherwise, initialize it:
store.init()

# if you want to check if a store is initialized, use `is_initialized`:
store.is_initialized # True
```

## Basic Usage

`EverCas` supports basic file storage, retrieval, and removal.

### Storing Content

Add content to the store using its absolute path.

``` python
# Put a single file
entry = await store.put("/some/absolute/path/to/a/file")

# Put all files in a directory tree
#  recursively with recursive = True 
async for src_path, entry in store.putdir("dir"):
    # The hexdigest of the file's contents
    entry.checksum

    # The path relative to store.root.
    entry.path

    # Whether the file previously existed.
    entry.is_duplicate
```

### Retrieving File Address

Get a file\'s `StoreEntry` by checksum. This entry would be
identical to the address returned by `put()`.

``` python
assert store.get(address.checksum) == entry
assert store.get('invalid') is None
```

### Retrieving Content

Get a `BufferedReader` handler for an existing file by checksum

``` python
fileio = store.open(entry.checksum)
```

**NOTE:** When getting a file that was saved with an extension, it\'s
not necessary to supply the extension. Extensions are ignored when
looking for a file based on the ID or path.

### Removing Content

Delete a file by address ID or path.

``` python
await store.delete(entry.checksum)
```

**NOTE:** When a file is deleted, any parent directories above the file
will also be deleted if they are empty directories.

## Advanced Usage

Below are some of the more advanced features of `EverCas`.

### Walking Corrupted Files

If you ever want to migrate a `Store` to a new config, files would not be in sync
with the new `depth` and `width` can be iterated over for custom processing.

``` python
async for corrupted_path, expected_entry in store.corrupted():
    # do something
```

**WARNING:** `EverCas.corrupted()` is a generator so be aware that
modifying the file system while iterating could have unexpected results.

### Walking All Files

Iterate over files.

``` python
for file in store.files():
    # do something

# Or using the class' iter method...
for file in fs:
    # do something
```

### Computing Size

Compute the size in bytes of all files in the `root` directory.

``` python
total_bytes = await store.size()
```

Count the total number of files.

``` python
total_files = await store.count()
```

### Using different put strategies

EverCas supports different put strategies, each with its own pros and cons, for inserting
files into the store. A put strategy can be either set as a default when initializing
the store, or on a per-file basis when using `put`.

``` python
from evercas.put_strategies import PutStrategy

# copy a file instead of moving
new_entry = await store.put("file/path", put_strategy=PutStrategy.COPY)

# this can be set on store init
store.init(default_put_strategy=PutStrategy.COPY)

```

For more details, please see the full documentation at
<https://weedonandscott.github.io/evercas/>.

### Acknowledgements

This software is based on HashFS, made by @dgilland with @x11x contributions, and inspired by parts of dud, by @kevin-hanselman.
