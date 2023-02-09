# -*- coding: utf-8 -*-
"""EverCas is a content-addressable file management system. What does that mean?
Simply, that EverCas manages a directory where files are saved based on the
file's hash.

Typical use cases for this kind of system are ones where:

- Files are written once and never change (e.g. image storage).
- It's desirable to have no duplicate files (e.g. user uploads).
- File metadata is stored elsewhere (e.g. in a database).
"""

from .evercas import EverCas, HashAddress

__all__ = ("EverCas", "HashAddress")
