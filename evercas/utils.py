# -*- coding: utf-8 -*-

import os
from typing import Any


def compact(items: list[Any]):
    """Return only truthy elements of `items`."""
    return [item for item in items if item]


def issubdir(subpath: str, path: str):
    """Return whether `subpath` is a sub-directory of `path`."""
    # Append os.sep so that paths like /usr/var2/log doesn't match /usr/var.
    path = os.path.realpath(path) + os.sep
    subpath = os.path.realpath(subpath)
    return subpath.startswith(path)


def shard(digest: str, depth: int, width: int):
    # This creates a list of `depth` number of tokens with width
    # `width` from the first part of the id plus the remainder.
    return compact(
        [digest[i * width : width * (i + 1)] for i in range(depth)]
        + [digest[depth * width :]]
    )
