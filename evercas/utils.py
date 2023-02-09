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


def shard(digest: str, prefix_depth: int, prefix_width: int):
    # This creates a list of `prefix_depth` number of tokens with width
    # `prefix_width` from the first part of the checksum plus the remainder.
    return compact(
        [digest[i * prefix_width : prefix_width * (i + 1)] for i in range(prefix_depth)]
        + [digest[prefix_depth * prefix_width :]]
    )
