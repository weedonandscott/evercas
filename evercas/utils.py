# -*- coding: utf-8 -*-

from typing import Any, AsyncGenerator

import anyio


def compact(items: list[Any]):
    """Return only truthy elements of `items`."""
    return [item for item in items if item]


def shard(checksum: str, prefix_depth: int, prefix_width: int) -> list[str]:
    # This creates a list of `prefix_depth` number of tokens with width
    # `prefix_width` from the first part of the checksum plus the remainder.
    if len(checksum) <= prefix_depth * prefix_width:
        raise ValueError("checksum must be larger prefix_depth * prefix_width")

    return compact(
        [
            checksum[i * prefix_width : prefix_width * (i + 1)]
            for i in range(prefix_depth)
        ]
        + [checksum[prefix_depth * prefix_width :]]
    )


async def find_files(
    path: anyio.Path, recursive: bool = False
) -> AsyncGenerator[anyio.Path, None]:
    if recursive:
        async for sub_path in path.glob("**"):
            if sub_path.is_file():
                yield sub_path
    else:
        async for sub_path in path.iterdir():
            if sub_path.is_file():
                yield sub_path
