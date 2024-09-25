# -*- coding: utf-8 -*-
# 
# Copyright (c) 2024~2999 - Cologler <skyoflw@gmail.com>
# ----------
# 
# ----------

from pathlib import Path
from typing import Callable


class _Reader:
    def __init__(self, read) -> None:
        self.read = read

    @classmethod
    def wrap_read(cls, read_func: Callable[[int | None], bytes], read_callback: Callable[[int], None]):
        def read(size: int | None):
            if read_bytes := read_func(size):
                read_callback(len(read_bytes))
            return read_bytes
        return cls(read)


def compress_zstd(src_path: str, dest_path: str, progress_callback: Callable[[int], None] | None):
    import zstandard

    compressor = zstandard.ZstdCompressor(
        write_checksum=True
    )

    with open(src_path, 'rb') as src, open(dest_path, 'xb') as dest:
        if progress_callback:
            fileobj = _Reader.wrap_read(src.read, progress_callback)
        else:
            fileobj = _Reader(src.read)
        compressor.copy_stream(fileobj, dest)

def decompress_zstd(src_path: Path, dest_path: Path, progress_callback: Callable[[int], None] | None):
    import zstandard

    decompressor = zstandard.ZstdDecompressor()

    with src_path.open('rb') as src, dest_path.open('xb') as dest:
        if progress_callback:
            fileobj = _Reader.wrap_read(src.read, progress_callback)
        else:
            fileobj = _Reader(src.read)
        decompressor.copy_stream(fileobj, dest)
