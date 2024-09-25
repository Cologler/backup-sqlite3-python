# -*- coding: utf-8 -*-
# 
# Copyright (c) 2024~2999 - Cologler <skyoflw@gmail.com>
# ----------
# 
# ----------

from typing import Callable, Protocol


class _BytesReader(Protocol):
    def read(self, size: int | None) -> bytes:
        ...


class _BytesWriter(Protocol):
    def write(self, b: bytes) -> int:
        ...


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


def compress_zstd(src: _BytesReader, dest: _BytesWriter, progress_callback: Callable[[int], None] | None):
    import zstandard

    compressor = zstandard.ZstdCompressor(
        write_checksum=True
    )

    if progress_callback:
        fileobj = _Reader.wrap_read(src.read, progress_callback)
    else:
        fileobj = _Reader(src.read)

    compressor.copy_stream(fileobj, dest)

def decompress_zstd(src: _BytesReader, dest: _BytesWriter, progress_callback: Callable[[int], None] | None):
    import zstandard

    decompressor = zstandard.ZstdDecompressor()

    if progress_callback:
        fileobj = _Reader.wrap_read(src.read, progress_callback)
    else:
        fileobj = _Reader(src.read)

    decompressor.copy_stream(fileobj, dest)
