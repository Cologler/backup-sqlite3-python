# -*- coding: utf-8 -*-
# 
# Copyright (c) 2024~2999 - Cologler <skyoflw@gmail.com>
# ----------
# 
# ----------

import io

from backup_sqlite3.compression import compress_zstd, decompress_zstd

def test_compress_zstd():
    content = b'hello world'
    cout = io.BytesIO()
    dout = io.BytesIO()
    compress_zstd(io.BytesIO(content), cout, None)
    cout.seek(0)
    decompress_zstd(cout, dout, None)

    assert content == dout.getvalue()
