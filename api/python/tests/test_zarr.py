#!/usr/bin/env python3

import pytest
import numpy as np
import acquire_zarr
from time import sleep

data = np.arange(64, 64, dtype=np.uint8)

def check_zarr(v3: bool, uri: str) -> None:
    zarr = acquire_zarr.AcquireZarrWriter()
    
    zarr.use_v3 = v3
    zarr.uri = uri
    
    zarr.dimensions = ["x", "y", "t"]
    assert zarr.dimensions == ["x", "y", "t"]
    
    zarr.dimension_sizes = [64, 64, 0]
    assert zarr.dimension_sizes == [64, 64, 0]
    
    zarr.chunk_sizes = [64, 64, 1]
    zarr.chunk_sizes[-1] = 1
    assert zarr.chunk_sizes == [64, 64, 1]
    
    zarr.compression_codec = acquire_zarr.CompressionCodec.COMPRESSION_NONE
    assert zarr.compression_codec == acquire_zarr.CompressionCodec.COMPRESSION_NONE
    
    #zarr.compression_codec = acquire_zarr.CompressionCodec.COMPRESSION_BLOSC_ZSTD
    #assert zarr.compression_codec == acquire_zarr.CompressionCodec.COMPRESSION_BLOSC_ZSTD
    
    zarr.compression_level = 5
    assert zarr.compression_level == 5
    zarr.compression_shuffle = 0
    assert zarr.compression_shuffle == 0
    
    sleep(1)
    zarr.start()
    for i in range(3):
        zarr.append(data)
        sleep(0.5)
        

def test_zarr_v2() -> None:
    check_zarr(False, "test_v2.zarr")
    
def test_zarr_v3() -> None:
    check_zarr(True, "test_v3.zarr")
