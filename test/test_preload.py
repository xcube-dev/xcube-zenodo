# The MIT License (MIT)
# Copyright (c) 2024-2025 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import unittest

import fsspec
import numpy as np
import xarray as xr
from xcube.core.store import new_data_store

from xcube_zenodo.preload import recursive_listdir


class ZenodoDataStoreTest(unittest.TestCase):

    def test_recursive_listdir(self):
        data = np.ones((4, 4))
        da = xr.DataArray(data, dims=("x", "y")).chunk({"x": 2, "y": 2})
        ds = xr.Dataset({"random_data": da})
        root = "datasource"
        memory_store = new_data_store("memory", root=root)
        memory_store.write_data(ds, "level1/level2/test.nc")
        memory_store.write_data(ds, "test.zarr")
        fs = fsspec.filesystem("memory")
        with fs.open("memory://example.txt", "w") as f:
            f.write("Hello, this is a test file in memory!")
        expected = [
            {
                "name": "/datasource/level1/level2/test.nc",
                "size": 8320,
                "type": "file",
            },
            {"name": "/datasource/test.zarr", "size": 0, "type": "directory"},
        ]
        files_info = recursive_listdir(fs, root)
        for file_info in files_info:
            if "created" in file_info:
                file_info.pop("created")
        self.assertCountEqual(expected, files_info)
