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

from fsspec.implementations.memory import MemoryFileSystem

from xcube_zenodo._utils import (
    identify_compressed_file_format,
    is_supported_compressed_file_format,
    is_zarr_directory,
    translate_data_id2uri,
)


class UtilsTest(unittest.TestCase):

    def test_identify_compressed_file_format(self):
        self.assertIsNone(identify_compressed_file_format("1234567/test.tif"))
        self.assertEqual("zip", identify_compressed_file_format("1234567/test.zip"))
        self.assertEqual("tar", identify_compressed_file_format("1234567/test.tar"))
        self.assertEqual(
            "tar.gz", identify_compressed_file_format("1234567/test.tar.gz")
        )

    def test_is_supported_compressed_file_format(self):
        self.assertTrue(is_supported_compressed_file_format("1234567/test.zip"))
        self.assertTrue(is_supported_compressed_file_format("1234567/test.tar"))
        self.assertTrue(is_supported_compressed_file_format("1234567/test.tar.gz"))
        self.assertFalse(is_supported_compressed_file_format("1234567/test.zarr"))

    def test_translate_data_id2uri(self):
        test_uri = translate_data_id2uri("1234567/test.tif")
        self.assertEqual("https://zenodo.org/records/1234567/files/test.tif", test_uri)

    def test_is_zarr_directory(self):
        self.fs = MemoryFileSystem()
        self.valid_zarr_path = "valid_zarr"
        self.invalid_zarr_path = "invalid_zarr"
        self.empty_path = "empty"

        # Create a valid Zarr directory
        self.fs.mkdir(self.valid_zarr_path)
        self.fs.touch(f"{self.valid_zarr_path}/.zgroup")

        # Create an invalid directory (no Zarr metadata files)
        self.fs.mkdir(self.invalid_zarr_path)
        self.fs.touch(f"{self.invalid_zarr_path}/random_file.txt")

        # Create an empty directory
        self.fs.mkdir(self.empty_path)

        self.assertTrue(is_zarr_directory(self.valid_zarr_path, self.fs))
        self.assertFalse(is_zarr_directory(self.invalid_zarr_path, self.fs))
        self.assertFalse(is_zarr_directory(self.empty_path, self.fs))
        self.assertFalse(is_zarr_directory("non_existent", self.fs))
