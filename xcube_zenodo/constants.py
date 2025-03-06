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

import logging

from xcube.util.jsonschema import JsonArraySchema, JsonBooleanSchema, JsonStringSchema

DATA_STORE_ID = "zenodo"
LOG = logging.getLogger("xcube.zenodo")
CACHE_FOLDER_NAME = "zenodo_cache"

# preload specific constants
COMPRESSED_FORMATS = ["zip", "tar", "tar.gz"]
DOWNLOAD_FOLDER = "downloads"
PRELOAD_DOWNLOAD_FRACTION = 0.4
PRELOAD_DECOMPRESSION_FRACTION = 0.1
PRELOAD_PROCESSING_FRACTION = 0.5

# parameter schemas
ZENODO_OPEN_SCHEMA = dict(
    file_names=JsonArraySchema(
        title="List of files name as given on the webpage of a given Zenodo record",
        items=JsonStringSchema(title="File name in a Zenodo record"),
    ),
    stack_along_time=JsonBooleanSchema(
        title="Switch to stack multiple files along a time axis", default=False
    ),
    time_axis=JsonArraySchema(
        title="Time axis to be assigned when `stack_along_time=True`", default=None
    ),
)
