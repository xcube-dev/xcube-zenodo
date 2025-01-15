# The MIT License (MIT)
# Copyright (c) 2024 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
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

from typing import Optional

from .constants import COMPRESSED_FORMATS
from .constants import MAP_FILE_EXTENSION_FORMAT


def identify_file_format(data_id: str) -> Optional[str]:
    for key, val in MAP_FILE_EXTENSION_FORMAT.items():
        if data_id.endswith(key.lower()):
            return val
    return None


def is_supported_compressed_file_format(data_id: str) -> bool:
    return identify_file_format(data_id) in COMPRESSED_FORMATS


def translate_data_id2fs_path(data_id: str) -> str:
    components = data_id.split("/")
    record_id = components[0]
    file_key = "/".join(components[1:])
    return f"records/{record_id}/files/{file_key}"


def translate_data_id2uri(data_id: str) -> str:
    return f"https://zenodo.org/{translate_data_id2fs_path(data_id)}"
