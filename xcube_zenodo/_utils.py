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

from typing import Any, Container

from xcube.core.store import DATASET_TYPE
from xcube.core.store import MULTI_LEVEL_DATASET_TYPE
from xcube.core.store import DataStoreError
from xcube.core.store import DataTypeLike


from .constants import MAP_FILE_EXTENSION_FORMAT


def get_attrs_from_record(
    record: dict, zenodo_file: dict, include_attrs: Container[str]
) -> dict[str, Any]:
    """Extracts the desired attributes from a Zenodo record object.

    Args:
        record: dict containing meta-data zenodo record.
        zenodo_file: entry representing one file in a record.
        include_attrs: A sequence of names of attributes to be returned
            for each dataset identifier. If given, the store will attempt
            to provide the set of requested dataset attributes in addition
            to the data ids. If no attributes are found, empty dictionary
            is returned.

    Returns:
        dictionary containing the attributes defined by *include_attrs*
        of data resources provided by this data store
    """
    attrs = {}
    supported_record_keys = [
        "created",
        "modified",
        "id",
        "conceptrecid",
        "doi",
        "conceptdoi",
        "doi_url",
        "metadata",
        "title",
        "links",
        "updated",
        "recid",
        "revision",
        "owners",
        "status",
        "stats",
        "state",
        "submitted",
    ]
    for key in supported_record_keys:
        if key in include_attrs and key in record:
            attrs[key] = record[key]
    supported_file_keys = [
        "file_id",
        "file_key",
        "file_size",
        "file_checksum",
        "file_links",
    ]
    for key in supported_file_keys:
        key_mod = key.replace("file_", "")
        if key in include_attrs and key_mod in zenodo_file:
            attrs[key] = zenodo_file[key_mod]
    return attrs


def is_valid_data_type(data_type: DataTypeLike) -> bool:
    """Auxiliary function to check if data type is supported
    by the store.

    Args:
        data_type: Data type that is to be checked.

    Returns:
        True if *data_type* is supported by the store, otherwise False
    """
    return (
        data_type is None
        or DATASET_TYPE.is_super_type_of(data_type)
        or MULTI_LEVEL_DATASET_TYPE.is_super_type_of(data_type)
    )


def is_valid_dataset_type(data_type: DataTypeLike) -> bool:
    """Auxiliary function to check if data type is a valid dataset type.

    Args:
        data_type: Data type that is to be checked.

    Returns:
        True if *data_type* is a valid dataset type, otherwise False
    """
    return data_type is None or DATASET_TYPE.is_super_type_of(data_type)


def estimate_file_format(data_id: str) -> str:
    ext = data_id.split(".")[-1]
    format_id = MAP_FILE_EXTENSION_FORMAT.get(ext.lower())
    return format_id


def is_supported_file_format(data_id: str) -> bool:
    return estimate_file_format(data_id) is not None


def translate_data_id2uri(data_id: str) -> str:
    components = data_id.split("/")
    record_id = components[0]
    file_key = "/".join(components[1:])
    return f"records/{record_id}/files/{file_key}"
