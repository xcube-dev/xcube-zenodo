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

import logging
from typing import Tuple, Iterator, Container, Any, Union

import requests
import xarray as xr
from xcube.util.jsonschema import (
    JsonObjectSchema,
    JsonStringSchema,
)
from xcube.core.store import (
    DataDescriptor,
    DataStore,
    DataTypeLike,
    new_data_store,
)

from .constants import API_RECORDS_ENDPOINT
from ._utils import get_attrs_from_record
from ._utils import is_supported_file_format
from ._utils import translate_data_id2uri


_LOG = logging.getLogger("xcube")


class ZenodoDataStore(DataStore):
    """Implementation of the Zenodo data store defined in the ``xcube_zenodo``
    plugin."""

    def __init__(self, access_token: str):
        self._requests_params = {"access_token": access_token}
        self._https_data_store = new_data_store("https", root="zenodo.org")

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        params = dict(
            access_token=JsonStringSchema(
                title="Zenodo access token.",
            )
        )
        return JsonObjectSchema(
            properties=dict(**params),
            required=["access_token"],
            additional_properties=False,
        )

    @classmethod
    def get_data_types(cls) -> Tuple[str, ...]:
        store = new_data_store("https", root="zenodo.org")
        return store.get_data_types()

    def get_data_types_for_data(self, data_id: str) -> Tuple[str, ...]:
        uri = translate_data_id2uri(data_id)
        return self._https_data_store.get_data_types_for_data(uri)

    def get_data_ids(
        self, data_type: DataTypeLike = None, include_attrs: Container[str] = None
    ) -> Union[Iterator[str], Iterator[tuple[str, dict[str, Any]]]]:
        params = self._requests_params
        page = 1
        while True:
            params["page"] = page
            response = requests.get(API_RECORDS_ENDPOINT, params=params)
            if response.status_code == 500:
                page += 1
                continue
            response.raise_for_status()
            data = response.json()
            for record in data["hits"]["hits"]:
                if not record["files"]:
                    continue
                for file in record["files"]:
                    data_id = f"{record["id"]}/{file["key"]}"
                    if not is_supported_file_format(data_id):
                        continue
                    if include_attrs is None:
                        yield data_id
                    else:
                        attrs = get_attrs_from_record(record, file, include_attrs)
                        yield data_id, attrs
            if "next" not in data["links"]:
                break
            page += 1

    def has_data(self, data_id: str, data_type: str = None) -> bool:
        uri = translate_data_id2uri(data_id)
        return self._https_data_store.has_data(data_id=uri, data_type=data_type)

    def describe_data(
        self, data_id: str, data_type: DataTypeLike = None
    ) -> DataDescriptor:
        uri = translate_data_id2uri(data_id)
        descriptor = self._https_data_store.describe_data(
            data_id=uri, data_type=data_type
        )
        descriptor.data_id = data_id
        return descriptor

    def get_data_opener_ids(
        self, data_id: str = None, data_type: DataTypeLike = None
    ) -> Tuple[str, ...]:
        if data_id is not None:
            uri = translate_data_id2uri(data_id)
        else:
            uri = data_id
        return self._https_data_store.get_data_opener_ids(
            data_id=uri, data_type=data_type
        )

    def get_open_data_params_schema(
        self, data_id: str = None, opener_id: str = None
    ) -> JsonObjectSchema:
        if data_id is not None:
            uri = translate_data_id2uri(data_id)
        else:
            uri = data_id
        return self._https_data_store.get_open_data_params_schema(
            data_id=uri, opener_id=opener_id
        )

    def open_data(
        self, data_id: str, opener_id: str = None, **open_params
    ) -> xr.Dataset:
        uri = translate_data_id2uri(data_id)
        return self._https_data_store.open_data(
            data_id=uri, opener_id=opener_id, **open_params
        )

    def search_data(
        self, data_type: DataTypeLike = None, **search_params
    ) -> Iterator[DataDescriptor]:
        schema = self.get_search_params_schema()
        schema.validate_instance(search_params)
        raise NotImplementedError("search_data() operation is not supported.")

    @classmethod
    def get_search_params_schema(
        cls, data_type: DataTypeLike = None
    ) -> JsonObjectSchema:
        return JsonObjectSchema(
            properties={},
            required=[],
            additional_properties=False,
        )
