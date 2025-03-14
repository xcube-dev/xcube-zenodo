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

from typing import Any, Container, Iterator, Tuple

import requests
import xarray as xr
from xcube.core.store import (
    DataDescriptor,
    DataStore,
    DataStoreError,
    DataTypeLike,
    PreloadedDataStore,
    new_data_store,
)
from xcube.util.jsonschema import JsonBooleanSchema, JsonObjectSchema, JsonStringSchema

from ._utils import is_supported_compressed_file_format
from .constants import CACHE_FOLDER_NAME, LOG
from .preload import ZenodoPreloadHandle


class ZenodoDataStore(DataStore):
    """Implementation of the Zenodo data store defined in the ``xcube_zenodo``
    plugin."""

    def __init__(
        self,
        root: str,
        cache_store_id: str = "file",
        cache_store_params: dict = None,
    ):
        self._root = root
        self._uri_root = f"zenodo.org/records/{root}/files"
        self._https_data_store = new_data_store("https", root=self._uri_root)
        if cache_store_params is None:
            cache_store_params = dict(root=f"{CACHE_FOLDER_NAME}/{root}")
        cache_store_params["max_depth"] = cache_store_params.pop("max_depth", 10)
        self.cache_store: PreloadedDataStore = new_data_store(
            cache_store_id, **cache_store_params
        )

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        params = dict(
            root=JsonStringSchema(
                title="Zenodo record ID",
                description="The record ID can be found in the url.",
            ),
            cache_store_id=JsonStringSchema(
                title="Store ID of cache data store.",
                description=(
                    "Store ID of a filesystem-based data store implemented in xcube."
                ),
                default="file",
            ),
            cache_store_params=JsonObjectSchema(
                title="Store parameters of cache data store.",
                description=(
                    "Store parameters of a filesystem-based data store"
                    "implemented in xcube."
                ),
                default=dict(root=CACHE_FOLDER_NAME, max_depth=10),
            ),
        )
        return JsonObjectSchema(
            properties=dict(**params),
            required=["root"],
            additional_properties=False,
        )

    @classmethod
    def get_data_types(cls) -> Tuple[str, ...]:
        store = new_data_store("https", root="zenodo.org")
        return store.get_data_types()

    def get_data_types_for_data(self, data_id: str) -> Tuple[str, ...]:
        return self._https_data_store.get_data_types_for_data(data_id)

    def get_data_ids(
        self, data_type: DataTypeLike = None, include_attrs: Container[str] = None
    ) -> Iterator[str] | Iterator[tuple[str, dict[str, Any]]]:
        files = self._get_files_from_record()
        for file in files:
            if self._https_data_store.has_data(
                file["key"]
            ) or is_supported_compressed_file_format(file["key"]):
                yield file["key"]

    def has_data(self, data_id: str, data_type: str = None) -> bool:
        return self._https_data_store.has_data(data_id=data_id, data_type=data_type)

    def describe_data(
        self, data_id: str, data_type: DataTypeLike = None
    ) -> DataDescriptor:
        return self._https_data_store.describe_data(
            data_id=data_id, data_type=data_type
        )

    def get_data_opener_ids(
        self, data_id: str = None, data_type: DataTypeLike = None
    ) -> Tuple[str, ...]:
        return self._https_data_store.get_data_opener_ids(
            data_id=data_id, data_type=data_type
        )

    def get_open_data_params_schema(
        self, data_id: str = None, opener_id: str = None
    ) -> JsonObjectSchema:
        return self._https_data_store.get_open_data_params_schema(
            data_id=data_id, opener_id=opener_id
        )

    def open_data(
        self, data_id: str, opener_id: str = None, **open_params
    ) -> xr.Dataset:
        if is_supported_compressed_file_format(data_id):
            raise DataStoreError(
                f"The dataset {data_id} is stored in a compressed format. "
                f"Please use store.preload_data({data_id!r}) first."
            )
        else:
            return self._https_data_store.open_data(
                data_id=data_id, opener_id=opener_id, **open_params
            )

    def preload_data(self, *data_ids: str, **preload_params) -> PreloadedDataStore:
        schema = self.get_preload_data_params()
        schema.validate_instance(preload_params)

        # this will load all data ids in the store
        if not data_ids:
            files = self._get_files_from_record()
            data_ids = []
            for file in files:
                if is_supported_compressed_file_format(file["key"]):
                    data_ids.append(file["key"])

        data_ids_sel = []
        for data_id in data_ids:
            if is_supported_compressed_file_format(data_id):
                data_ids_sel.append(f"https://{self._uri_root}/{data_id}")
            else:
                LOG.warning(
                    f"{data_id} cannot be preloaded. Only 'zip', 'tar', and "
                    "'tar.gz' compressed files are supported. The preload "
                    "request is discarded."
                )
        self.cache_store.preload_handle = ZenodoPreloadHandle(
            self.cache_store,
            *data_ids_sel,
            **preload_params,
        )
        return self.cache_store

    def get_preload_data_params(self) -> JsonObjectSchema:
        params = dict(
            blocking=JsonBooleanSchema(
                title="Switch to make the preloading process blocking or "
                "non-blocking",
                description="If True, the preloading process blocks the script.",
                default=True,
            ),
            silent=JsonBooleanSchema(
                title="Switch to visualize the preloading process.",
                description=(
                    "If False, the preloading progress will be visualized in a table."
                    "If True, the visualization will be suppressed."
                ),
                default=True,
            ),
        )
        return JsonObjectSchema(
            properties=dict(**params),
            required=[],
            additional_properties=False,
        )

    def search_data(self, data_type: DataTypeLike = None, **search_params):
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

    def _get_files_from_record(self):
        url = f"https://zenodo.org/api/records/{self._root}"
        response = requests.get(url)
        return response.json().get("files", [])
