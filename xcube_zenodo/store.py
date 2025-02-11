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

import xarray as xr
from xcube.core.store import (
    DataDescriptor,
    DataStore,
    DataStoreError,
    DataTypeLike,
    new_data_store,
)
from xcube.core.store.preload import PreloadHandle
from xcube.util.jsonschema import JsonBooleanSchema, JsonObjectSchema, JsonStringSchema

from ._utils import is_supported_compressed_file_format, translate_data_id2fs_path
from .constants import CACHE_FOLDER_NAME, LOG
from .preload import ZenodoPreloadHandle


class ZenodoDataStore(DataStore):
    """Implementation of the Zenodo data store defined in the ``xcube_zenodo``
    plugin."""

    def __init__(
        self,
        cache_store_id: str = "file",
        cache_store_params: dict = None,
    ):
        self._https_data_store = new_data_store("https", root="zenodo.org")
        if cache_store_params is None:
            cache_store_params = dict(root=CACHE_FOLDER_NAME)
        cache_store_params["max_depth"] = cache_store_params.pop("max_depth", 3)
        self.cache_store = new_data_store(cache_store_id, **cache_store_params)

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        params = dict(
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
                default=dict(root=CACHE_FOLDER_NAME, max_depth=3),
            ),
        )
        return JsonObjectSchema(
            properties=dict(**params),
            required=[],
            additional_properties=False,
        )

    @classmethod
    def get_data_types(cls) -> Tuple[str, ...]:
        store = new_data_store("https", root="zenodo.org")
        return store.get_data_types()

    def get_data_types_for_data(self, data_id: str) -> Tuple[str, ...]:
        uri = translate_data_id2fs_path(data_id)
        return self._https_data_store.get_data_types_for_data(uri)

    def get_data_ids(
        self, data_type: DataTypeLike = None, include_attrs: Container[str] = None
    ) -> Iterator[str] | Iterator[tuple[str, dict[str, Any]]]:
        raise DataStoreError(
            "`get_data_ids` is not supported because Zenodo hosts all types of "
            "research, making it unhelpful to crawl through all records."
        )

    def has_data(self, data_id: str, data_type: str = None) -> bool:
        uri = translate_data_id2fs_path(data_id)
        return self._https_data_store.has_data(data_id=uri, data_type=data_type)

    def describe_data(
        self, data_id: str, data_type: DataTypeLike = None
    ) -> DataDescriptor:
        uri = translate_data_id2fs_path(data_id)
        descriptor = self._https_data_store.describe_data(
            data_id=uri, data_type=data_type
        )
        descriptor.data_id = data_id
        return descriptor

    def get_data_opener_ids(
        self, data_id: str = None, data_type: DataTypeLike = None
    ) -> Tuple[str, ...]:
        if data_id is not None:
            uri = translate_data_id2fs_path(data_id)
        else:
            uri = data_id
        return self._https_data_store.get_data_opener_ids(
            data_id=uri, data_type=data_type
        )

    def get_open_data_params_schema(
        self, data_id: str = None, opener_id: str = None
    ) -> JsonObjectSchema:
        if data_id is not None:
            uri = translate_data_id2fs_path(data_id)
        else:
            uri = data_id
        return self._https_data_store.get_open_data_params_schema(
            data_id=uri, opener_id=opener_id
        )

    def open_data(
        self, data_id: str, opener_id: str = None, **open_params
    ) -> xr.Dataset:
        if is_supported_compressed_file_format(data_id):
            raise DataStoreError(
                f"The dataset {data_id} is stored in a compressed format. "
                f"Please use store.preload_data({data_id!r}) first."
            )
        elif self.cache_store.has_data(data_id):
            return self.cache_store.open_data(data_id=data_id, **open_params)
        else:
            uri = translate_data_id2fs_path(data_id)
            return self._https_data_store.open_data(
                data_id=uri, opener_id=opener_id, **open_params
            )

    def preload_data(self, *data_ids: str, **preload_params) -> PreloadHandle:

        schema = self.get_preload_data_params()
        schema.validate_instance(preload_params)
        data_ids_sel = []
        for data_id in data_ids:
            if is_supported_compressed_file_format(data_id):
                data_ids_sel.append(data_id)
            else:
                LOG.warning(
                    f"{data_id} cannot be preloaded. Only 'zip', 'tar', and "
                    "'tar.gz' compressed files are supported. The preload "
                    "request is discarded."
                )
        return ZenodoPreloadHandle(
            self.cache_store,
            *data_ids_sel,
            **preload_params,
        )

    def get_preload_data_params(self) -> JsonObjectSchema:
        params = dict(
            blocking=JsonBooleanSchema(
                title="Switch to make the preloading process blocking or "
                "non-blocking",
                description="If True, the preloading process blocks the script.",
                default=True,
            ),
            silent=JsonBooleanSchema(
                title="Switch to visual the preloading process.",
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
