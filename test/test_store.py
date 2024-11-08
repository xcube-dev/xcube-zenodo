# The MIT License (MIT)
# Copyright (c) 2024 by the xcube development team and contributors
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

import itertools
import unittest
from unittest.mock import patch
from unittest.mock import Mock

import numpy as np
import xarray as xr
from xcube.core.store import new_data_store
from xcube.core.store import get_data_store_params_schema
from xcube.core.store import DatasetDescriptor
from xcube.util.jsonschema import JsonObjectSchema

from xcube_zenodo.constants import DATA_STORE_ID


class ZenodoDataStoreTest(unittest.TestCase):

    def setUp(self):
        self.data_id_tif = "1000000/test.tif"
        self.mock_dataset = xr.Dataset(
            {
                "temperature": (("time", "x", "y"), np.random.rand(5, 5, 5)),
                "precipitation": (("time", "x", "y"), np.random.rand(5, 5, 5)),
            }
        )
        self.access_token = "xxx"

    def test_get_data_store_params_schema(self):
        schema = get_data_store_params_schema(DATA_STORE_ID)
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertIn("access_token", schema.properties)
        self.assertIn("access_token", schema.required)
        self.assertNotIn("url", schema.properties)

    def test_get_data_types(self):
        store = new_data_store(DATA_STORE_ID, access_token=self.access_token)
        self.assertCountEqual(
            ("dataset", "mldataset", "geodataframe"), store.get_data_types()
        )

    @patch("xcube.core.store.fs.store.BaseFsDataStore.get_data_types_for_data")
    def test_get_data_types_for_data(self, mock_get_data_types):
        mock_get_data_types.return_value = ("dataset", "mldataset")
        store = new_data_store(DATA_STORE_ID, access_token=self.access_token)
        self.assertCountEqual(
            ("dataset", "mldataset"),
            store.get_data_types_for_data(self.data_id_tif),
        )
        mock_get_data_types.assert_called_once_with("records/1000000/files/test.tif")

    @patch("xcube_zenodo.store.requests.get")
    def test_get_data_ids(self, mock_get):
        mock_response_1 = Mock()
        mock_response_1.status_code = 500

        mock_response_2 = Mock()
        mock_response_2.status_code = 200
        mock_response_2.json.return_value = {
            "hits": {"hits": [{"id": 10000000, "title": "test_title", "files": []}]},
            "links": {"next": "link_to_next_page"},
        }
        mock_response_2.raise_for_status.return_value = None

        mock_response_3 = Mock()
        mock_response_3.status_code = 200
        mock_response_3.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "id": 10000000,
                        "title": "test_title",
                        "files": [{"key": "test.pdf"}],
                    }
                ]
            },
            "links": {"next": "link_to_next_page"},
        }
        mock_response_3.raise_for_status.return_value = None

        mock_response_4 = Mock()
        mock_response_4.status_code = 200
        mock_response_4.raise_for_status.return_value = None
        mock_response_4.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "id": 10000000,
                        "title": "test_title",
                        "files": [
                            {
                                "key": "test.tif",
                                "size": 1000,
                                "links": {"self": "https://zenodo.org/test.tif"},
                            }
                        ],
                    }
                ]
            },
            "links": {},
        }
        mock_get.side_effect = [
            mock_response_1,
            mock_response_2,
            mock_response_3,
            mock_response_4,
        ]

        store = new_data_store(DATA_STORE_ID, access_token=self.access_token)
        include_attrs = ["title", "file_key", "file_size", "file_links"]
        data_ids = store.get_data_ids(include_attrs=include_attrs)
        data_ids = list(data_ids)
        expected_data_id = (
            "10000000/test.tif",
            {
                "title": "test_title",
                "file_key": "test.tif",
                "file_size": 1000,
                "file_links": {"self": "https://zenodo.org/test.tif"},
            },
        )
        self.assertEqual(expected_data_id, data_ids[0])
        self.assertEqual(4, mock_get.call_count)

        # test without attributes
        mock_get.reset_mock()
        mock_get.side_effect = [mock_response_4]
        data_ids = store.get_data_ids()
        data_ids = list(data_ids)
        self.assertEqual("10000000/test.tif", data_ids[0])
        self.assertEqual(1, mock_get.call_count)

    @patch("xcube.core.store.fs.store.BaseFsDataStore.has_data")
    def test_has_data(self, mock_has_data):
        mock_has_data.return_value = True

        store = new_data_store(DATA_STORE_ID, access_token=self.access_token)
        self.assertTrue(store.has_data(self.data_id_tif))
        mock_has_data.assert_called_once_with(
            data_id="records/1000000/files/test.tif", data_type=None
        )

    @patch("xcube.core.store.fs.store.BaseFsDataStore.describe_data")
    def test_describe_data(self, mock_describe_data):
        mock_describe_data.return_value = DatasetDescriptor(data_id=self.data_id_tif)
        store = new_data_store(DATA_STORE_ID, access_token=self.access_token)
        descriptor = store.describe_data(self.data_id_tif)
        expected_descriptor = {"data_id": self.data_id_tif, "data_type": "dataset"}
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertDictEqual(expected_descriptor, descriptor.to_dict())
        mock_describe_data.assert_called_once_with(
            data_id="records/1000000/files/test.tif", data_type=None
        )

    def test_get_data_opener_ids(self):
        store = new_data_store(DATA_STORE_ID, access_token=self.access_token)
        self.assertCountEqual(
            (
                "dataset:netcdf:https",
                "dataset:zarr:https",
                "dataset:levels:https",
                "mldataset:levels:https",
                "dataset:geotiff:https",
                "mldataset:geotiff:https",
                "geodataframe:shapefile:https",
                "geodataframe:geojson:https",
            ),
            store.get_data_opener_ids(),
        )
        self.assertCountEqual(
            ("dataset:geotiff:https",),
            store.get_data_opener_ids(self.data_id_tif),
        )
        self.assertCountEqual(
            ("dataset:geotiff:https",),
            store.get_data_opener_ids(self.data_id_tif, data_type="dataset"),
        )
        self.assertCountEqual(
            ("mldataset:geotiff:https",),
            store.get_data_opener_ids(self.data_id_tif, data_type="mldataset"),
        )

    def test_get_open_data_params_schema(self):
        store = new_data_store(DATA_STORE_ID, access_token=self.access_token)
        schema = store.get_open_data_params_schema()
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertIn("log_access", schema.properties)
        self.assertIn("cache_size", schema.properties)
        self.assertIn("group", schema.properties)
        self.assertIn("chunks", schema.properties)
        self.assertIn("decode_cf", schema.properties)
        self.assertIn("mask_and_scale", schema.properties)
        self.assertIn("decode_times", schema.properties)
        self.assertIn("drop_variables", schema.properties)
        self.assertIn("consolidated", schema.properties)
        schema = store.get_open_data_params_schema(data_id=self.data_id_tif)
        self.assertIn("tile_size", schema.properties)
        self.assertIn("overview_level", schema.properties)
        self.assertIn("data_type", schema.properties)
        schema = store.get_open_data_params_schema(
            data_id=self.data_id_tif, opener_id="mldataset:geotiff:https"
        )
        self.assertIn("tile_size", schema.properties)

    @patch("xcube.core.store.fs.store.BaseFsDataStore.open_data")
    def test_open_data(self, mock_open_data):
        mock_open_data.return_value = self.mock_dataset
        store = new_data_store(DATA_STORE_ID, access_token=self.access_token)

        ds = store.open_data(data_id=self.data_id_tif)
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(["temperature", "precipitation"], list(ds.data_vars))
        self.assertEqual(ds["temperature"].shape, (5, 5, 5))
        self.assertEqual(ds["precipitation"].shape, (5, 5, 5))
        mock_open_data.assert_called_once_with(
            data_id="records/1000000/files/test.tif", opener_id=None
        )

    def test_search_data(self):
        store = new_data_store(DATA_STORE_ID, access_token=self.access_token)
        with self.assertRaises(NotImplementedError) as context:
            store.search_data()
        self.assertEqual(
            str(context.exception), "search_data() operation is not supported."
        )
