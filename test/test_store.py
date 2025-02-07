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

import shutil
import unittest
from unittest.mock import patch

import numpy as np
import pytest
import requests
import xarray as xr
from xcube.core.store import (
    DatasetDescriptor,
    DataStoreError,
    get_data_store_params_schema,
    new_data_store,
)
from xcube.core.store.preload import PreloadStatus
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

    def test_get_data_store_params_schema(self):
        schema = get_data_store_params_schema(DATA_STORE_ID)
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertIn("cache_store_id", schema.properties)
        self.assertIn("cache_store_params", schema.properties)
        self.assertNotIn("cache_store_id", schema.required)
        self.assertNotIn("cache_store_params", schema.required)

    def test_get_data_types(self):
        store = new_data_store(DATA_STORE_ID)
        self.assertCountEqual(
            ("dataset", "mldataset", "geodataframe"), store.get_data_types()
        )

    @patch("xcube.core.store.fs.store.BaseFsDataStore.get_data_types_for_data")
    def test_get_data_types_for_data(self, mock_get_data_types):
        mock_get_data_types.return_value = ("dataset", "mldataset")
        store = new_data_store(DATA_STORE_ID)
        self.assertCountEqual(
            ("dataset", "mldataset"),
            store.get_data_types_for_data(self.data_id_tif),
        )
        mock_get_data_types.assert_called_once_with("records/1000000/files/test.tif")

    def test_get_data_ids(self):
        store = new_data_store(DATA_STORE_ID)
        with self.assertRaises(DataStoreError) as cm:
            store.get_data_ids()
        self.assertEqual(
            (
                "`get_data_ids` is not supported because Zenodo hosts all types of "
                "research, making it unhelpful to crawl through all records."
            ),
            f"{cm.exception}",
        )

    @patch("xcube.core.store.fs.store.BaseFsDataStore.has_data")
    def test_has_data(self, mock_has_data):
        mock_has_data.return_value = True

        store = new_data_store(DATA_STORE_ID)
        self.assertTrue(store.has_data(self.data_id_tif))
        mock_has_data.assert_called_once_with(
            data_id="records/1000000/files/test.tif", data_type=None
        )

    @patch("xcube.core.store.fs.store.BaseFsDataStore.describe_data")
    def test_describe_data(self, mock_describe_data):
        mock_describe_data.return_value = DatasetDescriptor(data_id=self.data_id_tif)
        store = new_data_store(DATA_STORE_ID)
        descriptor = store.describe_data(self.data_id_tif)
        expected_descriptor = {"data_id": self.data_id_tif, "data_type": "dataset"}
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertDictEqual(expected_descriptor, descriptor.to_dict())
        mock_describe_data.assert_called_once_with(
            data_id="records/1000000/files/test.tif", data_type=None
        )

    def test_get_data_opener_ids(self):
        store = new_data_store(DATA_STORE_ID)
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
        store = new_data_store(DATA_STORE_ID)
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
        store = new_data_store(DATA_STORE_ID)

        ds = store.open_data(data_id=self.data_id_tif)
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(["temperature", "precipitation"], list(ds.data_vars))
        self.assertEqual((5, 5, 5), ds["temperature"].shape)
        self.assertEqual((5, 5, 5), ds["precipitation"].shape)
        mock_open_data.assert_called_once_with(
            data_id="records/1000000/files/test.tif", opener_id=None
        )

    def test_open_data_compressed_format_not_preloaded(self):
        store = new_data_store(DATA_STORE_ID)

        with self.assertRaises(DataStoreError) as cm:
            _ = store.open_data(data_id="1234567/test.zip")
        self.assertEqual(
            (
                "The dataset 1234567/test.zip is stored in a compressed format. "
                "Please use store.preload_data('1234567/test.zip') first."
            ),
            f"{cm.exception}",
        )

    @patch("xcube.core.store.fs.store.BaseFsDataStore.open_data")
    @patch("xcube.core.store.fs.store.BaseFsDataStore.has_data")
    def test_open_data_compressed_format_preloaded(self, mock_has_data, mock_open_data):
        mock_has_data.return_value = True
        mock_open_data.return_value = self.mock_dataset
        store = new_data_store(DATA_STORE_ID)
        ds = store.open_data("1234567/test.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(["temperature", "precipitation"], list(ds.data_vars))
        self.assertEqual((5, 5, 5), ds["temperature"].shape)
        self.assertEqual((5, 5, 5), ds["precipitation"].shape)
        mock_has_data.assert_called_once_with("1234567/test.zarr")
        mock_open_data.assert_called_once_with(data_id="1234567/test.zarr")

    @pytest.mark.vcr()
    def test_preload_data_tar_gz(self):
        store = new_data_store(DATA_STORE_ID)
        data_id = "6453099/diaz2016_inputs_raw.zarr.tar.gz"
        handle = store.preload_data(data_id, blocking=True, silent=True)
        handle.close()

        self.assertCountEqual(
            ["6453099/diaz2016_inputs_raw.zarr"], store.cache_store.list_data_ids()
        )
        ds = store.open_data("6453099/diaz2016_inputs_raw.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertEqual(
            {
                "country": 160,
                "coef": 7,
                "seg": 12148,
                "elev": 15,
                "return_period": 5,
                "fund": 16,
                "rcp_pt": 12,
                "t": 20,
                "repseg": 8,
                "subset": 35,
            },
            ds.sizes,
        )
        shutil.rmtree(store.cache_store.root)

    @pytest.mark.vcr()
    def test_preload_data_zip(self):
        store = new_data_store(DATA_STORE_ID)
        data_ids = (
            "13333034/andorra.zip",
            "13333034/invalid_data_id.tif",
        )

        with self.assertLogs("xcube.zenodo", level="WARNING") as cm:
            handle = store.preload_data(*data_ids, blocking=True, silent=True)
        self.assertEqual(1, len(cm.output))
        msg = (
            "WARNING:xcube.zenodo:13333034/invalid_data_id.tif cannot be preloaded. "
            "Only 'zip', 'tar', and 'tar.gz' compressed files are supported. The "
            "preload request is discarded."
        )
        self.assertEqual(msg, str(cm.output[-1]))
        handle.close()

        self.assertCountEqual(
            [
                "13333034/andorra/disturbance_severity_1985_2023_andorra.zarr",
                "13333034/andorra/number_disturbances_andorra.zarr",
                "13333034/andorra/disturbance_agent_1985_2023_andorra.zarr",
                "13333034/andorra/annual_disturbances_1985_2023_andorra.zarr",
                "13333034/andorra/latest_disturbance_andorra.zarr",
                "13333034/andorra/disturbance_probability_1985_2023_andorra.zarr",
                "13333034/andorra/disturbance_agent_aggregated_andorra.zarr",
                "13333034/andorra/forest_mask_andorra.zarr",
                "13333034/andorra/greatest_disturbance_andorra.zarr",
            ],
            store.cache_store.list_data_ids(),
        )
        ds = store.open_data(
            "13333034/andorra/disturbance_probability_1985_2023_andorra.zarr"
        )
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual([f"band_{i}" for i in range(1, 40)], list(ds.data_vars))
        self.assertEqual(ds["band_1"].shape, (971, 1149))
        shutil.rmtree(store.cache_store.root)

    @pytest.mark.vcr()
    def test_preload_data_download_fails(self):
        store = new_data_store(DATA_STORE_ID)
        data_ids = "13333034/andorra_invalid.zip"

        handle = store.preload_data(data_ids, silent=True)
        state = handle._states[data_ids]
        self.assertEqual(PreloadStatus.failed, state.status)
        self.assertIsInstance(state.exception, requests.exceptions.HTTPError)
        self.assertEqual(
            "404 Client Error: NOT FOUND for url: https://zenodo.org/records"
            "/13333034/files/andorra_invalid.zip",
            state.exception.args[0],
        )

    def test_search_data(self):
        store = new_data_store(DATA_STORE_ID)
        with self.assertRaises(NotImplementedError) as context:
            store.search_data()
        self.assertEqual(
            str(context.exception), "search_data() operation is not supported."
        )
