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

import pytest
import xarray as xr
from xcube.core.store import new_data_store
from xcube.core.store import get_data_store_params_schema
from xcube.core.store import DatasetDescriptor
from xcube.util.jsonschema import JsonObjectSchema

from xcube_zenodo.constants import DATA_STORE_ID


ZENODO_ACCESS_TOKEN = "ZsZVfyPCmLYRZQtfSYWruNwXYBykonv0pXZYnrQYNNL0gGMJipYsx0CYvOSB"


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "filter_headers": ["authorization"],
        "ignore_localhost": True,
        "record_mode": "once",
    }


class StacDataStoreTest(unittest.TestCase):

    def setUp(self):
        self.data_id_tif = "8154445/planet_agb_30m_v0.1.tif"

    def test_get_data_store_params_schema(self):
        schema = get_data_store_params_schema(DATA_STORE_ID)
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertIn("access_token", schema.properties)
        self.assertIn("access_token", schema.required)
        self.assertNotIn("url", schema.properties)

    def test_get_data_types(self):
        store = new_data_store(DATA_STORE_ID, access_token=ZENODO_ACCESS_TOKEN)
        self.assertCountEqual(
            ("dataset", "mldataset", "geodataframe"), store.get_data_types()
        )

    def test_get_data_types_for_data(self):
        store = new_data_store(DATA_STORE_ID, access_token=ZENODO_ACCESS_TOKEN)
        self.assertCountEqual(
            ("dataset", "mldataset"),
            store.get_data_types_for_data(data_id=self.data_id_tif),
        )

    # @pytest.mark.vcr
    def test_get_data_ids(self):
        store = new_data_store(DATA_STORE_ID, access_token=ZENODO_ACCESS_TOKEN)
        include_attrs = ["title", "file_key", "file_size", "file_links"]
        data_ids = store.get_data_ids(include_attrs=include_attrs)
        data_ids = list(itertools.islice(data_ids, 1))
        expected_data_id = (
            (
                "14039675/ta_200hPa_6hrPlevPt_UKESM1-0-LL_ssp585"
                "_r1i1p1f1_207001-209912_djf.nc"
            ),
            {
                "title": (
                    'Data to support "Reduced winter-time Clear Air Turbulence in the '
                    "trans-Atlantic region under Stratospheric Aerosol Injection by "
                    "Katie L Barnes, Anthony C Jones, Paul D Williams and James M "
                    "Haywood, Submitted to Geophysical Research Letters, "
                    'November 2024"'
                ),
                "file_key": (
                    "ta_200hPa_6hrPlevPt_UKESM1-0-LL_ssp585_r1i1p1f1_"
                    "207001-209912_djf.nc"
                ),
                "file_size": 1202882929,
                "file_links": {
                    "self": (
                        "https://zenodo.org/api/records/14039675/files/ta_200hPa_6hrPl"
                        "evPt_UKESM1-0-LL_ssp585_r1i1p1f1_207001-209912_djf.nc/content"
                    )
                },
            },
        )
        self.assertEqual(expected_data_id, data_ids[0])

    def test_has_data(self):
        store = new_data_store(DATA_STORE_ID, access_token=ZENODO_ACCESS_TOKEN)
        self.assertTrue(store.has_data(self.data_id_tif))
        self.assertTrue(store.has_data(self.data_id_tif, data_type="dataset"))
        self.assertTrue(store.has_data(self.data_id_tif, data_type="mldataset"))
        self.assertFalse(store.has_data(self.data_id_tif, data_type="geodataframe"))
        self.assertFalse(store.has_data("8154445/non_existent_filename.tif"))
        self.assertFalse(store.has_data("wrong_record_id/planet_agb_30m_v0.1.tif"))
        self.assertFalse(store.has_data("wrong_record_id/planet_agb_30m_v0.1.tif"))

    def test_describe_data(self):
        store = new_data_store(DATA_STORE_ID, access_token=ZENODO_ACCESS_TOKEN)
        descriptor = store.describe_data(self.data_id_tif)
        expected_descriptor = {
            "data_id": "records/8154445/files/planet_agb_30m_v0.1.tif",
            "data_type": "dataset",
            "bbox": [
                2554652.2042793306,
                1338813.6609999156,
                7666562.204279331,
                5819703.660999916,
            ],
            "time_range": (None, None),
            "dims": {"x": 170397, "y": 149363},
            "spatial_res": 30.0,
            "coords": {
                "x": {"name": "x", "dtype": "float64", "dims": ["x"]},
                "y": {"name": "y", "dtype": "float64", "dims": ["y"]},
                "spatial_ref": {
                    "name": "spatial_ref",
                    "dtype": "int64",
                    "dims": [],
                    "attrs": {
                        "crs_wkt": (
                            'PROJCS["ETRS89-extended / LAEA Europe",GEOGCS["ETRS89",'
                            'DATUM["European_Terrestrial_Reference_System_1989",'
                            'SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY'
                            '["EPSG","7019"]],AUTHORITY["EPSG","6258"]],PRIMEM'
                            '["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",'
                            '0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY'
                            '["EPSG","4258"]],PROJECTION["Lambert_Azimuthal_Equal_Area'
                            '"],PARAMETER["latitude_of_center",52],PARAMETER["longitude'
                            '_of_center",10],PARAMETER["false_easting",4321000],'
                            'PARAMETER["false_northing",3210000],UNIT["metre",1,'
                            'AUTHORITY["EPSG","9001"]],AXIS["Northing",NORTH],AXIS['
                            '"Easting",EAST],AUTHORITY["EPSG","3035"]]'
                        ),
                        "semi_major_axis": 6378137.0,
                        "semi_minor_axis": 6356752.314140356,
                        "inverse_flattening": 298.257222101,
                        "reference_ellipsoid_name": "GRS 1980",
                        "longitude_of_prime_meridian": 0.0,
                        "prime_meridian_name": "Greenwich",
                        "geographic_crs_name": "ETRS89",
                        "horizontal_datum_name": (
                            "European Terrestrial Reference System 1989"
                        ),
                        "projected_crs_name": "ETRS89-extended / LAEA Europe",
                        "grid_mapping_name": "lambert_azimuthal_equal_area",
                        "latitude_of_projection_origin": 52.0,
                        "longitude_of_projection_origin": 10.0,
                        "false_easting": 4321000.0,
                        "false_northing": 3210000.0,
                        "spatial_ref": (
                            'PROJCS["ETRS89-extended / LAEA Europe",GEOGCS["ETRS89",DAT'
                            'UM["European_Terrestrial_Reference_System_1989",SPHEROID['
                            '"GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]]'
                            ',AUTHORITY["EPSG","6258"]],PRIMEM["Greenwich",0,AUTHORITY'
                            '["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORI'
                            'TY["EPSG","9122"]],AUTHORITY["EPSG","4258"]],PROJECTION'
                            '["Lambert_Azimuthal_Equal_Area"],PARAMETER["latitude_of_'
                            'center",52],PARAMETER["longitude_of_center",10],PARAMETER['
                            '"false_easting",4321000],PARAMETER["false_northing",3210'
                            '000],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["North'
                            'ing",NORTH],AXIS["Easting",EAST],AUTHORITY["EPSG","3035"]]'
                        ),
                        "GeoTransform": (
                            "2554652.2042793306 30.0 0.0 5819703.660999916 0.0 -30.0"
                        ),
                    },
                },
            },
            "data_vars": {
                "band_1": {
                    "name": "band_1",
                    "dtype": "uint16",
                    "dims": ["y", "x"],
                    "chunks": [512, 512],
                    "attrs": {
                        "AREA_OR_POINT": "Area",
                        "_FillValue": 0,
                        "scale_factor": 1.0,
                        "add_offset": 0.0,
                        "grid_mapping": "spatial_ref",
                    },
                }
            },
            "attrs": {
                "source": (
                    "https://zenodo.org/records/8154445/files/planet_agb_30m_v0.1.tif"
                )
            },
        }

        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertDictEqual(expected_descriptor, descriptor.to_dict())

    def test_get_data_opener_ids(self):
        store = new_data_store(DATA_STORE_ID, access_token=ZENODO_ACCESS_TOKEN)
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
        store = new_data_store(DATA_STORE_ID, access_token=ZENODO_ACCESS_TOKEN)
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

    def test_open_data(self):
        store = new_data_store(DATA_STORE_ID, access_token=ZENODO_ACCESS_TOKEN)
        ds = store.open_data(data_id=self.data_id_tif)
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(["band_1"], list(ds.data_vars))
        self.assertCountEqual([170397, 149363], [ds.sizes["y"], ds.sizes["x"]])
        self.assertCountEqual(
            [512, 512], [ds.chunksizes["y"][0], ds.chunksizes["x"][0]]
        )
