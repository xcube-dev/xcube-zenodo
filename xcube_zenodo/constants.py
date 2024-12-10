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

DATA_STORE_ID = "zenodo"
API_RECORDS_ENDPOINT = "https://zenodo.org/api/records"
PRELOAD_CACHE_FOLDER = "preload_cache/"

MAP_FILE_EXTENSION_FORMAT = {
    "zarr": "zarr",
    "levels": "levels",
    "nc": "netcdf",
    "tif": "geotiff",
    "tiff": "geotiff",
    "geotiff": "geotiff",
    "shp": "shapefile",
    "geojson": "geojson",
    "zip": "zip",
    "tar": "tar",
    "tar.gz": "tar.gz",
}
COMPRESSED_FORMATS = list(MAP_FILE_EXTENSION_FORMAT.keys())[-3:]
