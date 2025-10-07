# xcube-zenodo

[![Build Status](https://github.com/xcube-dev/xcube-zenodo/actions/workflows/unittest-workflow.yml/badge.svg?branch=main)](https://github.com/xcube-dev/xcube-zenodo/actions)
[![codecov](https://codecov.io/gh/xcube-dev/xcube-zenodo/graph/badge.svg?token=ktcp1maEgz)](https://codecov.io/gh/xcube-dev/xcube-zenodo)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/github/license/dcs4cop/xcube-smos)](https://github.com/xcube-dev/xcube-zenodo/blob/main/LICENSE)

`xcube-zenodo` is a Python package and
[xcube plugin](https://xcube.readthedocs.io/en/latest/plugins.html) that adds a
[data store](https://xcube.readthedocs.io/en/latest/api.html#data-store-framework)
named `zenodo` to xcube. The data store is used to access datasets which are published
on [Zenodo](https://zenodo.org/).


## How to use the xcube-zenodo plugin

### Lazy access of datasets published as `tif` or `netcdfs` and zipped (uncompressed) `zarr`

To access datasets published on Zenodo, locate the **record ID** in the URL of the 
respective Zenodo webpage. This ID is required when initializing the Zenodo data store.  

For example, the record ID for the [Canopy height and biomass map for Europe](https://zenodo.org/records/8154445)
is `"8154445"`. To access the dataset `"planet_canopy_cover_30m_v0.1.tif"`, the
following code snippet will **lazy-load** the dataset:

```python
from xcube.core.store import new_data_store

store = new_data_store("zenodo", "8154445")
ds = store.open_data(
    "planet_canopy_cover_30m_v0.1.tif",
    tile_size=(1024, 1024)
)
```

To learn more check out the Example note books:

- [Access TIF](examples/01_lazy_access_tif.ipynb)
- [Access NetCDF](examples/02_lazy_access_netcdf.ipynb)
- [Access zipped Zarr](examples/03_lazy_access_zarr.ipynb)


### Access compressed datasets via the xcube's preload API

If datasets are published as `zip`, `tar`, `tar.gz`, or `.rar` you can use the preload
API to preload the data into the local file system. If the compressed file contains
multiple datasets, the data IDs will be extended by one layer. A short example is shown
below.

```python
from xcube.core.store import new_data_store

store = new_data_store("zenodo", root="13333034")
cache_store = store.preload_data("andorra.zip")
preloaded_data_ids = cache_store.list_data_ids()
ds = store.open_data(preloaded_data_ids[0])
```

To learn more check out the example notebooks:

- [Access zipped TIF files](examples/04_preload_zip.ipynb)
- [Access RAR-compressed files](examples/05_preload_rar.ipynb)

> **Note:**
> The Python package [`rarfile`](https://github.com/markokr/rarfile) is used for
> handling RAR-compressed files. It requires an external decompression backend —
> such as **`unrar`** or **`bsdtar`** — to be installed on your system.


## Installing the xcube-zenodo plugin

This section describes three alternative methods you can use to install the
xcube-zenodo plugin.

For installation of conda packages, we recommend
[mamba](https://mamba.readthedocs.io/). It is also possible to use conda,
but note that installation may be significantly slower with conda than with
mamba. If using conda rather than mamba, replace the `mamba` command with
`conda` in the installation commands given below.

### Installation into a new environment with mamba

This method creates a new environment and installs the latest conda-forge
release of xcube-zenodo, along with all its required dependencies, into the
newly created environment.

To do so, execute the following commands:

```bash
mamba create --name xcube-zenodo --channel conda-forge xcube-zenodo
mamba activate xcube-zenodo
```

The name of the environment may be freely chosen.

### Installation into an existing environment with mamba

This method assumes that you have an existing environment, and you want
to install xcube-zenodo into it.

With the existing environment activated, execute this command:

```bash
mamba install --channel conda-forge xcube-zenodo
```

Once again, xcube and any other necessary dependencies will be installed
automatically if they are not already installed.

### Installation into an existing environment from the repository

If you want to install xcube-zenodo directly from the git repository (for example
in order to use an unreleased version or to modify the code), you can
do so as follows:

```bash
mamba create --name xcube-zenodo --channel conda-forge --only-deps xcube-zenodo
mamba activate xcube-zenodo
git clone https://github.com/xcube-dev/xcube-zenodo.git
python -m pip install --no-deps --editable xcube-zenodo/
```

This installs all the dependencies of xcube-zenodo into a fresh conda environment,
then installs xcube-zenodo into this environment from the repository.

## Testing <a name="testing"></a>

To run the unit test suite:

```bash
pytest
```

To analyze test coverage:

```bash
pytest --cov=xcube_zenodo
```

To produce an HTML
[coverage report](https://pytest-cov.readthedocs.io/en/latest/reporting.html):

```bash
pytest --cov-report html --cov=xcube_zenodo
```

### Some notes on the strategy of unit-testing <a name="unittest_strategy"></a>

The unit test suite uses [pytest-recording](https://pypi.org/project/pytest-recording/)
to mock https requests via the Python library `requests`. During development an
actual HTTP request is performed and the responses are saved in `cassettes/**.yaml`
files. During testing, only the `cassettes/**.yaml` files are used without an actual
HTTP request. During development, to save the responses to `cassettes/**.yaml`, run

```bash
pytest -v -s --record-mode new_episodes
```
Note that `--record-mode new_episodes` overwrites all cassettes. If one only
wants to write cassettes which are not saved already, `--record-mode once` can be used.
[pytest-recording](https://pypi.org/project/pytest-recording/) supports all records
modes given by [VCR.py](https://vcrpy.readthedocs.io/en/latest/usage.html#record-modes).
After recording the cassettes, testing can be then performed as usual.