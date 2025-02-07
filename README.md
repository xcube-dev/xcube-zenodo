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

### Lazy access of datasets published as tif or netcdfs

To access datasets publish on Zenodo, find your dataset on the Zenodo webpage and build
the data ID with the following structure `"<record_id>/<file_name>"`. For example for 
the [Canopy height and biomass map for Europe](https://zenodo.org/records/8154445) the
data ID for the dataset "planet_canopy_cover_30m_v0.1.tif" will be given by
`"8154445/planet_canopy_cover_30m_v0.1.tif"`. The record ID can be found in the url of 
the zenodo page. The following few lines of code will lazy load the dataset. 

```python
from xcube.core.store import new_data_store

store = new_data_store("zenodo")
ds = store.open_data(
    "8154445/planet_canopy_cover_30m_v0.1.tif",
    tile_size=(1024, 1024)
)
```

To learn more check out the [example notebook zenodo_data_store.ipynb](examples/zenodo_data_store.ipynb).


### Access compressed datasets via the xcube's preload API

If datasets are published as `zip`, `tar`, `tar.gz`, you can use the preload API to
preload the data into the local or s3 file system. If the compressed file contains
multiple datasets, the data IDs will be extended by one layer. A short example is shown
below.

```python
from xcube.core.store import new_data_store

store = new_data_store("zenodo")
handler = store.preload_data("13333034/andorra.zip")
preloaded_data_ids = store.cache_store.list_data_ids()
ds = store.open_data(preloaded_data_ids[0])
```

To learn more check out the example notebooks zenodo_data_store_preload*.ipynb in
[examples](examples).


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
git clone https://github.com/dcs4cop/xcube-cds.git
python -m pip install --no-deps --editable xcube-zenodo/
```

This installs all the dependencies of xcube-zenodo into a fresh conda environment,
then installs xcube-cds into this environment from the repository.

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