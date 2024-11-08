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


## Setup <a name="setup"></a>

### Installing the xcube-zenodo plugin from the repository <a name="install_source"></a>

To install xcube-zenodo directly from the git repository, clone the repository,
direct into `xcube-zenodo`, and follow the steps below:

```bash
conda env create -f environment.yml
conda activate xcube-stac
pip install .
```

This installs all the dependencies of `xcube-zenodo` into a fresh conda
environment, and installs xcube-zenodo into this environment from the
repository.

### Create Access Token
Create an access token for the Zenodo API following the [zenodo documentation](https://zenodo.org/login/?next=%2Faccount%2Fsettings%2Fapplications%2Ftokens%2Fnew%2F).
This access code will be required when initializing the zenodo data store.

## Testing <a name="testing"></a>

To run the unit test suite:

```bash
pytest
```

To analyze test coverage:

```bash
pytest --cov=xcube_stac
```

To produce an HTML
[coverage report](https://pytest-cov.readthedocs.io/en/latest/reporting.html):

```bash
pytest --cov-report html --cov=xcube_stac
```