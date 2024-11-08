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


## Create Access Token
Create an access token for the Zenodo API [here](https://zenodo.org/login/?next=%2Faccount%2Fsettings%2Fapplications%2Ftokens%2Fnew%2F).