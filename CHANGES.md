## Changes in 0.1.1

### Enhancements

* The data store now supports preloading compressed Zarr files even without an explicit
  `.zarr` extension in the file name, by verifying if the uncompressed directory is a
  valid Zarr structure.

### Fixes

* xcube-zenodo is now compatible with Python 3.10 and 3.11. Compatibility was
  previously limited to Python ≥3.12 due to the usage of identical quotes in nested
  f-strings. This has been fixed by using different quotes.

### Other changes

* The Zenodo API is no longer used, as it was only required for the `get_data_ids`
  method. This method is not supported because Zenodo hosts a wide range of research,
  making it impractical to crawl through all records. Removing reliance on the
  Zenodo API also eliminates the need for an access token, simplifying the setup
  of the store.
* The preload API of xcube, released in [xcube=1.8.0](https://github.com/xcube-dev/xcube/releases/tag/v1.8.0),
  is now used to ensure consistency with other xcube data store plugins. 


## Changes in 0.1.0

* Initial version of Zenodo Data Store.