## Changes in 1.0.0

### Major Changes
* The `ZenodoDataStore` class now requires a `root` parameter during initialization,
  which represents the Zenodo record ID. This means each instance of `ZenodoDataStore`
  corresponds to a single Zenodo record as its data source. The data id are then
  equivalent to the filenames given in one record.   
* The `preload_data` method now returns a store containing the preloaded data. After
  preloading, the returned store may be used to access the data.

### Enhancements

* The data store now supports preloading nested compressed files, including those 
  with multiple directory levels within the archive.
* If no data IDs are provided in the preload_data method, all compressed datasets in 
  the Zenodo record will be preloaded.

### Fixes
* Path handling during the data preload process is now managed through the abstract 
  file system of the cache data store.


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