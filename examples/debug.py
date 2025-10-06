from xcube.core.store import new_data_store


store = new_data_store("zenodo", root="12698637")
store.list_data_ids()
cache_store = store.preload_data(
    "2021-2023_TIFF.rar", target_format="zarr", chunks=(1024, 1024)
)

data_ids = cache_store.list_data_ids()
print(data_ids)
ds = cache_store.open_data(data_ids[0])
print(ds)
