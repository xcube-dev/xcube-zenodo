from xcube.core.store import new_data_store

store = new_data_store(
    "zenodo",
    access_token="ZsZVfyPCmLYRZQtfSYWruNwXYBykonv0pXZYnrQYNNL0gGMJipYsx0CYvOSB",
)

supported_keys = ["file_key"]
data_ids = store.get_data_ids(include_attrs=supported_keys)
for data_id, attrs in data_ids:
    if ".zarr" in attrs["file_key"]:
        print(data_id)
        print(attrs)
