from xcube.core.store import new_data_store

store = new_data_store(
    "zenodo",
    access_token="ZsZVfyPCmLYRZQtfSYWruNwXYBykonv0pXZYnrQYNNL0gGMJipYsx0CYvOSB",
)

supported_keys = ["title", "file_key", "file_size", "file_links"]
data_ids = store.get_data_ids(include_attrs=supported_keys)
for data_id, attrs in data_ids:
    print(data_id)
    print(attrs)
