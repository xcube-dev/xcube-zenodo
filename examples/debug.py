from xcube.core.store import new_data_store

store = new_data_store("zenodo")
handler = store.preload_data("11546130/mergedlabels.zarr.zip")
print("done")
