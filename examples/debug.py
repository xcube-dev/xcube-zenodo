from xcube.core.store import new_data_store


store = new_data_store("zenodo")
desc = store.describe_data("8154445/planet_canopy_cover_30m_v0.1.tif")
print(desc.to_dict())
