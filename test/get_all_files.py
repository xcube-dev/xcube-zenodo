from xcube.core.store import new_data_store

store = new_data_store("zenodo")
ds = store.open_data(
    "4305975",
    file_names=[
        "veg_ndvi_avhrr.mod09ga.gaps_p90_5km_s0..0cm_19820101..19820131_v1.0.tif",
        "veg_ndvi_avhrr.mod09ga.gaps_p90_5km_s0..0cm_19820201..19820228_v1.0.tif",
    ],
)
