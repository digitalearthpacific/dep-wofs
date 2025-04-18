from geocube.api.core import make_geocube
import geopandas as gpd
from numpy import bitwise_and
from odc.algo import mask_cleanup
import rioxarray
import xarray as xr

from dep_wofs.grid import GADM

GADM["land"] = 1


def decode_wofl(wofl):
    # Values from
    # https://docs.digitalearthafrica.org/en/latest/data_specs/Landsat_WOfS_specs.html
    # Bit | Flagging        | Value | Description
    # 0   | no data         | 1     | 1 = pixel masked out due to NO_DATA in
    #                                 source, 0 = valid data
    # 1   | non-contiguity  | 2     | At least one input band is missing or invalid
    # 2   | low solar angle | 4     | Solar incidence angle is less than 10 degrees
    # 3   | terrain shadow  | 8     | Terrain shadow
    # 4   | high slope      | 16    | Terrain slope (measured from SRTM) is larger
    #                                 than 12 degrees
    # 5   | cloud shadow    | 32    | Cloud shadow
    # 6   | cloud           | 64    | Cloud
    # 7   | water observed  | 128   | Classified as water by the decision tree
    names = (
        "no_data",
        "non-contiguity",
        "low_solar_angle",
        "terrain_shadow",
        "high_slope",
        "cloud_shadow",
        "cloud",
        "water",
    )
    if "water" in wofl:
        wofl = wofl.water

    return xr.merge(
        [
            (bitwise_and(wofl, 2**bit).rename(name) > 0).astype("uint8")
            for bit, name in enumerate(names)
        ]
    )


def get_ocean_and_land_classes(ds, filters=[("erosion", 3)]):
    land = (
        make_geocube(
            GADM.to_crs(ds.rio.crs),
            like=next(iter(ds.data_vars.values())),
            fill=0,
        )
        .astype(bool)
        .land
    )
    filtered_land = mask_cleanup(land, filters)

    ocean = ~land
    filtered_ocean = mask_cleanup(ocean, filters)

    land_wofl = xr.Dataset({name + "_land": (ds[name] & filtered_land) for name in ds})
    ocean_wofl = xr.Dataset(
        {name + "_ocean": (ds[name] & filtered_ocean) for name in ds}
    )
    return xr.merge([land_wofl, ocean_wofl])


def add_ocean_and_land_classes(decoded_wofl):
    return xr.merge([decoded_wofl, get_ocean_and_land_classes(decoded_wofl)])


def summarize_wofl_tile(wofl):
    decoded_wofl = decode_wofl(wofl)
    decoded_wofl = add_ocean_and_land_classes(decoded_wofl)
    return decoded_wofl.mean().compute().to_pandas()


def main():
    from pystac_client import Client
    import odc.stac

    cat = Client.open("https://stac.digitalearthpacific.org")
    summaries = []
    for i, item in enumerate(cat.get_collection("dep_ls_wofl").get_items()):
        wofl = odc.stac.load([item], chunks=dict(x=2048, y=2048)).water.squeeze()
        summaries.append(summarize_wofl_tile(wofl))
        print(i)


if __name__ == "__main__":
    main()
