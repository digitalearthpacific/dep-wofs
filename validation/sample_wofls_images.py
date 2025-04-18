from random import sample, seed
import warnings
import xml.etree.ElementTree as ET

import geopandas as gpd
import numpy as np
from odc.algo import mask_cleanup
import odc.stac
from odc.stats.plugins.wofs import StatsWofs
from osgeo.gdal import BuildVRT
import pandas as pd
from pystac_client import Client
import rioxarray
from shapely.geometry import Point
import xarray as xr

from dep_wofs.grid import ls_grid
from dep_wofs.utils import use_alternate_s3_href

from summarize_wofl_tiles import get_ocean_and_land_classes

CLIENT = Client.open("https://stac.digitalearthpacific.org")


def sample_tile(path, row, query=dict(), n=1):
    tile_bbox = ls_grid.loc[(path, row)].geometry.bounds
    query = {
        "landsat:wrs_row": dict(eq=str(row).zfill(3)),
        "landsat:wrs_path": dict(eq=str(path).zfill(3)),
    } | query
    items = list(
        CLIENT.search(collections=["dep_ls_wofl"], bbox=tile_bbox, query=query).items()
    )
    if n > len(items):
        n = len(items)
        warnings.warn(
            "n is greater than the number of items returned. Returning all items."
        )

    return sample(items, n)


def sample_catalog(query=dict()):
    path_rows = [(99, 63), (81, 71), (100, 65), (81, 75), (75, 66)]
    return [sample_tile(path, row, query)[0] for path, row in path_rows]


def sample_da(da, n):
    # I think it's still possible we have dupes
    pts = pd.DataFrame(
        dict(
            x=np.random.choice(da.x.values, size=n),
            y=np.random.choice(da.y.values, size=n),
        )
    ).to_xarray()
    df = da.sel(pts, method="nearest").to_dataframe()
    return gpd.GeoDataFrame(
        df, geometry=df[["x", "y"]].apply(Point, axis=1), crs=da.odc.crs
    )


def min_distance_to_series(point, point_series):
    dists = point.distance(point_series)
    return min(dists[dists > 0])


def sample_bool_da(bool_da, n, minimum_spacing=250):
    bool_da_stacked = bool_da.stack(z=("x", "y"))
    positives = bool_da_stacked[bool_da_stacked]
    max_samples = len(positives)
    if n > max_samples:
        n = max_samples
    sample_indices = np.random.default_rng().choice(max_samples, size=n, replace=False)
    points = pd.DataFrame(
        dict(x=positives.x[sample_indices], y=positives.y[sample_indices])
    ).apply(Point, axis=1)
    points = points[
        points.apply(min_distance_to_series, point_series=points) > minimum_spacing
    ]

    attempts = 1
    while len(points) < n and attempts < 10:
        sample_indices = np.random.default_rng().choice(
            max_samples, size=n, replace=False
        )
        more_points = pd.DataFrame(
            dict(x=positives.x[sample_indices], y=positives.y[sample_indices])
        ).apply(Point, axis=1)
        points = pd.concat([points, more_points])
        points = points[
            points.apply(min_distance_to_series, point_series=points) > minimum_spacing
        ]
        attempts += 1

    return gpd.GeoDataFrame(
        geometry=points,
        crs=bool_da.odc.crs,
    )


def get_classes_to_sample(wofl):
    dry_and_wet = StatsWofs().native_transform(wofl).drop_vars("bad")
    dry_and_wet_land_ocean_classes = get_ocean_and_land_classes(dry_and_wet)
    # remove small areas
    return dry_and_wet_land_ocean_classes.map(
        mask_cleanup, mask_filters=[("erosion", 3), ("dilation", 3)]
    )


def get_counts(classes_to_sample, n=25):
    return {
        name: min(value.item(), n)
        for name, value in classes_to_sample.sum().data_vars.items()
    }


def place_random_points(item, n=25):
    wofl = odc.stac.load([item]).squeeze(drop=True)  # drop time
    classes_to_sample = get_classes_to_sample(wofl)
    counts = get_counts(classes_to_sample, n)
    points = pd.concat(
        [
            sample_bool_da(classes_to_sample[var], count).assign(name=var)
            for var, count in counts.items()
        ]
    )

    points.to_file(f'J{item.properties["landsat:scene_id"]}.gpkg')
    points.drop(["name"], axis=1).assign(label="").sample(frac=1).to_file(
        f'J{item.properties["landsat:scene_id"]}_blinded.gpkg'
    )


# item_id = item.properties["landsat:scene_id"]
#    classes_to_sample.astype("int8").rio.to_raster(
#        f"{item_id}_classes_to_sample.tif", driver="COG"
#    )
# wofl.rio.to_raster(f"wofl_{item.properties['landsat:scene_id']}.tif", driver="COG")
# no_data.astype("int8").rio.to_raster(f"nodata_{item_id}.tif", driver="COG")
# land.astype("int8").rio.to_raster(f"land_{item_id}.tif", driver="COG")

# decoded_wofl.astype("int8").rio.to_raster(
#    f"decoded_wofl_{item.properties['landsat:scene_id']}.tif", driver="COG"
# )


def create_rgb_mosaic(item):
    # download data
    client = Client.open(
        "https://landsatlook.usgs.gov/stac-server",
        modifier=use_alternate_s3_href,
    )
    scene_id = item.properties["landsat:scene_id"]
    ls_item = client.search(
        collections=["landsat-c2l2-sr"],
        query={"landsat:scene_id": {"eq": scene_id}},
    ).items()
    odc.stac.load(
        ls_item, chunks=dict(x=2048, y=2048), bands=["red", "green", "blue"]
    ).squeeze().rio.to_raster(f"{scene_id}_rgb.tif", driver="COG")

    # Create a VRT with standard stretch that seems to look decent in most
    # situations.
    # srcNodata ensures sources are complex sources
    BuildVRT(f"{scene_id}_rgb.vrt", f"{scene_id}_rgb.tif", srcNodata=0)
    set_vrt_min_max(f"{scene_id}_rgb.vrt")


def set_vrt_min_max(vrt_path):
    tree = ET.parse(vrt_path)
    root = tree.getroot()
    for source in root.findall(".//ComplexSource"):
        lut = ET.SubElement(source, "LUT")
        lut.text = "1:1,7000:2,14000:230,65535:255"
    tree.write(vrt_path)


def main():
    items = sample_catalog(query={"eo:cloud_cover": {"lt": 40}})
    for item in items:
        print(item)
        place_random_points(item)
        create_rgb_mosaic(item)


if __name__ == "__main__":
    seed(1337)
    odc.stac.configure_s3_access(cloud_defaults=True, requester_pays=True)
    main()
