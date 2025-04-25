from pathlib import Path
from random import sample, seed
import warnings
import xml.etree.ElementTree as ET

from geocube.api.core import make_geocube
import geopandas as gpd
import numpy as np
from odc.algo import mask_cleanup
import odc.stac
from odc.stats.plugins.wofs import StatsWofs
from osgeo.gdal import BuildVRT
import pandas as pd
from pystac_client import Client
from retry import retry
import rioxarray
from shapely.geometry import Point
import xarray as xr

from dep_wofs.grid import ls_grid, GADM
from dep_wofs.utils import use_alternate_s3_href

DEP_CLIENT = Client.open("https://stac.digitalearthpacific.org")
OUTPUT_DIR = Path(__file__).parent / "../data/validation"


def get_ocean_and_land_classes(ds, filters=[("erosion", 3)]):
    GADM["land"] = 1
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
    breakpoint()

    land_wofl = xr.Dataset({name + "_land": (ds[name] & filtered_land) for name in ds})
    ocean_wofl = xr.Dataset(
        {name + "_ocean": (ds[name] & filtered_ocean) for name in ds}
    )
    return xr.merge([land_wofl, ocean_wofl])


def sample_catalog(pathrows, query=dict(), n_each=3):
    return [
        item for path, row in pathrows for item in sample_tile(path, row, query, n_each)
    ]


def sample_tile(path, row, query=dict(), n=1):
    """For the given path and row and other query parameters, return n randomly
    selected STAC Items."""
    tile_bbox = ls_grid.loc[(path, row)].geometry.bounds
    query = {
        "landsat:wrs_row": dict(eq=str(row).zfill(3)),
        "landsat:wrs_path": dict(eq=str(path).zfill(3)),
    } | query
    items = list(
        DEP_CLIENT.search(
            collections=["dep_ls_wofl"], bbox=tile_bbox, query=query
        ).items()
    )
    if n > len(items):
        n = len(items)
        warnings.warn(
            "n is greater than the number of items returned. Returning all items."
        )

    return sample(items, n)


def place_random_points(item, n=25):
    """Load the "dry" and "wet" wofl classes for the given item, and sample
    n points within each zone."""
    wofl = odc.stac.load([item]).squeeze(drop=True)  # drop time
    oafile = OUTPUT_DIR / f"{item.properties['landsat:scene_id']}_classes.tif"
    if not oafile.exists():
        classes_to_sample = get_classes_to_sample(wofl)
        classes_to_sample.astype(int).rio.write_crs(wofl.rio.crs).rio.to_raster(
            oafile,
            driver="COG",
            overwrite=True,
        )

        counts = get_counts(classes_to_sample, n)
        points = pd.concat(
            [
                sample_bool_da(classes_to_sample[var], count).assign(name=var)
                for var, count in counts.items()
            ]
        )

        output_file = OUTPUT_DIR / f'{item.properties["landsat:scene_id"]}.gpkg'


#    if not output_file.exists():
#        points.to_file(output_file)
#        points.drop(["name"], axis=1).assign(label="").sample(frac=1).to_file(
#            OUTPUT_DIR / f'{item.properties["landsat:scene_id"]}_blinded.gpkg'
#        )


def get_classes_to_sample(wofl):
    # This call returns a 3 variable Dataset with "dry", "wet", and "bad".
    dry_and_wet = StatsWofs().native_transform(wofl).drop_vars("bad")

    # Add "_land" and "_ocean" suffixes to each class
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


def sample_bool_da(bool_da, n, minimum_spacing=250):
    """For the given boolean type DataArray, sample n points within the `True`
    zones. Minimum spacing ensures that all points are at least that far apart.
    Fewer points than n may be returned if there are not enough cells with the
    given criteria and distance.
    """
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

    points = points[0 : min(n, len(points))]

    return gpd.GeoDataFrame(
        geometry=points,
        crs=bool_da.odc.crs,
    )


def min_distance_to_series(point, point_series):
    """Return minimum positive distance from a point to a set of points."""
    dists = point.distance(point_series)
    return min(dists[dists > 0])


@retry(tries=3)
def create_rgb_mosaic(item):
    """Create a 3-band rgb mosaic and corresponding vrt for the Landsat item
    identified by the metadata (specifically item.properties["landsat:scene_id"])
    of the given STAC item.
    """
    # Find landsat item
    client = Client.open(
        "https://landsatlook.usgs.gov/stac-server",
        modifier=use_alternate_s3_href,
    )
    scene_id = item.properties["landsat:scene_id"]
    ls_item = client.search(
        collections=["landsat-c2l2-sr"],
        query={"landsat:scene_id": {"eq": scene_id}},
    ).items()
    tif_file = OUTPUT_DIR / f"{scene_id}_rgb.tif"
    vrt_file = OUTPUT_DIR / f"{scene_id}_rgb.vrt"
    # load item and write 3-band tiff


#    if not tif_file.exists():
#        odc.stac.load(
#            ls_item, chunks=dict(x=2048, y=2048), bands=["red", "green", "blue"]
#        ).squeeze().rio.to_raster(tif_file, driver="COG", overwrite=True)
#
# Create a VRT with standard stretch that seems to look decent in most
# situations.
# srcNodata ensures sources are complex sources
#    if not vrt_file.exists():
#        BuildVRT(vrt_file, tif_file, srcNodata=0)
#        set_vrt_min_max(vrt_file)


def set_vrt_min_max(vrt_path):
    """Set the 'LUT' element of each complex source band in the given path
    to sensible values for a Landsat RGB mosaic."""
    tree = ET.parse(vrt_path)
    root = tree.getroot()
    for source in root.findall(".//ComplexSource"):
        lut = ET.SubElement(source, "LUT")
        lut.text = "1:1,7000:2,14000:230,65535:255"
    tree.write(vrt_path)


def main():
    pathrows = [
        (99, 63),
        (81, 71),
        (100, 65),
        (81, 75),
        (75, 66),
        (99, 66),
        (94, 64),
        (93, 63),
        (75, 72),
        (74, 72),
        (100, 51),
        (68, 70),
        (51, 71),
        (48, 67),
        (61, 59),
    ]
    items = sample_catalog(pathrows=pathrows, query={"eo:cloud_cover": {"lt": 40}})
    for item in items:
        print(item)
        place_random_points(item)
        create_rgb_mosaic(item)


if __name__ == "__main__":
    seed(1337)
    odc.stac.configure_s3_access(cloud_defaults=True, requester_pays=True)
    main()
