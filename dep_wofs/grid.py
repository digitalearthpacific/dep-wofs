from pathlib import Path
import geopandas as gpd
import pandas as pd

from dep_tools.grids import grid

# Replace this when the grid gymnastics have passed
GADM = gpd.read_file(
    "https://dep-public-staging.s3.us-west-2.amazonaws.com/aoi/aoi.gpkg"
)

# This needs to be addressed. The intersection code for the gridspec is just
# too slow because it needs to do the buffer. So either fix that, or cache
# this grid like coastlines
grid_gpdf = grid(intersect_with=GADM, return_type="GeoDataFrame")
grid_gs = grid()
# Use for wofs, i.e. summary products
grid = pd.DataFrame(
    index=grid_gpdf.index,
    data=dict(geobox=[grid_gs.tile_geobox(i) for i in grid_gpdf.index]),
)


# Used for wofls, i.e. daily products
ls_grid_path = Path("data/ls_grid.gpkg")
if not ls_grid_path.exists():
    landsat_pathrows = gpd.read_file(
        "https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/atoms/files/WRS2_descending_0.zip"
    )
    ls_grid = landsat_pathrows.loc[
        landsat_pathrows.sjoin(
            gadm_union.to_crs(landsat_pathrows.crs), how="inner"
        ).index
    ]
    ls_grid.to_file(ls_grid_path)

ls_grid = gpd.read_file(ls_grid_path).set_index(["PATH", "ROW"])
