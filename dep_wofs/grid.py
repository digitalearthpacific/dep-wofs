from pathlib import Path
import geopandas as gpd

from dep_tools.grids import grid, gadm_union

GADM = gadm_union()

# Use for wofs, i.e. summary products
grid = grid(intersect_with=GADM, return_type="GeoDataFrame")


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
