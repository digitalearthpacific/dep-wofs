import geopandas as gpd

grid = (
    gpd.read_file(
        "https://raw.githubusercontent.com/digitalearthpacific/dep-grid/master/grid_pacific.geojson"
        # "https://deppcpublicstorage.blob.core.windows.net/input/dep-grid/grid_pacific_land.gpkg"
    )
    .astype({"code": str, "gid": str})
    .set_index(["code", "gid"], drop=False)
)
