import geopandas as gpd

grid = (
    gpd.read_file(
        "https://raw.githubusercontent.com/digitalearthpacific/dep-grid/master/grid_pacific.geojson"
    )
    .astype({"code": str, "gid": str})
    .set_index(["code", "gid"], drop=False)
)
