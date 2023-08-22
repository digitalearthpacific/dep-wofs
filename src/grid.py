import geopandas as gpd

grid = (
    gpd.read_file("../dep-grid/grid_pacific.geojson")
    .astype({"code": str, "gid": str})
    .set_index(["code", "gid"], drop=False)
)
