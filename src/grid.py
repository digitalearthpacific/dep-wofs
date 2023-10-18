import geopandas as gpd

grid = (
    gpd.read_file(
        "https://raw.githubusercontent.com/digitalearthpacific/dep-grid/2a799daab9f56781392739a6336b6be7f2da26f9/grid_pacific.geojson"
    )
    .astype({"code": str, "gid": str})
    .set_index(["code", "gid"], drop=False)
)
