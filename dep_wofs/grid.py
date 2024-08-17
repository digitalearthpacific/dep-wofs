import dep_grid

# grid = dep_grid.grid(return_type="GeoSeries")
grid = dep_grid.grid(
    intersect_with=dep_grid.gadm_union
)  # .clip( dep_grid.gadm_union.to_crs(dep_grid.PACIFIC_EPSG))
