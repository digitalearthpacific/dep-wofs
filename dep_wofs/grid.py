import pandas as pd

from dep_tools.grids import grid, gadm


# This needs to be addressed. The intersection code for the gridspec is just
# too slow because it needs to do the buffer. So either fix that, or cache
# this grid like coastlines
grid_gpdf = grid(intersect_with=gadm(), return_type="GeoDataFrame")
grid_gs = grid()
# Use for wofs, i.e. summary products
grid = pd.DataFrame(
    index=grid_gpdf.index,
    data=dict(geobox=[grid_gs.tile_geobox(i) for i in grid_gpdf.index]),
)
