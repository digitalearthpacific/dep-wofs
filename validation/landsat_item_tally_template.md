# Landsat WOfL item tally

Number of items in the Digital Earth Pacific `dep_ls_wofl` STAC collection,
compared to the upstream USGS Landsat Collection 2 Level 2 scenes
(`landsat-c2l2-sr` at https://landsatlook.usgs.gov/stac-server) for the same
WRS-2 path/rows, which WOfL processing reads from. Generated {date} by
`landsat_item_tally.py`. Per-platform counts for both catalogs are in
`landsat_items_by_year_platform.csv`.

## WOfL items compared to USGS scenes

![WOfL items and USGS scenes per year](landsat_items_per_year.png)

{comparison_table}

## WOfL items by Landsat platform

![WOfL items per year by Landsat platform](landsat_items_per_year_by_platform.png)

{platform_table}
