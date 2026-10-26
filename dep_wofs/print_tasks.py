"""Print the tiles that need to be processed as a JSON list.

The Argo workflows use this list to start one job per tile. Tiles that the S3
log marks as complete are left out.
"""

import json
import sys
from itertools import product
from typing import Annotated, Optional, cast

from geopandas import GeoDataFrame
import typer
from cloud_logger import CsvLogger, filter_by_log, S3Handler
from dep_tools.grids import gadm_union, grid as dep_grid
from dep_tools.landsat_utils import landsat_grid
from dep_tools.namers import S3ItemPath
from dep_tools.parsers import bool_parser, datetime_parser

from config import BUCKET


def main(
    years: Annotated[str, typer.Option(parser=datetime_parser)],
    version: Annotated[str, typer.Option()],
    limit: Optional[str] = None,
    retry_errors: Annotated[str, typer.Option(parser=bool_parser)] = "True",
    grid: Optional[str] = "dep",
    dataset_id: Optional[str] = "wofs_summary_annual",
    overwrite_existing_log: Annotated[str, typer.Option(parser=bool_parser)] = "False",
    filter_using_log: Annotated[str, typer.Option(parser=bool_parser)] = "True",
) -> None:
    """Print the tiles still to process for the given years as a JSON list.

    Args:
        years: A year (``"2020"``), a range of years to process separately
            (``"2014_2024"``), or one period (``"1984/2023"``).
        version: The output version, e.g. ``"0.2.0"``.
        limit: The maximum number of tasks to print. All are printed if not set.
        retry_errors: Whether to include tiles whose last logged attempt failed.
        grid: ``"dep"`` for the DEP grid (summary products) or ``"ls"`` for the
            Landsat path/row grid (WOfLs).
        dataset_id: The dataset whose log is read, e.g. ``"wofs_summary_annual"``
            or ``"wofl"``.
        overwrite_existing_log: Whether to delete each year's log before reading
            it, so that every tile is included.
        filter_using_log: Whether to leave out tiles the log marks as complete.
    """
    this_grid = dep_land_tiles() if grid == "dep" else landsat_grid()
    first_name = dict(dep="column", ls="path")
    second_name = dict(dep="row", ls="row")

    params = list()
    for year in years:
        itempath = S3ItemPath(
            bucket=BUCKET,
            sensor="ls",
            dataset_id=dataset_id,
            version=version,
            time=str(year).replace("/", "_"),
        )

        logger = CsvLogger(
            name=dataset_id,
            path=f"{itempath.bucket}/{itempath.log_path()}",
            overwrite=overwrite_existing_log,
            header="time|index|status|paths|comment\n",
            cloud_handler=S3Handler,
        )

        grid_subset = (
            filter_by_log(this_grid, logger.parse_log(), retry_errors)
            if filter_using_log
            else this_grid
        )

        these_params = [
            {
                # we could take fromthe grid if we name the dep_grid multiindex
                first_name[grid]: region[0][0],
                second_name[grid]: region[0][1],
                "datetime": region[1],
            }
            for region in product(grid_subset.index, [year])
        ]
        params += these_params

    if limit is not None:
        params = params[0 : int(limit)]

    json.dump(params, sys.stdout)


def dep_land_tiles() -> GeoDataFrame:
    """DEP grid tiles that touch land, one row per tile."""
    return cast(
        GeoDataFrame,
        dep_grid(intersect_with=gadm_union(), return_type="GeoDataFrame"),
    )


if __name__ == "__main__":
    typer.run(main)
